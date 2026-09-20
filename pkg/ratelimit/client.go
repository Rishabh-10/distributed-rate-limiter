// Package ratelimit is the client for the distributed rate limiter service.
//
// It exists so that callers do not each reimplement the same four decisions:
// how long to wait for the limiter, what to do when the limiter itself is
// unreachable, how to identify the calling client, and how to render a
// rejection. Getting any of those wrong turns a rate limiter into an outage,
// and the defaults here are chosen so the safe answer is the automatic one.
//
// Typical use is the middleware:
//
//	rl := ratelimit.New("http://limiter:8080")
//	mux.Handle("/api/", rl.Middleware(apiHandler))
package ratelimit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// Decision is the limiter's answer for one request.
type Decision struct {
	Allowed   bool
	Limit     int64
	Remaining int64
	// ResetAfter is how long until the client is back to a full quota.
	ResetAfter time.Duration
	// RetryAfter is how long until a slot frees up. Zero when allowed.
	RetryAfter time.Duration
	// Degraded means the decision was not enforced — either the limiter
	// service could not reach Redis, or this client could not reach the
	// limiter service. An allowed+degraded decision is a fail-open, not a
	// grant of permission.
	Degraded bool
	// Algorithm and QuotaSource echo which rule was applied, for debugging.
	Algorithm   string
	QuotaSource string
}

// Request identifies what to charge.
type Request struct {
	// ClientID is whose quota to charge. Required.
	ClientID string
	// Resource scopes the quota, typically a route.
	Resource string
	// Cost is how many slots to consume. Zero means one.
	Cost int64
}

// Client talks to the rate limiter service.
type Client struct {
	baseURL string
	http    *http.Client

	failOpen   bool
	clientID   func(*http.Request) string
	resource   func(*http.Request) string
	cost       func(*http.Request) int64
	skip       func(*http.Request) bool
	onDeny     http.Handler
	onError    func(*http.Request, error)
	forwardHdr bool
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient replaces the underlying HTTP client. The supplied client's
// Timeout is respected as-is, so set one.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.http = hc }
}

// WithTimeout bounds each call to the limiter service.
//
// Keep this small. The limiter sits in front of a request that is already
// waiting, so every millisecond here is added to somebody's latency, and a
// limiter that is slow to answer should be treated as one that is not
// answering at all.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.http.Timeout = d }
}

// WithFailClosed makes the middleware reject traffic when the limiter service
// cannot be reached, instead of allowing it.
//
// The default is to fail open. Choose this only when unlimited traffic is
// worse than no traffic — a paid metering boundary, say. For protecting a
// backend from overload, failing open is almost always right: an unreachable
// limiter is not evidence that anyone is over quota.
func WithFailClosed() Option {
	return func(c *Client) { c.failOpen = false }
}

// WithClientIDFunc sets how the caller is identified.
//
// The default reads the X-Client-ID header and falls back to the peer IP.
// Forwarded headers (X-Forwarded-For and friends) are deliberately not
// trusted: anyone can set them, and a limiter keyed on a spoofable identifier
// limits nobody. Behind a proxy you control, supply a function that reads them.
func WithClientIDFunc(f func(*http.Request) string) Option {
	return func(c *Client) { c.clientID = f }
}

// WithResourceFunc sets how the resource is derived. The default is the
// request path.
func WithResourceFunc(f func(*http.Request) string) Option {
	return func(c *Client) { c.resource = f }
}

// WithCostFunc sets a per-request cost, for endpoints that are more expensive
// than one unit of quota.
func WithCostFunc(f func(*http.Request) int64) Option {
	return func(c *Client) { c.cost = f }
}

// WithSkipFunc exempts requests from limiting — health checks, static assets,
// anything that should never be throttled.
func WithSkipFunc(f func(*http.Request) bool) Option {
	return func(c *Client) { c.skip = f }
}

// WithDenyHandler replaces the response sent to a rejected caller. The
// rate-limit headers are already set when it runs.
func WithDenyHandler(h http.Handler) Option {
	return func(c *Client) { c.onDeny = h }
}

// WithErrorHandler registers a callback for failures to reach the limiter.
// This is where logging and metrics belong: the middleware itself stays quiet
// so that an outage cannot flood the logs of every service behind it.
func WithErrorHandler(f func(*http.Request, error)) Option {
	return func(c *Client) { c.onError = f }
}

// WithoutHeaders stops the middleware from adding X-RateLimit-* headers to
// allowed responses.
func WithoutHeaders() Option {
	return func(c *Client) { c.forwardHdr = false }
}

// New returns a Client for the limiter service at baseURL, for example
// "http://limiter:8080".
func New(baseURL string, opts ...Option) *Client {
	c := &Client{
		baseURL: trimSlash(baseURL),
		// A short default: the limiter answers in single-digit milliseconds
		// when healthy, and waiting longer than this helps nobody.
		http:       &http.Client{Timeout: 50 * time.Millisecond},
		failOpen:   true,
		clientID:   defaultClientID,
		resource:   func(r *http.Request) string { return r.URL.Path },
		cost:       func(*http.Request) int64 { return 1 },
		forwardHdr: true,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Check asks the limiter whether a request may proceed.
//
// It returns an error only when the limiter could not be consulted. Callers
// that want the fail-open behaviour for free should use Middleware instead.
func (c *Client) Check(ctx context.Context, req Request) (Decision, error) {
	if req.ClientID == "" {
		return Decision{}, fmt.Errorf("ratelimit: ClientID is required")
	}
	if req.Cost <= 0 {
		req.Cost = 1
	}

	body, err := json.Marshal(checkRequest{
		ClientID: req.ClientID,
		Resource: req.Resource,
		Cost:     req.Cost,
	})
	if err != nil {
		return Decision{}, fmt.Errorf("ratelimit: encode request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v1/check", bytes.NewReader(body))
	if err != nil {
		return Decision{}, fmt.Errorf("ratelimit: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return Decision{}, fmt.Errorf("ratelimit: call limiter: %w", err)
	}
	defer func() {
		// Drain before closing so the connection can be reused; a limiter is
		// called on every request and reconnecting each time would dominate
		// its cost.
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return Decision{}, fmt.Errorf("ratelimit: limiter returned %s", resp.Status)
	}

	var decoded checkResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&decoded); err != nil {
		return Decision{}, fmt.Errorf("ratelimit: decode response: %w", err)
	}

	return Decision{
		Allowed:     decoded.Allowed,
		Limit:       decoded.Limit,
		Remaining:   decoded.Remaining,
		ResetAfter:  time.Duration(decoded.ResetAfterMs) * time.Millisecond,
		RetryAfter:  time.Duration(decoded.RetryAfterMs) * time.Millisecond,
		Degraded:    decoded.Degraded,
		Algorithm:   decoded.Algorithm,
		QuotaSource: decoded.QuotaSource,
	}, nil
}

// checkRequest and checkResponse mirror the service's wire format. They are
// duplicated rather than imported from internal/httpapi so that this package
// stays importable by outside code.
type checkRequest struct {
	ClientID string `json:"client_id"`
	Resource string `json:"resource"`
	Cost     int64  `json:"cost"`
}

type checkResponse struct {
	Allowed      bool   `json:"allowed"`
	Limit        int64  `json:"limit"`
	Remaining    int64  `json:"remaining"`
	ResetAfterMs int64  `json:"reset_after_ms"`
	RetryAfterMs int64  `json:"retry_after_ms"`
	Algorithm    string `json:"algorithm"`
	Degraded     bool   `json:"degraded"`
	QuotaSource  string `json:"quota_source"`
}

// defaultClientID prefers an explicit header and falls back to the peer
// address, which is the only identifier that cannot be forged by the caller.
func defaultClientID(r *http.Request) string {
	if id := r.Header.Get("X-Client-ID"); id != "" {
		return id
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
