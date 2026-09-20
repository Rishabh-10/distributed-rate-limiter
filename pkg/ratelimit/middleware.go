package ratelimit

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"
)

// Middleware limits requests to next.
//
// The service's /v1/check endpoint answers with a decision, not a status code.
// This is the piece that turns `allowed: false` into a real 429 with the
// conventional headers, because only the caller knows what a rejection should
// look like to its own clients.
//
// When the limiter cannot be reached the request is allowed through and the
// error handler is invoked (see WithFailClosed to invert that).
func (c *Client) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c.skip != nil && c.skip(r) {
			next.ServeHTTP(w, r)
			return
		}

		clientID := c.clientID(r)
		if clientID == "" {
			// Nothing to charge. Failing open here is deliberate: an
			// unidentifiable caller is a gap in the caller's own identity
			// scheme, not evidence of abuse.
			next.ServeHTTP(w, r)
			return
		}

		decision, err := c.Check(r.Context(), Request{
			ClientID: clientID,
			Resource: c.resource(r),
			Cost:     c.cost(r),
		})
		if err != nil {
			if c.onError != nil {
				c.onError(r, err)
			}
			if c.failOpen {
				next.ServeHTTP(w, r)
				return
			}
			c.writeUnavailable(w)
			return
		}

		if c.forwardHdr {
			setHeaders(w.Header(), decision)
		}

		if decision.Allowed {
			next.ServeHTTP(w, r)
			return
		}

		if c.onDeny != nil {
			c.onDeny.ServeHTTP(w, r)
			return
		}
		writeTooManyRequests(w, decision)
	})
}

func setHeaders(h http.Header, d Decision) {
	h.Set("X-RateLimit-Limit", strconv.FormatInt(d.Limit, 10))
	h.Set("X-RateLimit-Remaining", strconv.FormatInt(d.Remaining, 10))
	h.Set("X-RateLimit-Reset", strconv.FormatInt(ceilSeconds(d.ResetAfter), 10))
	if d.Degraded {
		h.Set("X-RateLimit-Degraded", "true")
	}
}

func writeTooManyRequests(w http.ResponseWriter, d Decision) {
	if d.RetryAfter > 0 {
		// Retry-After is whole seconds; rounding down would invite the client
		// back before the slot actually frees, which is how a thundering herd
		// gets organised.
		w.Header().Set("Retry-After", strconv.FormatInt(ceilSeconds(d.RetryAfter), 10))
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusTooManyRequests)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":            "rate_limited",
			"message":         "too many requests",
			"retry_after_ms":  d.RetryAfter.Milliseconds(),
			"limit":           d.Limit,
			"window_reset_ms": d.ResetAfter.Milliseconds(),
		},
	})
}

func (c *Client) writeUnavailable(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    "rate_limiter_unavailable",
			"message": "rate limiter unavailable and configured to fail closed",
		},
	})
}

func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(math.Ceil(d.Seconds()))
}
