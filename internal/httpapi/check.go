package httpapi

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/limiter"
)

// maxBodyBytes caps the request body. The payload is a handful of short
// strings; anything larger is a mistake or an attack.
const maxBodyBytes = 64 << 10

// CheckRequest asks whether one request may proceed.
type CheckRequest struct {
	// ClientID identifies whose quota to charge. Required.
	ClientID string `json:"client_id"`
	// Resource scopes the quota, typically a route. Optional: an empty
	// resource means the client's global quota.
	Resource string `json:"resource"`
	// Cost is how many slots the request consumes. Defaults to 1.
	Cost int64 `json:"cost"`
}

// CheckResponse is the limiter's decision.
//
// Durations are milliseconds rather than Go duration strings so that callers
// in any language can act on them without a parser.
type CheckResponse struct {
	Allowed      bool   `json:"allowed"`
	Limit        int64  `json:"limit"`
	Remaining    int64  `json:"remaining"`
	ResetAfterMs int64  `json:"reset_after_ms"`
	RetryAfterMs int64  `json:"retry_after_ms"`
	Algorithm    string `json:"algorithm"`
	WindowMs     int64  `json:"window_ms"`
	// Degraded means Redis could not be consulted and the decision was not
	// enforced. An allowed+degraded response is a fail-open, not a permission.
	Degraded bool `json:"degraded"`
	// QuotaSource says which config entry produced the rule, so an operator
	// can answer "why did this client get this limit?" from one response.
	QuotaSource string `json:"quota_source"`
}

func (s *Server) handleCheck(w http.ResponseWriter, r *http.Request) {
	var req CheckRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, s.log, http.StatusRequestEntityTooLarge, CodePayloadLarge,
				"request body exceeds "+strconv.Itoa(maxBodyBytes)+" bytes")
			return
		}
		writeError(w, s.log, http.StatusBadRequest, CodeBadRequest,
			"invalid JSON body: "+err.Error())
		return
	}

	if req.ClientID == "" {
		writeError(w, s.log, http.StatusBadRequest, CodeBadRequest, "client_id is required")
		return
	}
	if req.Cost < 0 {
		writeError(w, s.log, http.StatusBadRequest, CodeBadRequest, "cost must not be negative")
		return
	}
	if req.Cost == 0 {
		req.Cost = 1
	}

	resolution := s.deps.Quotas.Load().Resolve(req.ClientID, req.Resource)
	rule := resolution.Rule
	key := limiter.Key(s.deps.KeyPrefix, rule.Algorithm, req.ClientID, req.Resource)

	start := time.Now()
	decision, err := s.deps.Limiter.Allow(r.Context(), key, rule, req.Cost)
	elapsed := time.Since(start)

	if err != nil {
		// Availability failures already came back as degraded decisions, so
		// reaching here means something is genuinely broken — a wiring bug or
		// a Redis error we deliberately refused to treat as an outage. Say so
		// rather than inventing an answer.
		s.deps.Metrics.ObserveError(rule.Algorithm, elapsed.Seconds())
		s.log.Error("limiter failed",
			"client_id", req.ClientID, "resource", req.Resource, "error", err)
		writeError(w, s.log, http.StatusServiceUnavailable, CodeUnavailable,
			"rate limit decision unavailable")
		return
	}

	s.deps.Metrics.ObserveDecision(rule.Algorithm, decision, elapsed.Seconds())

	resp := CheckResponse{
		Allowed:      decision.Allowed,
		Limit:        decision.Limit,
		Remaining:    decision.Remaining,
		ResetAfterMs: decision.ResetAfter.Milliseconds(),
		RetryAfterMs: decision.RetryAfter.Milliseconds(),
		Algorithm:    rule.Algorithm,
		WindowMs:     rule.Window.Milliseconds(),
		Degraded:     decision.Degraded,
		QuotaSource:  string(resolution.Source),
	}
	setRateLimitHeaders(w.Header(), decision)
	writeJSON(w, s.log, http.StatusOK, resp)
}

// setRateLimitHeaders mirrors the decision into the conventional headers, so a
// caller can forward them to its own client unchanged.
func setRateLimitHeaders(h http.Header, d limiter.Decision) {
	h.Set("X-RateLimit-Limit", strconv.FormatInt(d.Limit, 10))
	h.Set("X-RateLimit-Remaining", strconv.FormatInt(d.Remaining, 10))
	h.Set("X-RateLimit-Reset", strconv.FormatInt(ceilSeconds(d.ResetAfter), 10))
	if !d.Allowed && d.RetryAfter > 0 {
		// Retry-After is defined in whole seconds, and rounding down would
		// invite the client back before the slot actually frees.
		h.Set("Retry-After", strconv.FormatInt(ceilSeconds(d.RetryAfter), 10))
	}
	if d.Degraded {
		h.Set("X-RateLimit-Degraded", "true")
	}
}

func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(math.Ceil(d.Seconds()))
}
