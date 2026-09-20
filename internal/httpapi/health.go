package httpapi

import (
	"context"
	"net/http"
)

// QuotaResponse explains the rule governing a client.
type QuotaResponse struct {
	ClientID  string `json:"client_id"`
	Resource  string `json:"resource"`
	Algorithm string `json:"algorithm"`
	Limit     int64  `json:"limit"`
	WindowMs  int64  `json:"window_ms"`
	Burst     int64  `json:"burst,omitempty"`
	// Source names the config entry the rule came from: override, client,
	// resource or default.
	Source string `json:"source"`
}

// handleQuota reports the effective rule for a client, optionally scoped to a
// resource via ?resource=. It exists so that "why is this client limited to
// N?" is answerable without reading the config file and replaying its
// precedence rules by hand.
func (s *Server) handleQuota(w http.ResponseWriter, r *http.Request) {
	client := r.PathValue("client")
	if client == "" {
		writeError(w, s.log, http.StatusBadRequest, CodeBadRequest, "client is required")
		return
	}
	resource := r.URL.Query().Get("resource")

	res := s.deps.Quotas.Load().Resolve(client, resource)
	writeJSON(w, s.log, http.StatusOK, QuotaResponse{
		ClientID:  client,
		Resource:  resource,
		Algorithm: res.Rule.Algorithm,
		Limit:     res.Rule.Limit,
		WindowMs:  res.Rule.Window.Milliseconds(),
		Burst:     res.Rule.Burst,
		Source:    string(res.Source),
	})
}

// HealthResponse is the body of the health endpoints.
type HealthResponse struct {
	Status  string `json:"status"`
	Redis   string `json:"redis,omitempty"`
	Breaker string `json:"breaker,omitempty"`
	Error   string `json:"error,omitempty"`
}

// handleHealthz is liveness: it answers for the process itself and checks no
// dependencies. Failing it would have an orchestrator restart a process that
// is working perfectly well while Redis is down.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.log, http.StatusOK, HealthResponse{Status: "ok"})
}

// handleReadyz reports whether Redis is reachable.
//
// Note the asymmetry with /v1/check: readiness tells the truth about the
// dependency, while the data path keeps serving degraded decisions. They
// answer different questions — "is this instance healthy?" versus "should this
// request proceed?" — and conflating them would make a Redis outage cascade
// into every service behind the limiter.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	resp := HealthResponse{Status: "ok", Redis: "ok"}
	if s.deps.BreakerState != nil {
		resp.Breaker = s.deps.BreakerState()
	}
	if s.deps.Health == nil {
		writeJSON(w, s.log, http.StatusOK, resp)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.deps.HealthTimeout)
	defer cancel()

	if err := s.deps.Health(ctx); err != nil {
		resp.Status = "degraded"
		resp.Redis = "unreachable"
		resp.Error = err.Error()
		writeJSON(w, s.log, http.StatusServiceUnavailable, resp)
		return
	}
	writeJSON(w, s.log, http.StatusOK, resp)
}
