package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// Error codes returned to clients. They are part of the API contract: callers
// branch on the code, never on the message.
const (
	CodeBadRequest   = "bad_request"
	CodeNotFound     = "not_found"
	CodeInternal     = "internal_error"
	CodeUnavailable  = "limiter_unavailable"
	CodePayloadLarge = "payload_too_large"
)

// ErrorResponse is the envelope for every non-2xx body.
type ErrorResponse struct {
	Error APIError `json:"error"`
}

// APIError describes what went wrong.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, log *slog.Logger, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already out, so there is nothing to salvage for
		// the client; record it and move on.
		log.Warn("writing response body failed", "error", err)
	}
}

func writeError(w http.ResponseWriter, log *slog.Logger, status int, code, message string) {
	writeJSON(w, log, status, ErrorResponse{Error: APIError{Code: code, Message: message}})
}
