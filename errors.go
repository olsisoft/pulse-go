package pulse

import (
	"fmt"
)

// APIError is the base error type for any non-2xx HTTP response from the Pulse
// server. Carries the HTTP status code, the request path, and the parsed JSON
// body (if any) so log lines + bug reports are actionable.
//
// Use errors.As to test for a specific subtype:
//
//	var notFound *pulse.NotFoundError
//	if errors.As(err, &notFound) {
//	    // handle 404
//	}
//
// All Pulse-side errors satisfy the APIError interface contract.
type APIError struct {
	StatusCode int
	Path       string
	Body       map[string]any
}

// Error implements the error interface.
func (e *APIError) Error() string {
	msg := fmt.Sprintf("HTTP %d from %s", e.StatusCode, e.Path)
	if e.Body != nil {
		if v, ok := e.Body["error"].(string); ok && v != "" {
			return msg + " — " + v
		}
		if v, ok := e.Body["errorMessage"].(string); ok && v != "" {
			return msg + " — " + v
		}
		if v, ok := e.Body["message"].(string); ok && v != "" {
			return msg + " — " + v
		}
	}
	return msg
}

// AuthError — 401, invalid / missing / expired JWT.
type AuthError struct{ APIError }

// NotFoundError — 404, resource does not exist.
type NotFoundError struct{ APIError }

// ValidationError — 400, request body is malformed.
type ValidationError struct{ APIError }

// RateLimitError — 429, per-user or per-IP rate limit hit.
//
// RetryAfterSeconds carries the server's advised wait time, parsed from either
// the JSON body (`retryAfterSeconds` field) or the `Retry-After` HTTP header,
// whichever is present. Zero means the server gave no hint — back off with
// your own default.
type RateLimitError struct {
	APIError
	RetryAfterSeconds int
}

// ErrNoToken is returned by request() when the caller invokes an authenticated
// endpoint without setting a token first. It's wrapped in an AuthError so the
// shape is consistent with server-side 401s.
var errNoToken = &AuthError{APIError{
	StatusCode: 401,
	Body:       map[string]any{"error": "no token set: call client.Auth.Login(ctx, ...) first or pass pulse.WithToken(...)"},
}}
