package kosha

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Status-class sentinels: errors.Is(err, kosha.ErrNotFound) matches any
// *Error whose HTTP status is in that class, whatever its Code.
var (
	ErrInvalidArgument  = errors.New("kosha: invalid argument")   // 400, 422
	ErrUnauthenticated  = errors.New("kosha: unauthenticated")    // 401
	ErrPermissionDenied = errors.New("kosha: permission denied")  // 403
	ErrNotFound         = errors.New("kosha: not found")          // 404
	ErrConflict         = errors.New("kosha: conflict")           // 409
	ErrGone             = errors.New("kosha: gone")               // 410
	ErrPayloadTooLarge  = errors.New("kosha: payload too large")  // 413
	ErrRateLimited      = errors.New("kosha: rate limited")       // 429
	ErrUnavailable      = errors.New("kosha: unavailable")        // 503, 504
	ErrInternal         = errors.New("kosha: internal error")     // 500, 502
	ErrConfig           = errors.New("kosha: invalid client use") // ConfigError
)

// Error is a failed call that got an HTTP response.
//
// Code is the server's stable error code (docs/api/errors.md), else the
// deprecated reason of a pre-0.10 tenant-limit rejection, else "unknown".
// Branch on Code or on the sentinels, never on Message.
type Error struct {
	StatusCode int
	Code       ErrorCode
	Message    string
	Retryable  bool
	// RetryAfter is the delay the server asked for; 0 when it asked for none.
	RetryAfter time.Duration
	RequestID  string
	Details    map[string]any
	// Attempts is how many attempts were made before giving up.
	Attempts int
	// Body is the raw response body.
	Body []byte
}

func (e *Error) Error() string {
	return fmt.Sprintf("kosha: HTTP %d %s: %s", e.StatusCode, e.Code, e.Message)
}

// Is matches the status-class sentinels.
func (e *Error) Is(target error) bool {
	switch target {
	case ErrInvalidArgument:
		return e.StatusCode == 400 || e.StatusCode == 422
	case ErrUnauthenticated:
		return e.StatusCode == 401
	case ErrPermissionDenied:
		return e.StatusCode == 403
	case ErrNotFound:
		return e.StatusCode == 404
	case ErrConflict:
		return e.StatusCode == 409
	case ErrGone:
		return e.StatusCode == 410
	case ErrPayloadTooLarge:
		return e.StatusCode == 413
	case ErrRateLimited:
		return e.StatusCode == 429
	case ErrUnavailable:
		return e.StatusCode == 503 || e.StatusCode == 504
	case ErrInternal:
		return e.StatusCode == 500 || e.StatusCode == 502
	}
	return false
}

func newError(status int, header http.Header, body []byte, retryAfter time.Duration) *Error {
	e := &Error{StatusCode: status, Code: "unknown", Message: fmt.Sprintf("HTTP %d", status), RetryAfter: retryAfter, Body: body}
	var parsed struct {
		Error     string         `json:"error"`
		Code      string         `json:"code"`
		Reason    string         `json:"reason"`
		Retryable *bool          `json:"retryable"`
		RequestID string         `json:"request_id"`
		Details   map[string]any `json:"details"`
	}
	if json.Unmarshal(body, &parsed) == nil {
		switch {
		case parsed.Code != "":
			e.Code = ErrorCode(parsed.Code)
		case parsed.Reason != "":
			e.Code = ErrorCode(parsed.Reason)
		}
		if parsed.Error != "" {
			e.Message = parsed.Error
		}
		e.Retryable = parsed.Retryable != nil && *parsed.Retryable
		e.RequestID = parsed.RequestID
		e.Details = parsed.Details
	}
	if e.RequestID == "" {
		e.RequestID = header.Get("X-Request-Id")
	}
	return e
}

// TransportError is a failed call that got no response: DNS, a refused or
// reset connection, TLS, or the per-attempt timeout. It wraps the cause, so
// errors.Is(err, context.DeadlineExceeded) works.
type TransportError struct {
	Method, Path string
	// Timeout is true when the per-attempt timeout elapsed.
	Timeout  bool
	Attempts int
	Err      error
}

func (e *TransportError) Error() string {
	if e.Timeout {
		return fmt.Sprintf("kosha: %s %s timed out: %v", e.Method, e.Path, e.Err)
	}
	return fmt.Sprintf("kosha: %s %s failed: %v", e.Method, e.Path, e.Err)
}

func (e *TransportError) Unwrap() error { return e.Err }

// ConfigError is bad client configuration or arguments; nothing was sent.
// errors.Is(err, kosha.ErrConfig) matches it.
type ConfigError struct{ Message string }

func (e *ConfigError) Error() string        { return "kosha: " + e.Message }
func (e *ConfigError) Is(target error) bool { return target == ErrConfig }

func configErrorf(format string, args ...any) error {
	return &ConfigError{Message: fmt.Sprintf(format, args...)}
}
