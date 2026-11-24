package kosha

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RetryPolicy bounds retries (clients/conformance/RETRY.md). The policy is
// the same in the Python, TypeScript and Go SDKs.
type RetryPolicy struct {
	// MaxAttempts counts every attempt, including the first. 1 disables retries.
	MaxAttempts int
	// BaseDelay and MaxDelay shape full-jitter backoff:
	// rand(0, min(MaxDelay, BaseDelay*2^retry)).
	BaseDelay time.Duration
	MaxDelay  time.Duration
	// MaxRetryAfter: give up at once when the server asks to wait longer.
	MaxRetryAfter time.Duration
	// TotalBudget: give up when the next sleep would pass this, measured
	// from the first attempt.
	TotalBudget time.Duration
}

// DefaultRetryPolicy is the policy a new Client uses.
var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts:   3,
	BaseDelay:     250 * time.Millisecond,
	MaxDelay:      8 * time.Second,
	MaxRetryAfter: 30 * time.Second,
	TotalBudget:   60 * time.Second,
}

// IsIdempotent reports whether replaying method+path cannot change the
// outcome. Idempotent: every GET, search, exists, upsert (keyed by the
// required id), merge, delete_by_query, flush and namespace delete. Not:
// namespace create, schema put, branch create and delete, shards, and any
// route this table does not know.
func IsIdempotent(method, path string) bool {
	m := strings.ToUpper(method)
	if m == http.MethodGet || m == http.MethodHead {
		return true
	}
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	var parts []string
	for _, p := range strings.Split(path, "/") {
		if p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) < 2 || parts[0] != "v1" || parts[1] != "namespaces" {
		return false
	}
	switch {
	case m == http.MethodDelete:
		return len(parts) == 3
	case m == http.MethodPost && len(parts) == 4:
		switch parts[3] {
		case "search", "exists", "documents", "replace", "delete", "flush":
			return true
		}
	}
	return false
}

// retryableStatus applies RETRY.md rules 1 and 2 to an error response.
// replaySafe is idempotent or the caller's WithRetryNonIdempotent opt-in.
func retryableStatus(status int, body []byte, idempotent, replaySafe bool) bool {
	var parsed struct {
		Retryable *bool `json:"retryable"`
		Details   struct {
			Outcome string `json:"outcome"`
		} `json:"details"`
	}
	if json.Unmarshal(body, &parsed) == nil {
		// The write may have been applied (a forwarded write that timed
		// out): replay it only when that is safe, whatever retryable says.
		if parsed.Details.Outcome == "unknown" {
			return replaySafe
		}
		if parsed.Retryable != nil {
			return *parsed.Retryable
		}
	}
	switch status {
	case 429, 503:
		return true
	case 502, 504:
		return idempotent
	}
	return false
}

// serverDelay is the delay the server asked for: the body's retry_after_ms,
// else the Retry-After header (delta-seconds or an HTTP-date).
func serverDelay(body []byte, header http.Header, now time.Time) (time.Duration, bool) {
	var parsed struct {
		RetryAfterMs *float64 `json:"retry_after_ms"`
	}
	if json.Unmarshal(body, &parsed) == nil && parsed.RetryAfterMs != nil && *parsed.RetryAfterMs >= 0 {
		return time.Duration(*parsed.RetryAfterMs * float64(time.Millisecond)), true
	}
	return parseRetryAfter(header.Get("Retry-After"), now)
}

func parseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.ParseUint(v, 10, 32); err == nil {
		return time.Duration(secs) * time.Second, true
	}
	if at, err := http.ParseTime(v); err == nil {
		if d := at.Sub(now); d > 0 {
			return d, true
		}
		return 0, true
	}
	return 0, false
}

func (p RetryPolicy) backoff(retry int) time.Duration {
	limit := p.MaxDelay
	if retry < 30 {
		if d := p.BaseDelay << retry; d > 0 && d < limit {
			limit = d
		}
	}
	if limit <= 0 {
		return 0
	}
	return rand.N(limit)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
