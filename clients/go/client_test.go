package kosha

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type recorded struct {
	Method, Path string
	Header       http.Header
	Body         map[string]any
}

type fake struct {
	*httptest.Server
	mu   sync.Mutex
	reqs []recorded
}

func (f *fake) requests() []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recorded(nil), f.reqs...)
}

func newFake(t *testing.T, h func(n int, r recorded, w http.ResponseWriter)) *fake {
	t.Helper()
	f := &fake{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		rec := recorded{Method: r.Method, Path: r.URL.EscapedPath(), Header: r.Header.Clone()}
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &rec.Body)
		}
		f.mu.Lock()
		n := len(f.reqs)
		f.reqs = append(f.reqs, rec)
		f.mu.Unlock()
		h(n, rec, w)
	}))
	t.Cleanup(f.Close)
	return f
}

func writeJSON(w http.ResponseWriter, status int, body any, headers ...string) {
	for i := 0; i+1 < len(headers); i += 2 {
		w.Header().Set(headers[i], headers[i+1])
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func errBody(code string, retryable bool, extra ...any) map[string]any {
	b := map[string]any{"error": code + " happened", "code": code, "retryable": retryable, "request_id": "req-1"}
	for i := 0; i+1 < len(extra); i += 2 {
		b[extra[i].(string)] = extra[i+1]
	}
	return b
}

func testClient(t *testing.T, url string, opts ...Option) *Client {
	t.Helper()
	all := append([]Option{WithBaseURL(url), WithAPIKey("sk-test"),
		WithRetryPolicy(RetryPolicy{BaseDelay: 5 * time.Millisecond, MaxDelay: 20 * time.Millisecond})}, opts...)
	c, err := NewClient(all...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func asError(t *testing.T, err error) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("want *kosha.Error, got %T %v", err, err)
	}
	return e
}

func TestOutcomeUnknownReplayedOnlyWhenSafe(t *testing.T) {
	unknown := []byte(`{"retryable":true,"details":{"outcome":"unknown"}}`)
	if retryableStatus(503, unknown, false, false) {
		t.Fatal("non-idempotent outcome-unknown write was retried")
	}
	if !retryableStatus(503, unknown, true, true) {
		t.Fatal("idempotent outcome-unknown write was not retried")
	}
	if !retryableStatus(503, []byte(`{"retryable":false,"details":{"outcome":"unknown"}}`), true, true) {
		t.Fatal("idempotent outcome-unknown write with retryable:false was not retried")
	}
	if !retryableStatus(503, unknown, false, true) {
		t.Fatal("opt-in did not apply")
	}
	if !retryableStatus(503, []byte(`{"retryable":true}`), false, false) {
		t.Fatal("retryable:true was not honoured")
	}
}

func TestRetryAfterHonoured(t *testing.T) {
	f := newFake(t, func(n int, _ recorded, w http.ResponseWriter) {
		if n == 0 {
			writeJSON(w, 429, map[string]any{"error": "slow down"}, "Retry-After", "1")
			return
		}
		writeJSON(w, 200, map[string]any{"results": []any{}, "total_hits": 0})
	})
	start := time.Now()
	out, err := testClient(t, f.URL).Namespace("ns").Search(context.Background(), SearchRequest{QueryText: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if out.TotalHits == nil || *out.TotalHits != 0 {
		t.Fatalf("bad response %+v", out)
	}
	if got := len(f.requests()); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if el := time.Since(start); el < 950*time.Millisecond {
		t.Fatalf("retried after %v, want >= 950ms", el)
	}
}

func TestQuotaNotRetried(t *testing.T) {
	f := newFake(t, func(_ int, _ recorded, w http.ResponseWriter) {
		writeJSON(w, 429, errBody("namespace_quota_exceeded", false))
	})
	_, err := testClient(t, f.URL).Namespace("ns").Search(context.Background(), SearchRequest{})
	e := asError(t, err)
	if len(f.requests()) != 1 || e.Attempts != 1 || e.Retryable || e.Code != "namespace_quota_exceeded" {
		t.Fatalf("got %d requests, %+v", len(f.requests()), e)
	}
	if !errors.Is(err, ErrRateLimited) || errors.Is(err, ErrNotFound) {
		t.Fatal("status sentinels mismatch")
	}
}

func TestBare503Backoff(t *testing.T) {
	f := newFake(t, func(n int, _ recorded, w http.ResponseWriter) {
		if n < 2 {
			writeJSON(w, 503, map[string]any{"error": "unavailable"})
			return
		}
		writeJSON(w, 200, map[string]any{"status": "flushed"})
	})
	out, err := testClient(t, f.URL).Namespace("ns").Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Status == nil || *out.Status != "flushed" || len(f.requests()) != 3 {
		t.Fatalf("got %+v after %d requests", out, len(f.requests()))
	}
}

func TestMaxAttempts(t *testing.T) {
	f := newFake(t, func(_ int, _ recorded, w http.ResponseWriter) {
		writeJSON(w, 503, errBody("unavailable", true, "retry_after_ms", 10))
	})
	_, err := testClient(t, f.URL).ListNamespaces(context.Background())
	e := asError(t, err)
	if len(f.requests()) != 3 || e.Attempts != 3 || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("got %d requests, %+v", len(f.requests()), e)
	}
}

func TestRetryAfterTooLong(t *testing.T) {
	f := newFake(t, func(_ int, _ recorded, w http.ResponseWriter) {
		writeJSON(w, 429, map[string]any{"error": "later"}, "Retry-After", "120")
	})
	start := time.Now()
	_, err := testClient(t, f.URL).ListNamespaces(context.Background())
	e := asError(t, err)
	if len(f.requests()) != 1 || e.RetryAfter != 120*time.Second || time.Since(start) > time.Second {
		t.Fatalf("got %d requests, %+v", len(f.requests()), e)
	}
}

func TestBodyDelayWinsOverHeader(t *testing.T) {
	f := newFake(t, func(n int, _ recorded, w http.ResponseWriter) {
		if n == 0 {
			writeJSON(w, 429, errBody("tenant_qps_exceeded", true, "retry_after_ms", 50), "Retry-After", "120")
			return
		}
		writeJSON(w, 200, map[string]any{"namespaces": []any{}})
	})
	start := time.Now()
	if _, err := testClient(t, f.URL).ListNamespaces(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.requests()) != 2 || time.Since(start) > time.Second {
		t.Fatalf("got %d requests in %v", len(f.requests()), time.Since(start))
	}
}

func TestTotalBudget(t *testing.T) {
	f := newFake(t, func(_ int, _ recorded, w http.ResponseWriter) {
		writeJSON(w, 503, errBody("unavailable", true, "retry_after_ms", 500))
	})
	c := testClient(t, f.URL, WithRetryPolicy(RetryPolicy{TotalBudget: 200 * time.Millisecond}))
	_, err := c.ListNamespaces(context.Background())
	if e := asError(t, err); e.Attempts != 1 || len(f.requests()) != 1 {
		t.Fatalf("got %d requests, %+v", len(f.requests()), e)
	}
}

func Test502OnlyIdempotent(t *testing.T) {
	f := newFake(t, func(_ int, _ recorded, w http.ResponseWriter) {
		writeJSON(w, 502, map[string]any{"error": "bad gateway"})
	})
	c := testClient(t, f.URL)
	_, _ = c.CreateNamespace(context.Background(), "a", nil)
	if n := len(f.requests()); n != 1 {
		t.Fatalf("create: %d requests, want 1", n)
	}
	_, _ = c.ListNamespaces(context.Background())
	if n := len(f.requests()); n != 4 {
		t.Fatalf("list: %d requests total, want 4", n)
	}
}

func TestTimeoutRetriedOnlyWhenIdempotent(t *testing.T) {
	f := newFake(t, func(_ int, _ recorded, w http.ResponseWriter) {
		time.Sleep(300 * time.Millisecond)
		writeJSON(w, 200, map[string]any{})
	})
	c := testClient(t, f.URL, WithTimeout(50*time.Millisecond))
	ctx := context.Background()
	_, err := c.CreateNamespace(ctx, "a", nil)
	var te *TransportError
	if !errors.As(err, &te) || !te.Timeout || te.Attempts != 1 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("create: %v", err)
	}
	_, err = c.Namespace("a").Search(ctx, SearchRequest{}, WithMaxAttempts(2))
	if !errors.As(err, &te) || te.Attempts != 2 {
		t.Fatalf("search: %v", err)
	}
	_, err = c.CreateNamespace(ctx, "a", nil, WithMaxAttempts(2), WithRetryNonIdempotent())
	if !errors.As(err, &te) || te.Attempts != 2 {
		t.Fatalf("opted-in create: %v", err)
	}
}

func TestRefusedConnectionRetried(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	_, err := testClient(t, url).CreateNamespace(context.Background(), "a", nil)
	var te *TransportError
	if !errors.As(err, &te) || te.Attempts != 3 || te.Timeout {
		t.Fatalf("got %v", err)
	}
}

func TestContextCancelsSleep(t *testing.T) {
	f := newFake(t, func(_ int, _ recorded, w http.ResponseWriter) {
		writeJSON(w, 503, errBody("unavailable", true, "retry_after_ms", 5000))
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := testClient(t, f.URL).ListNamespaces(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
		t.Fatalf("got %v after %v", err, time.Since(start))
	}
}

func TestErrorFallbacks(t *testing.T) {
	f := newFake(t, func(n int, _ recorded, w http.ResponseWriter) {
		switch n {
		case 0:
			writeJSON(w, 403, map[string]any{"error": "nope", "reason": "body_too_large"}, "X-Request-Id", "hdr-1")
		case 1:
			w.WriteHeader(500)
			_, _ = w.Write([]byte("plain failure"))
		default:
			writeJSON(w, 409, errBody("brand_new_code", false, "details", map[string]any{"x": 1}))
		}
	})
	c := testClient(t, f.URL, WithRetryPolicy(RetryPolicy{MaxAttempts: 1}))
	ctx := context.Background()
	_, err := c.ListNamespaces(ctx)
	if e := asError(t, err); e.Code != "body_too_large" || e.RequestID != "hdr-1" || !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("reason fallback: %+v", e)
	}
	_, err = c.ListNamespaces(ctx)
	if e := asError(t, err); e.Code != "unknown" || string(e.Body) != "plain failure" || !errors.Is(err, ErrInternal) {
		t.Fatalf("non-JSON body: %+v", e)
	}
	_, err = c.ListNamespaces(ctx)
	if e := asError(t, err); e.Code != "brand_new_code" || e.Details["x"] != float64(1) || e.RequestID != "req-1" {
		t.Fatalf("unknown code: %+v", e)
	}
}

func TestRequestShape(t *testing.T) {
	f := newFake(t, func(_ int, _ recorded, w http.ResponseWriter) {
		writeJSON(w, 200, map[string]any{"results": []any{}, "surprise": true})
	})
	c := testClient(t, f.URL, WithHeader("X-Extra", "1"))
	_, err := c.Namespace("my ns").Search(context.Background(), SearchRequest{QueryText: "q", MaxResults: Ptr[int64](5)})
	if err != nil {
		t.Fatal(err)
	}
	r := f.requests()[0]
	if r.Path != "/v1/namespaces/my%20ns/search" {
		t.Errorf("path = %q", r.Path)
	}
	if r.Header.Get("Authorization") != "Bearer sk-test" || r.Header.Get("X-Extra") != "1" {
		t.Errorf("headers = %v", r.Header)
	}
	if ua := r.Header.Get("User-Agent"); !strings.HasPrefix(ua, "kosha-go/"+Version+" (go") {
		t.Errorf("user agent = %q", ua)
	}
	if len(r.Body) != 2 || r.Body["query_text"] != "q" || r.Body["max_results"] != float64(5) {
		t.Errorf("body = %v", r.Body)
	}
}

func TestConfig(t *testing.T) {
	t.Setenv("KOSHA_API_KEY", "")
	t.Setenv("KOSHA_HOST", "")
	if _, err := NewClient(WithBaseURL("http://x")); !errors.Is(err, ErrConfig) {
		t.Fatalf("missing key: %v", err)
	}
	if _, err := NewClient(WithAPIKey("k")); !errors.Is(err, ErrConfig) {
		t.Fatalf("missing URL: %v", err)
	}
	t.Setenv("KOSHA_API_KEY", "k")
	t.Setenv("KOSHA_HOST", "http://x")
	if _, err := NewClient(); err != nil {
		t.Fatal(err)
	}
}

func TestIsIdempotent(t *testing.T) {
	for _, c := range []struct {
		method, path string
		want         bool
	}{
		{"GET", "/v1/namespaces/a/stats", true},
		{"POST", "/v1/namespaces/a/search", true},
		{"POST", "/v1/namespaces/a/documents", true},
		{"POST", "/v1/namespaces/a/replace", true},
		{"POST", "/v1/namespaces/a/delete", true},
		{"POST", "/v1/namespaces/a/flush", true},
		{"DELETE", "/v1/namespaces/a", true},
		{"POST", "/v1/namespaces", false},
		{"POST", "/v1/namespaces/a/schema", false},
		{"POST", "/v1/namespaces/a/branch", false},
		{"DELETE", "/v1/namespaces/a/branch", false},
		{"POST", "/v1/namespaces/a/shards", false},
	} {
		if got := IsIdempotent(c.method, c.path); got != c.want {
			t.Errorf("IsIdempotent(%s %s) = %v", c.method, c.path, got)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 21, 7, 28, 0, 0, time.UTC)
	if d, ok := parseRetryAfter("2", now); !ok || d != 2*time.Second {
		t.Errorf("seconds: %v %v", d, ok)
	}
	if d, ok := parseRetryAfter("Wed, 21 Oct 2026 07:28:03 GMT", now); !ok || d != 3*time.Second {
		t.Errorf("date: %v %v", d, ok)
	}
	if _, ok := parseRetryAfter("soon", now); ok {
		t.Error("garbage parsed")
	}
}
