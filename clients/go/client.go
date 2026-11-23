package kosha

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"
)

// Client is a Kosha /v1 API client. It is safe for concurrent use.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
	retry      RetryPolicy
	timeout    time.Duration
	header     http.Header
	userAgent  string
}

// Option configures a Client.
type Option func(*Client)

// WithAPIKey sets the tenant API key (default: $KOSHA_API_KEY).
func WithAPIKey(key string) Option { return func(c *Client) { c.apiKey = key } }

// WithBaseURL sets the server URL, e.g. https://kosha.example.com (default: $KOSHA_HOST).
func WithBaseURL(u string) Option { return func(c *Client) { c.baseURL = u } }

// WithHTTPClient sets the http.Client (proxies, TLS, tracing transports).
// Its Timeout should be zero or longer than the per-attempt timeout.
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.httpClient = h } }

// WithRetryPolicy replaces DefaultRetryPolicy; zero fields keep the default.
func WithRetryPolicy(p RetryPolicy) Option {
	return func(c *Client) {
		d := DefaultRetryPolicy
		if p.MaxAttempts > 0 {
			d.MaxAttempts = p.MaxAttempts
		}
		if p.BaseDelay > 0 {
			d.BaseDelay = p.BaseDelay
		}
		if p.MaxDelay > 0 {
			d.MaxDelay = p.MaxDelay
		}
		if p.MaxRetryAfter > 0 {
			d.MaxRetryAfter = p.MaxRetryAfter
		}
		if p.TotalBudget > 0 {
			d.TotalBudget = p.TotalBudget
		}
		c.retry = d
	}
}

// WithTimeout sets the per-attempt timeout (default 30s).
func WithTimeout(d time.Duration) Option { return func(c *Client) { c.timeout = d } }

// WithHeader adds a header to every request.
func WithHeader(key, value string) Option { return func(c *Client) { c.header.Add(key, value) } }

// NewClient builds a client. The API key and base URL come from the
// options, else from $KOSHA_API_KEY and $KOSHA_HOST; it is an error if
// either is still missing.
func NewClient(opts ...Option) (*Client, error) {
	c := &Client{
		apiKey:     os.Getenv("KOSHA_API_KEY"),
		baseURL:    os.Getenv("KOSHA_HOST"),
		httpClient: http.DefaultClient,
		retry:      DefaultRetryPolicy,
		timeout:    30 * time.Second,
		header:     http.Header{},
		userAgent:  fmt.Sprintf("kosha-go/%s (%s; %s/%s)", Version, runtime.Version(), runtime.GOOS, runtime.GOARCH),
	}
	for _, o := range opts {
		o(c)
	}
	if c.apiKey == "" {
		return nil, configErrorf("no API key: use WithAPIKey or set KOSHA_API_KEY")
	}
	if c.baseURL == "" {
		return nil, configErrorf("no server URL: use WithBaseURL or set KOSHA_HOST")
	}
	if _, err := url.Parse(c.baseURL); err != nil {
		return nil, configErrorf("bad server URL: %v", err)
	}
	c.baseURL = strings.TrimRight(c.baseURL, "/")
	return c, nil
}

// CallOption overrides a client setting for one call.
type CallOption func(*callConfig)

type callConfig struct {
	maxAttempts        int
	timeout            time.Duration
	retryNonIdempotent bool
	header             http.Header
}

// WithMaxAttempts sets the attempts for one call (1 disables retries).
func WithMaxAttempts(n int) CallOption { return func(c *callConfig) { c.maxAttempts = n } }

// WithCallTimeout sets the per-attempt timeout for one call.
func WithCallTimeout(d time.Duration) CallOption { return func(c *callConfig) { c.timeout = d } }

// WithRetryNonIdempotent lets a timed-out or reset non-idempotent call
// (namespace create, schema put, ...) be retried.
func WithRetryNonIdempotent() CallOption { return func(c *callConfig) { c.retryNonIdempotent = true } }

// WithCallHeader adds a header to one call.
func WithCallHeader(key, value string) CallOption {
	return func(c *callConfig) {
		if c.header == nil {
			c.header = http.Header{}
		}
		c.header.Add(key, value)
	}
}

// Do sends any /v1 request through the client's retry and error handling,
// for routes without a typed method. body (if non-nil) is sent as JSON and
// a 2xx response is decoded into out (if non-nil).
func (c *Client) Do(ctx context.Context, method, path string, body, out any, opts ...CallOption) error {
	cfg := callConfig{maxAttempts: c.retry.MaxAttempts, timeout: c.timeout}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.maxAttempts < 1 {
		cfg.maxAttempts = 1
	}
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return configErrorf("encoding request body: %v", err)
		}
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	idempotent := IsIdempotent(method, path)
	started := time.Now()

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return &TransportError{Method: method, Path: path, Attempts: attempt - 1, Err: err}
		}
		status, header, respBody, err := c.attempt(ctx, method, path, payload, cfg)
		var (
			failure  error
			retry    bool
			delay    time.Duration
			hasDelay bool
		)
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return &TransportError{Method: method, Path: path, Attempts: attempt, Err: ctx.Err()}
			}
			te := &TransportError{Method: method, Path: path, Attempts: attempt, Err: err, Timeout: errors.Is(err, context.DeadlineExceeded)}
			failure = te
			retry = (!te.Timeout && beforeWrite(err)) || idempotent || cfg.retryNonIdempotent
		case status >= 200 && status < 300:
			if out == nil || len(bytes.TrimSpace(respBody)) == 0 {
				return nil
			}
			if err := json.Unmarshal(respBody, out); err != nil {
				return &Error{StatusCode: status, Code: "unknown", Message: "invalid JSON in response: " + err.Error(), Attempts: attempt, Body: respBody}
			}
			return nil
		default:
			delay, hasDelay = serverDelay(respBody, header, time.Now())
			e := newError(status, header, respBody, delay)
			e.Attempts = attempt
			failure = e
			retry = retryableStatus(status, respBody, idempotent, idempotent || cfg.retryNonIdempotent)
		}

		if !retry || attempt >= cfg.maxAttempts {
			return failure
		}
		if hasDelay {
			if delay > c.retry.MaxRetryAfter {
				return failure
			}
			delay += rand.N(100 * time.Millisecond)
		} else {
			delay = c.retry.backoff(attempt - 1)
		}
		if time.Since(started)+delay > c.retry.TotalBudget {
			return failure
		}
		if err := sleepCtx(ctx, delay); err != nil {
			return &TransportError{Method: method, Path: path, Attempts: attempt, Err: err}
		}
	}
}

func (c *Client) attempt(ctx context.Context, method, path string, payload []byte, cfg callConfig) (int, http.Header, []byte, error) {
	actx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	var reader io.Reader
	if payload != nil {
		// A bytes.Reader sets GetBody, so redirects and retries replay it.
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(actx, method, c.baseURL+path, reader)
	if err != nil {
		return 0, nil, nil, err
	}
	for k, vs := range c.header {
		req.Header[k] = append([]string(nil), vs...)
	}
	for k, vs := range cfg.header {
		req.Header[k] = append([]string(nil), vs...)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, body, nil
}

// beforeWrite reports whether the request failed before any byte of it was
// sent (DNS, refused connection, TLS handshake): safe to replay for every
// operation (RETRY.md rule 3).
func beforeWrite(err error) bool {
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return true
	}
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// Namespace returns a handle on one namespace of the key's tenant. It sends nothing.
func (c *Client) Namespace(name string) *Namespace {
	return &Namespace{c: c, name: name}
}

// CreateNamespace creates a namespace, with an optional schema.
func (c *Client) CreateNamespace(ctx context.Context, name string, schema *NamespaceSchema, opts ...CallOption) (*CreateNamespaceResponse, error) {
	var out CreateNamespaceResponse
	err := c.Do(ctx, http.MethodPost, "/v1/namespaces", CreateNamespaceRequest{Namespace: name, Schema: schema}, &out, opts...)
	return result(&out, err)
}

// ListNamespaces lists the tenant's namespaces.
func (c *Client) ListNamespaces(ctx context.Context, opts ...CallOption) (*ListNamespacesResponse, error) {
	var out ListNamespacesResponse
	return result(&out, c.Do(ctx, http.MethodGet, "/v1/namespaces", nil, &out, opts...))
}

// DeleteNamespace deletes a namespace and its documents.
func (c *Client) DeleteNamespace(ctx context.Context, name string, opts ...CallOption) (*DeleteNamespaceResponse, error) {
	var out DeleteNamespaceResponse
	return result(&out, c.Do(ctx, http.MethodDelete, "/v1/namespaces/"+url.PathEscape(name), nil, &out, opts...))
}

func result[T any](out *T, err error) (*T, error) {
	if err != nil {
		return nil, err
	}
	return out, nil
}
