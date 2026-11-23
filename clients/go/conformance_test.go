//go:build conformance

// The shared SDK conformance suite (clients/conformance), Go runner:
//
//	go test -tags conformance -run Conformance ./...
//
// Skipped unless KOSHA_CONFORMANCE_URL is set; see clients/conformance/README.md.
// The Python and TypeScript runners implement the same ops and matchers.
package kosha

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const confLang = "go"

var (
	confRoot    = filepath.Join("..", "conformance")
	confURL     = os.Getenv("KOSHA_CONFORMANCE_URL")
	confControl = os.Getenv("KOSHA_CONFORMANCE_CONTROL")
	confRunID   = envOr("KOSHA_CONFORMANCE_RUN_ID", strconv.FormatInt(time.Now().UnixMilli()%2176782336, 16))
	confKeys    = map[string]string{
		"default": envOr("KOSHA_CONFORMANCE_API_KEY", "sk-kosha-dev"),
		"small":   envOr("KOSHA_CONFORMANCE_SMALL_KEY", "sk-conformance-small"),
		"limited": envOr("KOSHA_CONFORMANCE_LIMITED_KEY", "sk-conformance-limited"),
		"invalid": "sk-conformance-invalid",
	}
	confBig = strings.Repeat("x", 5000)
)

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

type confStep struct {
	Op     string         `json:"op"`
	Args   map[string]any `json:"args"`
	Client *struct {
		TimeoutMs          int  `json:"timeout_ms"`
		MaxAttempts        int  `json:"max_attempts"`
		TotalBudgetMs      int  `json:"total_budget_ms"`
		RetryNonIdempotent bool `json:"retry_non_idempotent"`
	} `json:"client"`
	Expect map[string]any `json:"expect"`
}

type confCase struct {
	ID     string     `json:"id"`
	Tenant string     `json:"tenant"`
	Setup  []confStep `json:"setup"`
	Faults []any      `json:"faults"`
	Steps  []confStep `json:"steps"`
}

type confResult struct {
	IDs       []string
	HasIDs    bool
	TotalHits *int64
	Count     *int64
	Indexed   *int64
	Batches   *int64
	FailedIDs []string
}

func withIDs(ids []string) confResult {
	n := int64(len(ids))
	if ids == nil {
		ids = []string{}
	}
	return confResult{IDs: ids, HasIDs: true, Count: &n}
}

func i64(v int64) *int64 { return &v }

// ── Generators and placeholders ─────────────────────────────────────────

var tmplRe = regexp.MustCompile(`\{i(?:%(\d+))?(?::0(\d+))?\}`)

func render(tmpl string, i int) string {
	return tmplRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		g := tmplRe.FindStringSubmatch(m)
		n := i
		if g[1] != "" {
			mod, _ := strconv.Atoi(g[1])
			n = i % mod
		}
		s := strconv.Itoa(n)
		if g[2] != "" {
			w, _ := strconv.Atoi(g[2])
			for len(s) < w {
				s = "0" + s
			}
		}
		return s
	})
}

func generate(spec map[string]any) []Document {
	count := int(spec["count"].(float64))
	idT := spec["id"].(string)
	fields, _ := spec["fields"].(map[string]any)
	names := make([]string, 0, len(fields))
	for n := range fields {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]Document, count)
	for i := range count {
		d := Document{Id: render(idT, i), Fields: []Field{}}
		for _, n := range names {
			f := fields[n].(map[string]any)
			d.Fields = append(d.Fields, Field{Name: n, FieldType: FieldType(f["type"].(string)), Value: render(f["value"].(string), i)})
		}
		out[i] = d
	}
	return out
}

func substitute(v any, ns string) any {
	switch t := v.(type) {
	case string:
		return strings.ReplaceAll(strings.ReplaceAll(t, "$NS", ns), "$BIG", confBig)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = substitute(e, ns)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = substitute(e, ns)
		}
		return out
	}
	return v
}

// convert re-decodes a JSON value into a typed struct.
func convert[T any](v any) T {
	var out T
	raw, _ := json.Marshal(v)
	if err := json.Unmarshal(raw, &out); err != nil {
		panic(fmt.Sprintf("conformance: bad args %s: %v", raw, err))
	}
	return out
}

func documents(v any) []Document {
	if m, ok := v.(map[string]any); ok {
		if g, ok := m["generate"].(map[string]any); ok {
			return generate(g)
		}
	}
	return convert[[]Document](v)
}

func intArg(a map[string]any, k string) int {
	if f, ok := a[k].(float64); ok {
		return int(f)
	}
	return 0
}

// ── Ops ─────────────────────────────────────────────────────────────────

type confOp func(ctx context.Context, c *Client, ns string, a map[string]any, o []CallOption) (confResult, error)

func bulkResult(r *BulkResult) confResult {
	failed := []string{}
	for _, f := range r.Failures {
		failed = append(failed, f.IDs...)
	}
	return confResult{Indexed: i64(r.Indexed), Batches: i64(int64(r.Batches)), FailedIDs: failed}
}

var confOps = map[string]confOp{
	"create_namespace": func(ctx context.Context, c *Client, ns string, a map[string]any, o []CallOption) (confResult, error) {
		var schema *NamespaceSchema
		if s, ok := a["schema"]; ok {
			v := convert[NamespaceSchema](s)
			schema = &v
		}
		_, err := c.CreateNamespace(ctx, ns, schema, o...)
		return confResult{}, err
	},
	"list_namespaces": func(ctx context.Context, c *Client, _ string, _ map[string]any, o []CallOption) (confResult, error) {
		r, err := c.ListNamespaces(ctx, o...)
		if err != nil {
			return confResult{}, err
		}
		var ids []string
		if r.Namespaces != nil {
			for _, n := range *r.Namespaces {
				if n.Name != nil {
					ids = append(ids, *n.Name)
				}
			}
		}
		return withIDs(ids), nil
	},
	"delete_namespace": func(ctx context.Context, c *Client, ns string, _ map[string]any, o []CallOption) (confResult, error) {
		_, err := c.DeleteNamespace(ctx, ns, o...)
		return confResult{}, err
	},
	"put_schema": func(ctx context.Context, c *Client, ns string, a map[string]any, o []CallOption) (confResult, error) {
		_, err := c.Namespace(ns).PutSchema(ctx, convert[NamespaceSchema](a["schema"]), o...)
		return confResult{}, err
	},
	"get_schema": func(ctx context.Context, c *Client, ns string, _ map[string]any, o []CallOption) (confResult, error) {
		r, err := c.Namespace(ns).GetSchema(ctx, o...)
		if err != nil {
			return confResult{}, err
		}
		var ids []string
		if r.Schema != nil && r.Schema.Fields != nil {
			for k := range *r.Schema.Fields {
				ids = append(ids, k)
			}
		}
		sort.Strings(ids)
		return withIDs(ids), nil
	},
	"upsert": func(ctx context.Context, c *Client, ns string, a map[string]any, o []CallOption) (confResult, error) {
		r, err := c.Namespace(ns).Upsert(ctx, documents(a["documents"]), o...)
		if err != nil {
			return confResult{}, err
		}
		return confResult{Indexed: r.IndexedCount}, nil
	},
	"merge": func(ctx context.Context, c *Client, ns string, a map[string]any, o []CallOption) (confResult, error) {
		r, err := c.Namespace(ns).Merge(ctx, documents(a["documents"]), o...)
		if err != nil {
			return confResult{}, err
		}
		return confResult{Indexed: r.IndexedCount}, nil
	},
	"exists": func(ctx context.Context, c *Client, ns string, a map[string]any, o []CallOption) (confResult, error) {
		r, err := c.Namespace(ns).Exists(ctx, convert[[]string](a["ids"]), o...)
		if err != nil {
			return confResult{}, err
		}
		var ids []string
		if r.Existing != nil {
			ids = *r.Existing
		}
		return withIDs(ids), nil
	},
	"search": func(ctx context.Context, c *Client, ns string, a map[string]any, o []CallOption) (confResult, error) {
		r, err := c.Namespace(ns).Search(ctx, convert[SearchRequest](a["body"]), o...)
		if err != nil {
			return confResult{}, err
		}
		var ids []string
		if r.Results != nil {
			for _, h := range *r.Results {
				ids = append(ids, deref(h.DocId))
			}
		}
		res := withIDs(ids)
		res.TotalHits = r.TotalHits
		return res, nil
	},
	"search_all": func(ctx context.Context, c *Client, ns string, a map[string]any, o []CallOption) (confResult, error) {
		var ids []string
		for h, err := range c.Namespace(ns).SearchAll(ctx, convert[SearchRequest](a["body"]), intArg(a, "page_size"), o...) {
			if err != nil {
				return confResult{}, err
			}
			ids = append(ids, deref(h.DocId))
		}
		return withIDs(ids), nil
	},
	"bulk_index": func(ctx context.Context, c *Client, ns string, a map[string]any, o []CallOption) (confResult, error) {
		opts := BulkOptions{
			MaxDocsPerBatch:  intArg(a, "max_docs_per_batch"),
			MaxBytesPerBatch: intArg(a, "max_bytes_per_batch"),
			Concurrency:      intArg(a, "concurrency"),
			ContinueOnError:  a["on_error"] == "continue",
		}
		r, err := c.Namespace(ns).BulkIndex(ctx, documents(a["documents"]), opts, o...)
		if r == nil {
			return confResult{}, err
		}
		return bulkResult(r), err
	},
	"delete_by_query": func(ctx context.Context, c *Client, ns string, a map[string]any, o []CallOption) (confResult, error) {
		r, err := c.Namespace(ns).DeleteByQuery(ctx, convert[DeleteByQueryRequest](a["body"]), o...)
		if err != nil {
			return confResult{}, err
		}
		return confResult{Count: r.Deleted}, nil
	},
	"flush": func(ctx context.Context, c *Client, ns string, _ map[string]any, o []CallOption) (confResult, error) {
		_, err := c.Namespace(ns).Flush(ctx, o...)
		return confResult{}, err
	},
	"stats": func(ctx context.Context, c *Client, ns string, _ map[string]any, o []CallOption) (confResult, error) {
		r, err := c.Namespace(ns).Stats(ctx, o...)
		if err != nil {
			return confResult{}, err
		}
		return confResult{Count: r.Documents}, nil
	},
	"raw_request": func(ctx context.Context, c *Client, _ string, a map[string]any, o []CallOption) (confResult, error) {
		var out map[string]any
		return confResult{}, c.Do(ctx, a["method"].(string), a["path"].(string), a["body"], &out, o...)
	},
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ── Matchers ────────────────────────────────────────────────────────────

type confMeta struct {
	elapsed  time.Duration
	attempts func() (int, error)
}

type confMatcher func(want any, r confResult, err error, m confMeta) error

func expectEq(got, want any) error {
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("got %v, want %v", got, want)
	}
	return nil
}

func wantInt(v any) int64 { return int64(v.(float64)) }

func intEq(got *int64, want any) error {
	if got == nil {
		return fmt.Errorf("got nothing, want %v", want)
	}
	return expectEq(*got, wantInt(want))
}

func sortedCopy(s []string) []string {
	out := slices.Clone(s)
	if out == nil {
		out = []string{}
	}
	sort.Strings(out)
	return out
}

func errorKind(err error) string {
	var ce *ConfigError
	var he *Error
	var te *TransportError
	switch {
	case errors.As(err, &ce):
		return "config"
	case errors.As(err, &he):
		return "http"
	case errors.As(err, &te):
		return "transport"
	}
	return "unknown"
}

var confMatchers = map[string]confMatcher{
	"ok": func(any, confResult, error, confMeta) error { return nil },
	"ids": func(want any, r confResult, _ error, _ confMeta) error {
		var ids []string
		if m, ok := want.(map[string]any); ok {
			for _, d := range generate(m["generate"].(map[string]any)) {
				ids = append(ids, d.Id)
			}
		} else {
			ids = convert[[]string](want)
		}
		if ids == nil {
			ids = []string{}
		}
		return expectEq(r.IDs, ids)
	},
	"ids_unordered": func(want any, r confResult, _ error, _ confMeta) error {
		return expectEq(sortedCopy(r.IDs), sortedCopy(convert[[]string](want)))
	},
	"ids_include": func(want any, r confResult, _ error, _ confMeta) error {
		for _, id := range convert[[]string](want) {
			if !slices.Contains(r.IDs, id) {
				return fmt.Errorf("%q not in %v", id, r.IDs)
			}
		}
		return nil
	},
	"total_hits": func(want any, r confResult, _ error, _ confMeta) error { return intEq(r.TotalHits, want) },
	"count":      func(want any, r confResult, _ error, _ confMeta) error { return intEq(r.Count, want) },
	"unique": func(_ any, r confResult, _ error, _ confMeta) error {
		seen := map[string]bool{}
		for _, id := range r.IDs {
			if seen[id] {
				return fmt.Errorf("duplicate id %q", id)
			}
			seen[id] = true
		}
		return nil
	},
	"indexed": func(want any, r confResult, _ error, _ confMeta) error { return intEq(r.Indexed, want) },
	"batches": func(want any, r confResult, _ error, _ confMeta) error { return intEq(r.Batches, want) },
	"failed_ids": func(want any, r confResult, _ error, _ confMeta) error {
		return expectEq(sortedCopy(r.FailedIDs), sortedCopy(convert[[]string](want)))
	},
	"error": func(want any, _ confResult, err error, _ confMeta) error {
		if err == nil {
			return errors.New("expected an error, the call succeeded")
		}
		w := want.(map[string]any)
		if k, ok := w["kind"]; ok && errorKind(err) != k {
			return fmt.Errorf("error kind %s (%v)", errorKind(err), err)
		}
		var he *Error
		errors.As(err, &he)
		if he == nil {
			he = &Error{}
		}
		if v, ok := w["status"]; ok && int64(he.StatusCode) != wantInt(v) {
			return fmt.Errorf("status %d (%v)", he.StatusCode, err)
		}
		if v, ok := w["code"]; ok && string(he.Code) != v {
			return fmt.Errorf("code %q (%v)", he.Code, err)
		}
		if v, ok := w["retryable"]; ok && he.Retryable != v {
			return fmt.Errorf("retryable %v (%v)", he.Retryable, err)
		}
		if v, ok := w["has_request_id"]; ok && (he.RequestID != "") != v {
			return fmt.Errorf("request id %q (%v)", he.RequestID, err)
		}
		if v, ok := w["retry_after_ms"]; ok && he.RetryAfter.Milliseconds() != wantInt(v) {
			return fmt.Errorf("retry after %v (%v)", he.RetryAfter, err)
		}
		return nil
	},
	"attempts": func(want any, _ confResult, _ error, m confMeta) error {
		n, err := m.attempts()
		if err != nil {
			return err
		}
		return expectEq(int64(n), wantInt(want))
	},
	"attempts_min": func(want any, _ confResult, _ error, m confMeta) error {
		n, err := m.attempts()
		if err != nil {
			return err
		}
		if int64(n) < wantInt(want) {
			return fmt.Errorf("attempts %d < %v", n, want)
		}
		return nil
	},
	"min_elapsed_ms": func(want any, _ confResult, _ error, m confMeta) error {
		if m.elapsed.Milliseconds() < wantInt(want) {
			return fmt.Errorf("elapsed %v < %vms", m.elapsed, want)
		}
		return nil
	},
	"max_elapsed_ms": func(want any, _ confResult, _ error, m confMeta) error {
		if m.elapsed.Milliseconds() > wantInt(want) {
			return fmt.Errorf("elapsed %v > %vms", m.elapsed, want)
		}
		return nil
	},
}

// ── Runner ──────────────────────────────────────────────────────────────

func controlCall(method, path string, body any, out any) error {
	var rd *bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = bytes.NewReader(raw)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, strings.TrimRight(confControl, "/")+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("faultproxy %s %s: %s", method, path, resp.Status)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func loadConfCases(t *testing.T) []confCase {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(confRoot, "cases", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no case files: %v", err)
	}
	sort.Strings(files)
	var all []confCase
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var cs []confCase
		if err := json.Unmarshal(raw, &cs); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		all = append(all, cs...)
	}
	return all
}

func needsProxy(c confCase) bool {
	if len(c.Faults) > 0 {
		return true
	}
	for _, s := range c.Steps {
		if _, ok := s.Expect["attempts"]; ok {
			return true
		}
		if _, ok := s.Expect["attempts_min"]; ok {
			return true
		}
	}
	return false
}

func confClient(t *testing.T, apiKey string, s *confStep) *Client {
	t.Helper()
	opts := []Option{WithBaseURL(confURL), WithAPIKey(apiKey)}
	if s != nil && s.Client != nil {
		if s.Client.TimeoutMs > 0 {
			opts = append(opts, WithTimeout(time.Duration(s.Client.TimeoutMs)*time.Millisecond))
		}
		opts = append(opts, WithRetryPolicy(RetryPolicy{
			MaxAttempts: s.Client.MaxAttempts,
			TotalBudget: time.Duration(s.Client.TotalBudgetMs) * time.Millisecond,
		}))
	}
	c, err := NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func runConfOp(c *Client, s confStep, ns, tag string) (confResult, error) {
	args, _ := substitute(s.Args, ns).(map[string]any)
	if args == nil {
		args = map[string]any{}
	}
	target := ns
	if v, ok := args["namespace"].(string); ok {
		target = v
		delete(args, "namespace")
	}
	opts := []CallOption{WithCallHeader("X-Conformance-Case", tag)}
	if s.Client != nil && s.Client.RetryNonIdempotent {
		opts = append(opts, WithRetryNonIdempotent())
	}
	op, ok := confOps[s.Op]
	if !ok {
		return confResult{}, fmt.Errorf("unknown op %q", s.Op)
	}
	return op(context.Background(), c, target, args, opts)
}

func TestConformanceRunnerImplementsSchema(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(confRoot, "case.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Defs struct {
			Op     struct{ Enum []string } `json:"op"`
			Expect struct {
				Properties map[string]any `json:"properties"`
			} `json:"expect"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	var ops, matchers, wantMatchers []string
	for k := range confOps {
		ops = append(ops, k)
	}
	for k := range confMatchers {
		matchers = append(matchers, k)
	}
	for k := range schema.Defs.Expect.Properties {
		wantMatchers = append(wantMatchers, k)
	}
	if err := expectEq(sortedCopy(ops), sortedCopy(schema.Defs.Op.Enum)); err != nil {
		t.Errorf("ops: %v", err)
	}
	if err := expectEq(sortedCopy(matchers), sortedCopy(wantMatchers)); err != nil {
		t.Errorf("matchers: %v", err)
	}
	if render("d{i:04}", 7) != "d0007" || render("t{i%7}", 23) != "t2" || render("x{i%7:02}-{i}", 23) != "x02-23" {
		t.Error("template rendering")
	}
}

func TestConformance(t *testing.T) {
	cases := loadConfCases(t)
	slug := regexp.MustCompile(`[^a-z0-9]+`)
	for _, c := range cases {
		t.Run(c.ID, func(t *testing.T) {
			if confURL == "" {
				t.Skip("KOSHA_CONFORMANCE_URL is not set")
			}
			if needsProxy(c) && confControl == "" {
				t.Skip("needs faultproxy (KOSHA_CONFORMANCE_CONTROL)")
			}
			key := fmt.Sprintf("%s-%s-%s", confLang, confRunID, c.ID)
			ns := fmt.Sprintf("conf-%s-%s-%s", confLang, confRunID, slug.ReplaceAllString(c.ID, "-"))
			tenant := c.Tenant
			if tenant == "" {
				tenant = "default"
			}
			apiKey := confKeys[tenant]
			if len(c.Faults) > 0 {
				if err := controlCall("POST", "/faults", map[string]any{"case": key, "rules": c.Faults}, nil); err != nil {
					t.Fatal(err)
				}
			}
			defer func() {
				if len(c.Faults) > 0 {
					_ = controlCall("DELETE", "/faults?case="+url.QueryEscape(key), nil, nil)
				}
				cl, _ := NewClient(WithBaseURL(confURL), WithAPIKey(apiKey), WithRetryPolicy(RetryPolicy{MaxAttempts: 1}))
				_, _ = runConfOp(cl, confStep{Op: "delete_namespace"}, ns, key+"#cleanup")
			}()

			setup := confClient(t, apiKey, nil)
			for _, s := range c.Setup {
				if _, err := runConfOp(setup, s, ns, key+"#setup"); err != nil {
					t.Fatalf("setup %s failed: %v", s.Op, err)
				}
			}
			for i, s := range c.Steps {
				tag := fmt.Sprintf("%s#%d", key, i)
				want, _ := substitute(s.Expect, ns).(map[string]any)
				start := time.Now()
				res, err := runConfOp(confClient(t, apiKey, &s), s, ns, tag)
				meta := confMeta{elapsed: time.Since(start), attempts: func() (int, error) {
					var log struct {
						Requests []struct{ Step string } `json:"requests"`
					}
					if err := controlCall("GET", "/log?case="+url.QueryEscape(key), nil, &log); err != nil {
						return 0, err
					}
					n := 0
					for _, r := range log.Requests {
						if key+"#"+r.Step == tag {
							n++
						}
					}
					return n, nil
				}}
				if _, ok := want["error"]; !ok && err != nil {
					t.Fatalf("step %d (%s) failed: %v", i, s.Op, err)
				}
				names := make([]string, 0, len(want))
				for k := range want {
					names = append(names, k)
				}
				sort.Strings(names)
				for _, name := range names {
					m, ok := confMatchers[name]
					if !ok {
						t.Fatalf("unknown matcher %q", name)
					}
					if merr := m(want[name], res, err, meta); merr != nil {
						t.Fatalf("step %d (%s) %s: %v", i, s.Op, name, merr)
					}
				}
			}
		})
	}
}
