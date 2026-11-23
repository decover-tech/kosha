package kosha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func mkDoc(i int, tag string) Document {
	fields := []Field{{Name: "title", FieldType: FieldTypeText, Value: fmt.Sprintf("title %d", i)}}
	if tag != "" {
		fields = append(fields, Field{Name: "tag", FieldType: FieldTypeKeyword, Value: tag})
	}
	return Document{Id: fmt.Sprintf("d%04d", i), Fields: fields}
}

// fakeSearch sorts docs by the request's keys, applies search_after and pages.
func fakeSearch(docs []Document, body map[string]any) map[string]any {
	type key struct {
		name string
		dir  int
	}
	var keys []key
	for _, e := range body["sort"].([]any) {
		for k, v := range e.(map[string]any) {
			dir := 1
			if o, _ := v.(map[string]any)["order"].(string); o == "desc" {
				dir = -1
			}
			keys = append(keys, key{k, dir})
		}
	}
	value := func(d Document, k string) string {
		if k == "_id" {
			return d.Id
		}
		for _, f := range d.Fields {
			if f.Name == k {
				return f.Value
			}
		}
		return ""
	}
	vals := func(d Document) []string {
		out := make([]string, len(keys))
		for i, k := range keys {
			out[i] = value(d, k.name)
		}
		return out
	}
	cmp := func(a, b []string) int {
		for i, k := range keys {
			if c := strings.Compare(a[i], b[i]); c != 0 {
				return c * k.dir
			}
		}
		return 0
	}
	rows := slices.Clone(docs)
	sort.SliceStable(rows, func(i, j int) bool { return cmp(vals(rows[i]), vals(rows[j])) < 0 })
	if sa, ok := body["search_after"].([]any); ok {
		after := make([]string, len(sa))
		for i, v := range sa {
			after[i] = v.(string)
		}
		rows = slices.DeleteFunc(rows, func(d Document) bool { return cmp(vals(d), after) <= 0 })
	}
	limit := int(body["max_results"].(float64))
	if len(rows) > limit {
		rows = rows[:limit]
	}
	var include []any
	if inc, ok := body["source_includes"].([]any); ok {
		include = inc
	}
	results := []any{}
	for _, d := range rows {
		fields := []any{}
		for _, f := range d.Fields {
			if include == nil || slices.Contains(include, any(f.Name)) {
				fields = append(fields, map[string]any{"name": f.Name, "field_type": f.FieldType, "value": f.Value})
			}
		}
		results = append(results, map[string]any{"doc_id": d.Id, "score": 1.0, "fields": fields})
	}
	return map[string]any{"results": results, "total_hits": len(docs)}
}

func collect(t *testing.T, seq func(func(Hit, error) bool)) ([]string, error) {
	t.Helper()
	var ids []string
	for h, err := range seq {
		if err != nil {
			return ids, err
		}
		ids = append(ids, *h.DocId)
	}
	return ids, nil
}

func TestSearchAllEveryIDOnceInOrder(t *testing.T) {
	var docs []Document
	for i := range 250 {
		docs = append(docs, mkDoc(i, fmt.Sprintf("t%d", i%7)))
	}
	f := newFake(t, func(_ int, r recorded, w http.ResponseWriter) { writeJSON(w, 200, fakeSearch(docs, r.Body)) })
	ns := testClient(t, f.URL).Namespace("n")
	ctx := context.Background()

	ids, err := collect(t, ns.SearchAll(ctx, SearchRequest{}, 100))
	if err != nil {
		t.Fatal(err)
	}
	want := make([]string, len(docs))
	for i, d := range docs {
		want[i] = d.Id
	}
	if !slices.Equal(ids, want) {
		t.Fatalf("by id: got %d ids, first %v", len(ids), ids[:3])
	}
	reqs := f.requests()
	if len(reqs) != 3 || fmt.Sprint(reqs[1].Body["search_after"]) != "[d0099]" {
		t.Fatalf("requests: %d, second cursor %v", len(reqs), reqs[1].Body["search_after"])
	}

	req := SearchRequest{Sort: &[]map[string]any{{"tag": map[string]any{"order": "asc"}}}, SourceIncludes: &[]string{"title"}}
	ids, err = collect(t, ns.SearchAll(ctx, req, 100))
	if err != nil {
		t.Fatal(err)
	}
	sorted := slices.Clone(docs)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Fields[1].Value+sorted[i].Id < sorted[j].Fields[1].Value+sorted[j].Id
	})
	want = want[:0]
	for _, d := range sorted {
		want = append(want, d.Id)
	}
	if !slices.Equal(ids, want) {
		t.Fatalf("by tag: wrong order")
	}
	last := f.requests()[len(f.requests())-1].Body
	if fmt.Sprint(last["source_includes"]) != "[title tag]" || len(last["sort"].([]any)) != 2 {
		t.Fatalf("last request %v", last)
	}
}

func TestSearchAllRejects(t *testing.T) {
	ns := testClient(t, "http://127.0.0.1:1").Namespace("n")
	hybrid := KoshaServiceSearchBodyModeHybrid
	for _, req := range []SearchRequest{
		{From: Ptr[int64](10)},
		{SearchAfter: &[]string{"x"}},
		{Rerank: &V1Rerank{}},
		{Knn: &V1KnnQuery{}},
		{Mode: &hybrid},
	} {
		if _, err := collect(t, ns.SearchAll(context.Background(), req, 0)); !errors.Is(err, ErrConfig) {
			t.Errorf("%+v: got %v", req, err)
		}
	}
}

func TestSearchAllStuckCursor(t *testing.T) {
	f := newFake(t, func(_ int, _ recorded, w http.ResponseWriter) {
		writeJSON(w, 200, map[string]any{"results": []any{map[string]any{"doc_id": "same"}}})
	})
	_, err := collect(t, testClient(t, f.URL).Namespace("n").SearchAll(context.Background(), SearchRequest{}, 1))
	if !errors.Is(err, ErrConfig) {
		t.Fatalf("got %v", err)
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func indexedHandler(_ int, r recorded, w http.ResponseWriter) {
	writeJSON(w, 200, map[string]any{"indexed_count": len(r.Body["documents"].([]any))})
}

func batchIDs(r recorded) []string {
	var ids []string
	for _, d := range r.Body["documents"].([]any) {
		ids = append(ids, d.(map[string]any)["id"].(string))
	}
	return ids
}

func TestBulkByCount(t *testing.T) {
	f := newFake(t, indexedHandler)
	var docs []Document
	for i := range 250 {
		docs = append(docs, mkDoc(i, ""))
	}
	res, err := testClient(t, f.URL).Namespace("n").BulkIndex(context.Background(), docs, BulkOptions{MaxDocsPerBatch: 40})
	if err != nil {
		t.Fatal(err)
	}
	if res.Indexed != 250 || res.Batches != 7 || len(res.Failures) != 0 {
		t.Fatalf("result %+v", res)
	}
}

func TestBulkByteCap(t *testing.T) {
	f := newFake(t, indexedHandler)
	big := Document{Id: "big", Fields: []Field{{Name: "title", FieldType: FieldTypeText, Value: strings.Repeat("x", 5000)}}}
	one := len(mustJSON(mkDoc(1, ""))) + 1
	docs := []Document{mkDoc(1, ""), mkDoc(2, ""), big, mkDoc(3, "")}
	res, err := testClient(t, f.URL).Namespace("n").BulkIndex(context.Background(), docs, BulkOptions{MaxBytesPerBatch: 2*one + 1, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	var got [][]string
	for _, r := range f.requests() {
		got = append(got, batchIDs(r))
	}
	if res.Batches != 3 || fmt.Sprint(got) != "[[d0001 d0002] [big] [d0003]]" {
		t.Fatalf("batches %v", got)
	}
}

func TestBulkFailureStopsAndReports(t *testing.T) {
	f := newFake(t, func(n int, r recorded, w http.ResponseWriter) {
		if slices.Contains(batchIDs(r), "d0005") {
			writeJSON(w, 400, errBody("invalid_argument", false))
			return
		}
		indexedHandler(n, r, w)
	})
	var docs []Document
	for i := range 20 {
		docs = append(docs, mkDoc(i, ""))
	}
	ns := testClient(t, f.URL).Namespace("n")
	res, err := ns.BulkIndex(context.Background(), docs, BulkOptions{MaxDocsPerBatch: 4, Concurrency: 1})
	var be *BulkError
	if !errors.As(err, &be) || !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("got %v", err)
	}
	if len(res.Failures) != 1 || res.Failures[0].BatchIndex != 1 ||
		fmt.Sprint(res.Failures[0].IDs) != "[d0004 d0005 d0006 d0007]" || res.Indexed != 4 || len(f.requests()) != 2 {
		t.Fatalf("result %+v after %d requests", res, len(f.requests()))
	}

	res, err = ns.BulkIndex(context.Background(), docs, BulkOptions{MaxDocsPerBatch: 4, ContinueOnError: true})
	if err != nil || res.Indexed != 16 || res.Batches != 5 || len(res.Failures) != 1 {
		t.Fatalf("continue: %v %+v", err, res)
	}
}

func TestBulkLastOccurrenceWins(t *testing.T) {
	var mu sync.Mutex
	var order []string
	f := newFake(t, func(n int, r recorded, w http.ResponseWriter) {
		ds := r.Body["documents"].([]any)
		if ds[0].(map[string]any)["id"] == "d0000" {
			time.Sleep(100 * time.Millisecond)
		}
		mu.Lock()
		for _, d := range ds {
			m := d.(map[string]any)
			order = append(order, fmt.Sprintf("%s:%s", m["id"], m["fields"].([]any)[0].(map[string]any)["value"]))
		}
		mu.Unlock()
		indexedHandler(n, r, w)
	})
	v := func(i int, val string) Document {
		return Document{Id: fmt.Sprintf("d%04d", i), Fields: []Field{{Name: "title", FieldType: FieldTypeText, Value: val}}}
	}
	docs := []Document{v(0, "a"), v(1, "a"), v(2, "a"), v(0, "b"), v(3, "a"), v(3, "c")}
	if _, err := testClient(t, f.URL).Namespace("n").BulkIndex(context.Background(), docs, BulkOptions{MaxDocsPerBatch: 2, Concurrency: 4}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if slices.Index(order, "d0000:b") < slices.Index(order, "d0000:a") || !slices.Contains(order, "d0003:c") || slices.Contains(order, "d0003:a") {
		t.Fatalf("order %v", order)
	}
}
