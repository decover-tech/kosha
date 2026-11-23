package kosha

import (
	"context"
	"encoding/json"
	"iter"
	"slices"
	"strconv"
	"strings"
)

// DefaultPageSize is SearchAll's page size when neither the pageSize
// argument nor the request's MaxResults sets one.
const DefaultPageSize = 500

// SearchAll yields every hit of req, page by page with search_after.
//
// The sort always ends in an _id tie-break (appended when missing; the
// default sort is _id ascending), sort fields are added to SourceIncludes
// when it is set and removed from SourceExcludes, and pageSize (0: req.MaxResults, else 500) becomes
// max_results. Iteration stops on a short page. A document that exists and
// is unchanged for the whole iteration is yielded exactly once, in sort
// order; this is not a snapshot.
//
// The request may not set From, SearchAfter, Rerank, Knn, Rank or a
// semantic/hybrid Mode; those yield a *ConfigError. After an error the
// iterator stops.
func (n *Namespace) SearchAll(ctx context.Context, req SearchRequest, pageSize int, opts ...CallOption) iter.Seq2[Hit, error] {
	return func(yield func(Hit, error) bool) {
		if pageSize == 0 && req.MaxResults != nil {
			pageSize = int(*req.MaxResults)
		}
		if pageSize == 0 {
			pageSize = DefaultPageSize
		}
		body, keys, descending, err := pagedRequest(req, pageSize)
		if err != nil {
			yield(Hit{}, err)
			return
		}
		var cursor []string
		for {
			if cursor != nil {
				c := cursor
				body.SearchAfter = &c
			}
			page, err := n.Search(ctx, body, opts...)
			if err != nil {
				yield(Hit{}, err)
				return
			}
			var hits []Hit
			if page.Results != nil {
				hits = *page.Results
			}
			for _, h := range hits {
				if !yield(h, nil) {
					return
				}
			}
			if len(hits) < pageSize {
				return
			}
			next := cursorFor(hits[len(hits)-1], keys, descending)
			if cursor != nil && slices.Equal(next, cursor) {
				yield(Hit{}, configErrorf("SearchAll: the cursor did not advance (is a sort field stored and returned?)"))
				return
			}
			cursor = next
		}
	}
}

// sortKey is the field a sort entry names: {"date": {"order": "desc"}}.
func sortKey(entry map[string]any) (string, error) {
	if len(entry) != 1 {
		return "", configErrorf("SearchAll: a sort entry must name exactly one field, got %v", entry)
	}
	for k := range entry {
		return k, nil
	}
	return "", nil
}

// isDescending reports whether a sort entry {"f": {"order": "desc"}} sorts
// its field descending.
func isDescending(entry map[string]any, key string) bool {
	spec, ok := entry[key].(map[string]any)
	return ok && spec["order"] == "desc"
}

// pagedRequest is the request SearchAll sends for each page: the caller's
// sort with an _id tie-break, the sort fields kept in SourceIncludes and
// taken out of SourceExcludes (the cursor is read from them), and
// MaxResults set to the page size. descending[i] is whether keys[i] sorts
// desc.
func pagedRequest(req SearchRequest, pageSize int) (body SearchRequest, keys []string, descending []bool, err error) {
	switch {
	case req.From != nil:
		return req, nil, nil, configErrorf("SearchAll pages with search_after; do not set From")
	case req.SearchAfter != nil:
		return req, nil, nil, configErrorf("SearchAll sets SearchAfter itself")
	case req.Rerank != nil:
		return req, nil, nil, configErrorf("SearchAll cannot page a reranked search")
	case req.Knn != nil || req.Rank != nil ||
		(req.Mode != nil && (*req.Mode == KoshaServiceSearchBodyModeSemantic || *req.Mode == KoshaServiceSearchBodyModeHybrid)):
		return req, nil, nil, configErrorf("SearchAll cannot page a kNN or hybrid search")
	case pageSize < 1:
		return req, nil, nil, configErrorf("page size must be positive")
	}
	var sort []map[string]any
	if req.Sort != nil {
		sort = slices.Clone(*req.Sort)
	}
	keys = make([]string, 0, len(sort)+1)
	descending = make([]bool, 0, len(sort)+1)
	for _, e := range sort {
		k, err := sortKey(e)
		if err != nil {
			return req, nil, nil, err
		}
		keys = append(keys, k)
		descending = append(descending, isDescending(e, k))
	}
	if len(keys) == 0 || keys[len(keys)-1] != "_id" {
		sort = append(sort, map[string]any{"_id": map[string]any{"order": "asc"}})
		keys = append(keys, "_id")
		descending = append(descending, false)
	}
	body = req
	body.Sort = &sort
	body.MaxResults = Ptr(int64(pageSize))
	if req.SourceIncludes != nil {
		include := slices.Clone(*req.SourceIncludes)
		for _, k := range keys {
			if k != "_id" && k != "_score" && !slices.Contains(include, k) {
				include = append(include, k)
			}
		}
		body.SourceIncludes = &include
	}
	if req.SourceExcludes != nil {
		exclude := make([]string, 0, len(*req.SourceExcludes))
		for _, f := range *req.SourceExcludes {
			if !slices.Contains(keys, f) {
				exclude = append(exclude, f)
			}
		}
		body.SourceExcludes = &exclude
	}
	return body, keys, descending, nil
}

// sortValueOf is the value the server sorts a stored field value by: a
// JSON list (a multi-valued keyword) sorts by its smallest element
// ascending and its largest descending, and an empty list as missing ("").
// Mirrors the server's list_elements.
func sortValueOf(value string, descending bool) string {
	trimmed := strings.TrimSpace(value)
	if !strings.HasPrefix(value, "[") || !strings.HasSuffix(trimmed, "]") {
		return value
	}
	dec := json.NewDecoder(strings.NewReader(trimmed))
	dec.UseNumber()
	var parsed []any
	if dec.Decode(&parsed) != nil {
		return value
	}
	var elements []string
	for _, e := range parsed {
		switch v := e.(type) {
		case string:
			elements = append(elements, v)
		case json.Number:
			elements = append(elements, v.String())
		case bool:
			elements = append(elements, strconv.FormatBool(v))
		}
	}
	if len(elements) == 0 {
		return ""
	}
	if descending {
		return slices.Max(elements)
	}
	return slices.Min(elements)
}

// cursorFor is the search_after cursor after hit: _id is the document id,
// _score the score, any other key the value the server sorted the hit by
// (its stored value for that field, or for a list the element sortValueOf
// picks), "" when it has none.
func cursorFor(hit Hit, keys []string, descending []bool) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		switch k {
		case "_id":
			if hit.DocId != nil {
				out[i] = *hit.DocId
			}
		case "_score":
			var s float64
			if hit.Score != nil {
				s = *hit.Score
			}
			out[i] = strconv.FormatFloat(s, 'f', -1, 64)
		default:
			if hit.Fields != nil {
				for _, f := range *hit.Fields {
					if f.Name == k {
						out[i] = sortValueOf(f.Value, i < len(descending) && descending[i])
						break
					}
				}
			}
		}
	}
	return out
}
