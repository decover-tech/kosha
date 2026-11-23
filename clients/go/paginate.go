package kosha

import (
	"context"
	"iter"
	"slices"
	"strconv"
)

// DefaultPageSize is SearchAll's page size when neither the pageSize
// argument nor the request's MaxResults sets one.
const DefaultPageSize = 500

// SearchAll yields every hit of req, page by page with search_after.
//
// The sort always ends in an _id tie-break (appended when missing; the
// default sort is _id ascending), sort fields are added to SourceIncludes
// when it is set, and pageSize (0: req.MaxResults, else 500) becomes
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
		body, keys, err := pagedRequest(req, pageSize)
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
			next := cursorFor(hits[len(hits)-1], keys)
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

func pagedRequest(req SearchRequest, pageSize int) (SearchRequest, []string, error) {
	switch {
	case req.From != nil:
		return req, nil, configErrorf("SearchAll pages with search_after; do not set From")
	case req.SearchAfter != nil:
		return req, nil, configErrorf("SearchAll sets SearchAfter itself")
	case req.Rerank != nil:
		return req, nil, configErrorf("SearchAll cannot page a reranked search")
	case req.Knn != nil || req.Rank != nil ||
		(req.Mode != nil && (*req.Mode == KoshaServiceSearchBodyModeSemantic || *req.Mode == KoshaServiceSearchBodyModeHybrid)):
		return req, nil, configErrorf("SearchAll cannot page a kNN or hybrid search")
	case pageSize < 1:
		return req, nil, configErrorf("page size must be positive")
	}
	var sort []map[string]any
	if req.Sort != nil {
		sort = slices.Clone(*req.Sort)
	}
	keys := make([]string, 0, len(sort)+1)
	for _, e := range sort {
		k, err := sortKey(e)
		if err != nil {
			return req, nil, err
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 || keys[len(keys)-1] != "_id" {
		sort = append(sort, map[string]any{"_id": map[string]any{"order": "asc"}})
		keys = append(keys, "_id")
	}
	body := req
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
	return body, keys, nil
}

// cursorFor is the search_after cursor after hit (the kosha-cli rule): _id
// is the document id, _score the score, any other key the hit's first
// stored value for that field, "" when it has none.
func cursorFor(hit Hit, keys []string) []string {
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
						out[i] = f.Value
						break
					}
				}
			}
		}
	}
	return out
}
