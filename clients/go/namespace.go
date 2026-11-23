package kosha

import (
	"context"
	"net/http"
	"net/url"
)

// Namespace is a handle on one namespace. Get one from Client.Namespace.
type Namespace struct {
	c    *Client
	name string
}

// Name is the namespace's name.
func (n *Namespace) Name() string { return n.name }

func (n *Namespace) path(action string) string {
	p := "/v1/namespaces/" + url.PathEscape(n.name)
	if action != "" {
		p += "/" + action
	}
	return p
}

func (n *Namespace) do(ctx context.Context, method, action string, body, out any, opts []CallOption) error {
	if n.name == "" {
		return configErrorf("namespace name is empty")
	}
	return n.c.Do(ctx, method, n.path(action), body, out, opts...)
}

// PutSchema adds or changes field schemas.
func (n *Namespace) PutSchema(ctx context.Context, schema NamespaceSchema, opts ...CallOption) (*PutSchemaResponse, error) {
	var out PutSchemaResponse
	return result(&out, n.do(ctx, http.MethodPost, "schema", schema, &out, opts))
}

// GetSchema returns the namespace's schema.
func (n *Namespace) GetSchema(ctx context.Context, opts ...CallOption) (*GetSchemaResponse, error) {
	var out GetSchemaResponse
	return result(&out, n.do(ctx, http.MethodGet, "schema", nil, &out, opts))
}

// Upsert inserts each document, or replaces the stored one with the same id in full.
func (n *Namespace) Upsert(ctx context.Context, docs []Document, opts ...CallOption) (*IndexDocumentsResponse, error) {
	if docs == nil {
		docs = []Document{}
	}
	var out IndexDocumentsResponse
	return result(&out, n.do(ctx, http.MethodPost, "documents", KoshaServiceIndexDocumentsBody{Documents: docs}, &out, opts))
}

// Merge merges partial documents onto the stored copies by id; unknown ids are inserted.
func (n *Namespace) Merge(ctx context.Context, docs []Document, opts ...CallOption) (*ReplaceDocumentsResponse, error) {
	if docs == nil {
		docs = []Document{}
	}
	var out ReplaceDocumentsResponse
	return result(&out, n.do(ctx, http.MethodPost, "replace", KoshaServiceReplaceDocumentsBody{Documents: docs}, &out, opts))
}

// Search runs one search.
func (n *Namespace) Search(ctx context.Context, req SearchRequest, opts ...CallOption) (*SearchResponse, error) {
	var out SearchResponse
	return result(&out, n.do(ctx, http.MethodPost, "search", req, &out, opts))
}

// Exists reports which of ids are stored.
func (n *Namespace) Exists(ctx context.Context, ids []string, opts ...CallOption) (*ExistsResponse, error) {
	if ids == nil {
		ids = []string{}
	}
	var out ExistsResponse
	return result(&out, n.do(ctx, http.MethodPost, "exists", KoshaServiceExistsBody{Ids: ids}, &out, opts))
}

// DeleteByQuery deletes the documents matching req.Filter.
func (n *Namespace) DeleteByQuery(ctx context.Context, req DeleteByQueryRequest, opts ...CallOption) (*DeleteByQueryResponse, error) {
	var out DeleteByQueryResponse
	return result(&out, n.do(ctx, http.MethodPost, "delete", req, &out, opts))
}

// Flush makes buffered writes into a searchable segment.
func (n *Namespace) Flush(ctx context.Context, opts ...CallOption) (*FlushResponse, error) {
	var out FlushResponse
	return result(&out, n.do(ctx, http.MethodPost, "flush", struct{}{}, &out, opts))
}

// Stats returns document and segment counts.
func (n *Namespace) Stats(ctx context.Context, opts ...CallOption) (*StatsResponse, error) {
	var out StatsResponse
	return result(&out, n.do(ctx, http.MethodGet, "stats", nil, &out, opts))
}
