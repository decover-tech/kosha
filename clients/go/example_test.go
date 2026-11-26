package kosha_test

import (
	"context"
	"errors"
	"fmt"

	kosha "github.com/decover-tech/kosha/clients/go"
)

// Compiled, not run: it needs a server. Mirrors the README.
func Example() {
	ctx := context.Background()
	c, err := kosha.NewClient(kosha.WithBaseURL("https://kosha.example.com"), kosha.WithAPIKey("sk-..."))
	if err != nil {
		panic(err)
	}
	_, err = c.CreateNamespace(ctx, "docs", &kosha.NamespaceSchema{
		Fields: &map[string]kosha.FieldSchema{
			"title": {Type: kosha.Ptr("text"), Stored: kosha.Ptr(true), Indexed: kosha.Ptr(true)},
			"tag":   {Type: kosha.Ptr("keyword"), Stored: kosha.Ptr(true), Indexed: kosha.Ptr(true)},
		},
	})
	if errors.Is(err, kosha.ErrConflict) {
		fmt.Println("already there")
	}
	docs := c.Namespace("docs")
	_, _ = docs.Upsert(ctx, []kosha.Document{{Id: "1", Fields: []kosha.Field{
		{Name: "title", FieldType: kosha.FieldTypeText, Value: "hello world"},
		{Name: "tag", FieldType: kosha.FieldTypeKeyword, Value: "a"},
	}}})
	page, err := docs.Search(ctx, kosha.SearchRequest{QueryText: "hello", MaxResults: kosha.Ptr[int64](10)})
	if err == nil && page.TotalHits != nil {
		fmt.Println(*page.TotalHits)
	}
	req := kosha.SearchRequest{Sort: &[]map[string]any{{"tag": map[string]any{"order": "asc"}}}}
	for hit, err := range docs.SearchAll(ctx, req, 500) {
		if err != nil {
			break
		}
		fmt.Println(*hit.DocId)
	}
	res, err := docs.BulkIndex(ctx, nil, kosha.BulkOptions{MaxDocsPerBatch: 500, Concurrency: 2})
	var be *kosha.BulkError
	if errors.As(err, &be) {
		fmt.Println(be.Result.Failures[0].IDs)
	} else if res != nil {
		fmt.Println(res.Indexed)
	}
}
