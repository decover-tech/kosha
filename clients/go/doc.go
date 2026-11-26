// Package kosha is the Go SDK for Kosha's /v1 HTTP API.
//
//	c, err := kosha.NewClient(kosha.WithBaseURL("https://kosha.example.com"), kosha.WithAPIKey(key))
//	ns := c.Namespace("docs")
//	page, err := ns.Search(ctx, kosha.SearchRequest{QueryText: "hello", MaxResults: kosha.Ptr[int64](10)})
//
// Request and response types are generated from api/openapi/v1/kosha.openapi.json
// (zz_generated_types.go); the transport, retries, SearchAll and BulkIndex
// are hand-written. The module has no dependencies outside the standard
// library.
//
// Every call retries under the policy in clients/conformance/RETRY.md:
// a response with "retryable": true (or a bare 429/503) is retried after
// the server's delay or jittered backoff, a bounded number of times.
package kosha
