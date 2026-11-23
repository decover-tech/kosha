package kosha

// The public type surface: deliberate aliases onto the generated models in
// zz_generated_types.go (ADR-351 §a). A renamed schema in api/openapi breaks
// the build here instead of silently changing the SDK's API.

type (
	// Document is one document to index: an id and typed fields.
	Document = V1Document
	// Field is one field value; every value travels as a string.
	Field = V1Field
	// FieldType is a field value's type ("Text", "Keyword", ...).
	FieldType = V1FieldFieldType

	// NamespaceSchema maps field names to their schema.
	NamespaceSchema = V1NamespaceSchema
	// FieldSchema is one field's type, analysis and storage settings.
	FieldSchema = V1FieldSchema

	CreateNamespaceRequest  = V1CreateNamespaceRequest
	CreateNamespaceResponse = V1CreateNamespaceResponse
	ListNamespacesResponse  = V1ListNamespacesResponse
	ListedNamespace         = V1ListedNamespace
	DeleteNamespaceResponse = V1DeleteNamespaceResponse
	GetSchemaResponse       = V1GetNamespaceSchemaResponse
	PutSchemaResponse       = V1PutNamespaceSchemaResponse

	IndexDocumentsResponse   = V1IndexDocumentsResponse
	ReplaceDocumentsResponse = V1ReplaceDocumentsResponse
	ExistsResponse           = V1ExistsResponse
	DeleteByQueryRequest     = KoshaServiceDeleteByQueryBody
	DeleteByQueryResponse    = V1DeleteByQueryResponse
	FlushResponse            = V1FlushResponse
	StatsResponse            = V1GetStatsResponse

	// SearchRequest is the body of a search; QueryText is required ("" for none).
	SearchRequest  = KoshaServiceSearchBody
	SearchResponse = V1SearchResponse
	// Hit is one search result.
	Hit = V1ScoredDocument

	// ErrorResponse is the body of every non-2xx response (docs/api/errors.md).
	ErrorResponse = V1ErrorResponse
	// ErrorCode is the stable machine-readable error code. Codes are added
	// without a new API version, so treat an unknown one by its HTTP status.
	ErrorCode = V1ErrorResponseCode
)

// Field value types.
const (
	FieldTypeText     = V1FieldFieldTypeText
	FieldTypeKeyword  = V1FieldFieldTypeKeyword
	FieldTypeInteger  = V1FieldFieldTypeInteger
	FieldTypeFloat    = V1FieldFieldTypeFloat
	FieldTypeDate     = V1FieldFieldTypeDate
	FieldTypeBoolean  = V1FieldFieldTypeBoolean
	FieldTypeVector   = V1FieldFieldTypeVector
	FieldTypeGeoPoint = V1FieldFieldTypeGeoPoint
)

// Ptr returns a pointer to v, for the optional fields of request bodies:
//
//	kosha.SearchRequest{QueryText: "q", MaxResults: kosha.Ptr[int64](20)}
func Ptr[T any](v T) *T { return &v }
