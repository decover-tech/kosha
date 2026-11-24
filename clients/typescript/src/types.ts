// The public type surface: deliberate aliases onto the generated schema
// (ADR-351 §a). A renamed schema in api/openapi breaks this file's build
// instead of silently changing the SDK's API.
import type { components } from "./generated/schema.js";

type Schemas = components["schemas"];

export type Document = Schemas["v1Document"];
export type Field = Schemas["v1Field"];
export type FieldType = Field["field_type"];

export type NamespaceSchema = Schemas["v1NamespaceSchema"];
export type FieldSchema = Schemas["v1FieldSchema"];

export type CreateNamespaceResponse = Schemas["v1CreateNamespaceResponse"];
export type ListNamespacesResponse = Schemas["v1ListNamespacesResponse"];
export type ListedNamespace = Schemas["v1ListedNamespace"];
export type DeleteNamespaceResponse = Schemas["v1DeleteNamespaceResponse"];
export type GetSchemaResponse = Schemas["v1GetNamespaceSchemaResponse"];
export type PutSchemaResponse = Schemas["v1PutNamespaceSchemaResponse"];

export type IndexDocumentsResponse = Schemas["v1IndexDocumentsResponse"];
export type ReplaceDocumentsResponse = Schemas["v1ReplaceDocumentsResponse"];
export type ExistsResponse = Schemas["v1ExistsResponse"];
export type DeleteByQueryRequest = Schemas["KoshaServiceDeleteByQueryBody"];
export type DeleteByQueryResponse = Schemas["v1DeleteByQueryResponse"];
export type FlushResponse = Schemas["v1FlushResponse"];
export type StatsResponse = Schemas["v1GetStatsResponse"];

export type SearchRequest = Schemas["KoshaServiceSearchBody"];
export type SearchResponse = Schemas["v1SearchResponse"];
/** One search result. */
export type Hit = Schemas["v1ScoredDocument"];

export type ErrorResponse = Schemas["v1ErrorResponse"];
/** The documented error codes (docs/api/errors.md). */
export type ErrorCode = NonNullable<ErrorResponse["code"]>;
