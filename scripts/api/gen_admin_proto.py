#!/usr/bin/env python3
"""Write proto/kosha/v1/admin.proto from the admin rows of the route table.

The operator API (`/v1/admin/*` and `GET /v1/stats`) is beta (ADR-350 §a.5):
its paths, methods, path and query parameters are exact, and its bodies are
free-form JSON objects (`google.protobuf.Struct`) until each one is typed.
This script reads `crates/kosha-server/src/routes.rs`, so the admin spec and
the route table are generated from one list; `make openapi` runs it, and the
spec-sync check fails if the checked-in proto is stale.

Usage: python3 scripts/api/gen_admin_proto.py [--check]
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ROUTES_RS = os.path.join(ROOT, "crates/kosha-server/src/routes.rs")
OUT = os.path.join(ROOT, "proto/kosha/v1/admin.proto")

# Query parameters each admin GET reads (all optional).
QUERY_PARAMS = {
    "AdminService_GetTenantUsage": [
        ("tenant", "string", "Narrow to one tenant."),
        ("from", "int64", "Window start, Unix seconds (default: 30 days before `to`)."),
        ("to", "int64", "Window end, Unix seconds (default: now)."),
    ],
    "AdminService_ListApiKeys": [("tenant_id", "string", "Narrow to one tenant.")],
    "AdminService_ListInvoices": [("tenant", "string", "Narrow to one tenant.")],
    "AdminService_ListDeletions": [
        ("state", "string", "\"pending\" or \"completed\"."),
        ("tenant", "string", "Narrow to one tenant."),
        ("limit", "uint32", "At most this many records."),
    ],
}

ROW = re.compile(r'admin\(\s*"(GET|POST|DELETE)",\s*"([^"]+)",\s*"(AdminService_\w+)",\s*\w+,?\s*\)')


def rows():
    src = open(ROUTES_RS).read()
    found = ROW.findall(src)
    if not found:
        sys.exit("no admin rows found in %s" % ROUTES_RS)
    return found


INT64 = (
    " [(grpc.gateway.protoc_gen_openapiv2.options.openapiv2_field) = {\n"
    "    type: INTEGER\n"
    '    format: "int64"\n'
    "  }]"
)


# The file-level error responses: (status, description, sends Retry-After).
ADMIN_RESPONSES = (
        ("400", "invalid_argument", False),
        ("401", "unauthenticated", False),
        ("403", "permission_denied", False),
        ("404", "not_found", False),
        ("405", "method_not_allowed", False),
        (
            "408",
            "request_timeout: the request body did not arrive in time; "
            "retry after `Retry-After` seconds",
            True,
        ),
        ("409", "already_exists, conflict", False),
        ("413", "payload_too_large", False),
        ("500", "internal", False),
        (
            "501",
            "not_implemented: this server was built or configured without the "
            "feature (an embedding provider, s3, postgres); not retryable",
            False,
        ),
        ("503", "unavailable; retry after `Retry-After` seconds", True),
)


def _retry_response(status, doc):
    return (
        "      responses: {\n"
        '        key: "%s"\n'
        "        value: {\n"
        '          description: "%s"\n'
        '          schema: {json_schema: {ref: ".kosha.v1.ErrorResponse"}}\n'
        "          headers: {\n"
        '            key: "Retry-After"\n'
        "            value: {\n"
        '              type: "integer"\n'
        '              description: "Seconds to wait before retrying."\n'
        "            }\n"
        "          }\n"
        "        }\n"
        "      }\n"
    ) % (status, doc)


# The per-operation copies of the responses that carry Retry-After.
RETRY_RESPONSES = "".join(
    _retry_response(status, doc) for status, doc, retry in ADMIN_RESPONSES if retry
)


def swagger_option():
    """The file-level openapiv2 option: the admin document's info, security
    and shared error responses. The version is kosha.proto's, so both
    documents carry one contract revision."""
    kosha = open(os.path.join(ROOT, "proto/kosha/v1/kosha.proto")).read()
    version = re.search(r'version: "([^"]+)"', kosha).group(1)
    lines = [
        "option (grpc.gateway.protoc_gen_openapiv2.options.openapiv2_swagger) = {",
        "  info: {",
        '    title: "Kosha Admin API"',
        '    version: "%s"' % version,
        "    description:",
        '      "The operator API. Every route needs the admin credential '
        '(KOSHA_ADMIN_API_KEY) as `Authorization: Bearer <key>` or `X-Api-Key: <key>`. '
        'Beta: bodies are free-form JSON objects."',
        "  }",
        "  schemes: [HTTPS, HTTP]",
        '  consumes: "application/json"',
        '  produces: "application/json"',
        "  security_definitions: {",
    ]
    for key, header, doc in (
        ("BearerAuth", "Authorization", "`Bearer <admin key>`"),
        ("ApiKeyAuth", "X-Api-Key", "The admin key itself."),
    ):
        lines += [
            "    security: {",
            '      key: "%s"' % key,
            "      value: {",
            "        type: TYPE_API_KEY",
            "        in: IN_HEADER",
            '        name: "%s"' % header,
            '        description: "%s"' % doc,
            "      }",
            "    }",
        ]
    lines.append("  }")
    for key in ("BearerAuth", "ApiKeyAuth"):
        lines += ["  security: {", '    security_requirement: {key: "%s"}' % key, "  }"]
    for status, doc, retry in ADMIN_RESPONSES:
        lines += [
            "  responses: {",
            '    key: "%s"' % status,
            "    value: {",
            '      description: "%s"' % doc,
            '      schema: {json_schema: {ref: ".kosha.v1.ErrorResponse"}}',
        ]
        if retry:
            lines += [
                "      headers: {",
                '        key: "Retry-After"',
                "        value: {",
                '          type: "integer"',
                '          description: "Seconds to wait before retrying."',
                "        }",
                "      }",
            ]
        lines += ["    }", "  }"]
    lines.append("};")
    return lines


# The codes an admin operation can return (`x-kosha-error-codes`, for typed
# SDK errors). The admin API is beta, so one superset serves every
# operation; the public spec lists codes per operation.
ADMIN_ERROR_CODES = (
    "already_exists",
    "conflict",
    "internal",
    "invalid_argument",
    "method_not_allowed",
    "not_found",
    "not_implemented",
    "payload_too_large",
    "permission_denied",
    "request_timeout",
    "unauthenticated",
    "unavailable",
)
ADMIN_ERROR_CODES_TEXT = ", ".join('{string_value: "%s"}' % c for c in ADMIN_ERROR_CODES)


def render():
    out = [
        "// Code generated by scripts/api/gen_admin_proto.py from",
        "// crates/kosha-server/src/routes.rs. DO NOT EDIT.",
        "",
        'syntax = "proto3";',
        "",
        "package kosha.v1;",
        "",
        'import "google/api/annotations.proto";',
        'import "google/protobuf/struct.proto";',
        'import "protoc-gen-openapiv2/options/annotations.proto";',
        "",
        'option go_package = "github.com/decover-tech/kosha/proto/kosha/v1;koshav1";',
        "option java_multiple_files = true;",
        'option java_package = "com.decover.kosha.v1";',
    ]
    out.extend(swagger_option())
    out += [
        "",
        "// The operator API: the admin credential (KOSHA_ADMIN_API_KEY) only.",
        "// Beta (ADR-350 §a.5): paths, methods and parameters are exact; request",
        "// and response bodies are JSON objects documented in the server source",
        "// until each is typed.",
        "service AdminService {",
    ]
    messages = []
    for method, template, op in rows():
        rpc = op.split("_", 1)[1]
        params = re.findall(r"\{(\w+)\}", template)
        fields = []
        n = 1
        for p in params:
            fields.append("  string %s = %d;" % (p, n))
            n += 1
        for name, typ, doc in QUERY_PARAMS.get(op, []):
            fields.append("  // %s" % doc)
            if typ == "int64":
                fields.append("  optional int64 %s = %d%s;" % (name, n, INT64))
            else:
                fields.append("  optional %s %s = %d;" % (typ, name, n))
            n += 1
        has_body = method in ("POST", "DELETE") and op not in (
            "AdminService_SyncBilling",
            "AdminService_RemoveOrgMember",
            "AdminService_ScrubApiKeyPlaintext",
        )
        if has_body:
            fields.append("  // The JSON request body.")
            fields.append("  google.protobuf.Struct body = %d;" % n)
        binding = '    option (google.api.http) = {\n      %s: "%s"%s\n    };' % (
            method.lower(),
            template,
            '\n      body: "body"' if has_body else "",
        )
        out.append("  rpc %s(%sRequest) returns (google.protobuf.Struct) {" % (rpc, rpc))
        out.append(binding)
        out.append(
            "    option (grpc.gateway.protoc_gen_openapiv2.options.openapiv2_operation) = {\n"
            # Per operation: the generator drops headers of file-level
            # responses (grpc-gateway v2.24.0), so every retryable status
            # is repeated here with its Retry-After.
            + RETRY_RESPONSES
            + "      extensions: {\n"
            '        key: "x-kosha-error-codes"\n'
            "        value: {list_value: {values: [" + ADMIN_ERROR_CODES_TEXT + "]}}\n"
            "      }\n"
            "      extensions: {\n"
            '        key: "x-stability-level"\n'
            '        value: {string_value: "beta"}\n'
            "      }\n"
            "    };"
        )
        out.append("  }")
        messages.append("message %sRequest {" % rpc)
        messages.extend(fields)
        messages.append("}")
        messages.append("")
    out.append("}")
    out.append("")
    out.extend(messages)
    return "\n".join(out).rstrip("\n") + "\n"


def main():
    text = render()
    if "--check" in sys.argv:
        current = open(OUT).read() if os.path.exists(OUT) else ""
        if current != text:
            sys.exit("%s is stale: run `make openapi`" % os.path.relpath(OUT, ROOT))
        return
    with open(OUT, "w") as f:
        f.write(text)


if __name__ == "__main__":
    main()
