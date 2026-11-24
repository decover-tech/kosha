#!/usr/bin/env python3
"""Fail when a public API operation has no conformance case (stdlib only).

Every operationId in api/openapi/v1/kosha.openapi.json must appear in some
case's ``covers`` in clients/conformance/cases/*.json, or be on EXCLUDED
below with a reason. Also checks each case against the closed sets in
case.schema.json (case keys, ops, matchers) and that ``covers`` names only
real operations, so a typo cannot satisfy the check.

    python3 clients/conformance/check_coverage.py
"""

from __future__ import annotations

import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = os.path.join(HERE, "..", "..", "api", "openapi", "v1", "kosha.openapi.json")

# Public operations the SDK suite does not exercise, and why.
EXCLUDED = {
    "KoshaService_HealthCheck": "probe; not an SDK operation",
    "KoshaService_GetBilling": "billing (#349) is deployment-specific and off in the test stack",
    "KoshaService_GetUsage": "metering is deployment-specific; reachable through request()",
    "KoshaService_BranchNamespace": "no typed SDK method in 0.x; reachable through request()",
    "KoshaService_DeleteBranch": "no typed SDK method in 0.x; reachable through request()",
    "KoshaService_ListBranches": "no typed SDK method in 0.x; reachable through request()",
    "KoshaService_PutShards": "no typed SDK method in 0.x; reachable through request()",
    "KoshaService_ExportDocuments": "no typed SDK method in 0.x; reachable through request()",
}


def load_cases() -> list[tuple[str, dict]]:
    out = []
    for path in sorted(glob.glob(os.path.join(HERE, "cases", "*.json"))):
        with open(path, encoding="utf-8") as f:
            for case in json.load(f):
                out.append((os.path.basename(path), case))
    return out


def main() -> int:
    with open(SPEC, encoding="utf-8") as f:
        spec = json.load(f)
    with open(os.path.join(HERE, "case.schema.json"), encoding="utf-8") as f:
        schema = json.load(f)["$defs"]
    ops = set(schema["op"]["enum"])
    matchers = set(schema["expect"]["properties"])
    case_keys = set(schema["case"]["properties"])
    step_keys = set(schema["step"]["properties"])

    operations = {
        op["operationId"]
        for item in spec["paths"].values()
        for op in item.values()
        if isinstance(op, dict) and "operationId" in op
    }
    problems: list[str] = []
    covered: set[str] = set()
    seen_ids: set[str] = set()
    for file, case in load_cases():
        where = f"{file}:{case.get('id', '?')}"
        if case.get("id") in seen_ids:
            problems.append(f"{where}: duplicate case id")
        seen_ids.add(case.get("id"))
        for key in set(case) - case_keys:
            problems.append(f"{where}: unknown case key {key!r}")
        for name in case.get("covers", []):
            if name not in operations:
                problems.append(f"{where}: covers unknown operation {name!r}")
            covered.add(name)
        for i, step in enumerate(case.get("setup", []) + case.get("steps", [])):
            if step.get("op") not in ops:
                problems.append(f"{where} step {i}: unknown op {step.get('op')!r}")
            for key in set(step) - step_keys:
                problems.append(f"{where} step {i}: unknown step key {key!r}")
            for key in set(step.get("expect", {})) - matchers:
                problems.append(f"{where} step {i}: unknown matcher {key!r}")

    for name in sorted(set(EXCLUDED) - operations):
        problems.append(f"EXCLUDED names {name!r}, which is not in the spec any more")
    missing = sorted(operations - covered - set(EXCLUDED))
    for name in missing:
        problems.append(f"{name} has no conformance case (add one, or exclude it with a reason)")

    if problems:
        print("\n".join(problems), file=sys.stderr)
        return 1
    print(f"conformance coverage: {len(operations - set(EXCLUDED))} operations covered, {len(EXCLUDED)} excluded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
