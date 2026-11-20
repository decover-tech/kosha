#!/usr/bin/env python3
"""Create a Kosha API key for a tenant. Deprecated: use the CLI directly.

    kosha admin create-api-key --tenant acme-corp --description "staging key"

This script is a thin wrapper that runs exactly that command, so existing
runbooks keep working. Connection settings come from the CLI's usual
sources: KOSHA_HOST, KOSHA_API_KEY (which must be the server's
KOSHA_ADMIN_API_KEY), or a profile in ~/.kosha/config.toml.

The CLI binary is found in this order: $KOSHA_BIN, `kosha` on PATH, then
`cargo run -p kosha-cli` from this checkout.

Usage:
    python scripts/create_api_key.py acme-corp "staging key for Acme Corp"
"""

import os
import shutil
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def kosha_command():
    explicit = os.environ.get("KOSHA_BIN")
    if explicit:
        return [explicit]
    on_path = shutil.which("kosha")
    if on_path:
        return [on_path]
    cargo = shutil.which("cargo")
    if cargo:
        manifest = os.path.join(REPO_ROOT, "Cargo.toml")
        return [cargo, "run", "-q", "--manifest-path", manifest, "-p", "kosha-cli", "--"]
    sys.exit(
        "ERROR: the kosha CLI was not found. Install it with "
        "`cargo install --path crates/kosha-cli` or set KOSHA_BIN."
    )


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("Usage: python scripts/create_api_key.py <tenant_id> [description]")
        print("Prefer: kosha admin create-api-key --tenant <tenant_id> [--description ...]")
        sys.exit(0 if len(sys.argv) >= 2 else 1)

    tenant_id = sys.argv[1]
    description = sys.argv[2] if len(sys.argv) > 2 else ""
    argv = kosha_command() + [
        "admin",
        "create-api-key",
        "--tenant",
        tenant_id,
        "--description",
        description,
    ]
    os.execv(argv[0], argv)


if __name__ == "__main__":
    main()
