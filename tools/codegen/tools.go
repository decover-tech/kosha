//go:build tools

// Package tools pins the API toolchain (#350). `go run` resolves each tool
// from this module's go.sum, so every contributor and CI job runs the same
// versions with no network plugin registry involved.
package tools

import (
	_ "github.com/bufbuild/buf/cmd/buf"
	_ "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2"
)
