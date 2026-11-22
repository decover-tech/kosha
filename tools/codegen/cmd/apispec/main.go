// Command apispec turns the Swagger 2.0 documents protoc-gen-openapiv2
// writes into the checked-in OpenAPI 3.0.3 documents under api/openapi/v1/,
// and lints them (#350, ADR-350 §d).
//
//	apispec convert <in.swagger.json> <out.openapi.json>
//	apispec lint [--public] <openapi.json>...
//	apispec check-version <base.openapi.json> <head.openapi.json> <CHANGELOG.md>
//
// convert does two things only: the Swagger 2 to OpenAPI 3 conversion
// (kin-openapi openapi2conv) and a stable, indented JSON encoding. It never
// edits the contract; the proto does.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/getkin/kin-openapi/openapi2"
	"github.com/getkin/kin-openapi/openapi2conv"
	"github.com/getkin/kin-openapi/openapi3"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "convert":
		if len(os.Args) != 4 {
			usage()
		}
		err = convert(os.Args[2], os.Args[3])
	case "lint":
		args := os.Args[2:]
		public := false
		if len(args) > 0 && args[0] == "--public" {
			public, args = true, args[1:]
		}
		if len(args) == 0 {
			usage()
		}
		for _, path := range args {
			if lintErr := lintFile(path, public); lintErr != nil {
				err = errors.Join(err, lintErr)
			}
		}
	case "check-version":
		if len(os.Args) != 5 {
			usage()
		}
		err = checkVersion(os.Args[2], os.Args[3], os.Args[4])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "apispec:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: apispec convert <in.swagger.json> <out.openapi.json>")
	fmt.Fprintln(os.Stderr, "       apispec lint [--public] <openapi.json>...")
	fmt.Fprintln(os.Stderr, "       apispec check-version <base.openapi.json> <head.openapi.json> <CHANGELOG.md>")
	os.Exit(2)
}

// convert reads a Swagger 2.0 document and writes it as OpenAPI 3.0.3.
func convert(in, out string) error {
	raw, err := os.ReadFile(in)
	if err != nil {
		return err
	}
	raw, err = fixAdditionalPropertiesRefs(raw)
	if err != nil {
		return fmt.Errorf("%s: %w", in, err)
	}
	var v2 openapi2.T
	if err := json.Unmarshal(raw, &v2); err != nil {
		return fmt.Errorf("%s: %w", in, err)
	}
	v3, err := openapi2conv.ToV3(&v2)
	if err != nil {
		return fmt.Errorf("%s: convert: %w", in, err)
	}
	v3.OpenAPI = "3.0.3"
	encoded, err := encode(v3)
	if err != nil {
		return err
	}
	return os.WriteFile(out, encoded, 0o644)
}

// fixAdditionalPropertiesRefs works around openapi2conv (kin-openapi
// v0.128.0) copying `additionalProperties` verbatim: a proto map of
// messages (`map<string, FieldSchema>`) keeps its Swagger 2
// `#/definitions/...` reference, which then fails to resolve in the
// OpenAPI 3 document. Rewriting those references to `#/components/schemas/`
// before converting is part of the conversion, not an edit of the contract.
func fixAdditionalPropertiesRefs(raw []byte) ([]byte, error) {
	var generic any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&generic); err != nil {
		return nil, err
	}
	var walk func(node any)
	walk = func(node any) {
		switch value := node.(type) {
		case map[string]any:
			if extra, ok := value["additionalProperties"].(map[string]any); ok {
				if ref, ok := extra["$ref"].(string); ok {
					extra["$ref"] = strings.Replace(ref, "#/definitions/", "#/components/schemas/", 1)
				}
			}
			for _, child := range value {
				walk(child)
			}
		case []any:
			for _, child := range value {
				walk(child)
			}
		}
	}
	walk(generic)
	return json.Marshal(generic)
}

// encode renders a document as indented JSON with sorted object keys (a
// round trip through a generic value: encoding/json sorts map keys), so
// the same proto always produces the same bytes.
func encode(doc *openapi3.T) ([]byte, error) {
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	var generic any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&generic); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(generic); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func load(path string) (*openapi3.T, error) {
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return doc, nil
}

var (
	semver    = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$`)
	snakeCase = regexp.MustCompile(`^[a-z0-9_]+$`)
)

// lintFile applies the ADR-350 §d rules to one OpenAPI 3 document.
func lintFile(path string, public bool) error {
	doc, err := load(path)
	if err != nil {
		return err
	}
	var problems []string
	fail := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}
	if err := doc.Validate(openapi3.NewLoader().Context); err != nil {
		fail("invalid OpenAPI 3 document: %v", err)
	}
	if doc.Info == nil || !semver.MatchString(doc.Info.Version) {
		fail("info.version must be MAJOR.MINOR.PATCH")
	}
	for _, name := range []string{"BearerAuth", "ApiKeyAuth"} {
		if doc.Components == nil || doc.Components.SecuritySchemes[name] == nil {
			fail("security scheme %s is missing", name)
		}
	}
	if doc.Components != nil {
		for name, schema := range doc.Components.Schemas {
			if strings.Contains(name, "rpcStatus") || strings.Contains(name, "protobufAny") {
				fail("schema %s: google.rpc.Status leaked into the spec", name)
			}
			if schema.Value == nil {
				continue
			}
			for prop := range schema.Value.Properties {
				if !snakeCase.MatchString(prop) {
					fail("schema %s: property %q is not snake_case", name, prop)
				}
			}
		}
	}
	paths := doc.Paths.Map()
	keys := make([]string, 0, len(paths))
	for key := range paths {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if public && strings.HasPrefix(key, "/v1/admin") {
			fail("%s: an admin path in the public spec", key)
		}
		for method, op := range paths[key].Operations() {
			where := method + " " + key
			if op.OperationID == "" {
				fail("%s: no operationId", where)
			}
			level, _ := op.Extensions["x-stability-level"].(string)
			if level != "stable" && level != "beta" {
				fail("%s: x-stability-level must be \"stable\" or \"beta\"", where)
			}
			if op.Responses == nil {
				continue
			}
			for _, status := range []string{"429", "503"} {
				response := op.Responses.Value(status)
				if response == nil || response.Value == nil {
					continue
				}
				if response.Value.Headers["Retry-After"] == nil {
					fail("%s: response %s does not declare Retry-After", where, status)
				}
			}
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("%s:\n  %s", path, strings.Join(problems, "\n  "))
	}
	return nil
}

// checkVersion fails when the contract changed (anything but info.version)
// without a higher info.version and a matching api/CHANGELOG.md heading.
func checkVersion(basePath, headPath, changelogPath string) error {
	base, err := stripVersion(basePath)
	if err != nil {
		return err
	}
	head, err := stripVersion(headPath)
	if err != nil {
		return err
	}
	if bytes.Equal(base.body, head.body) {
		return nil
	}
	if compareSemver(head.version, base.version) <= 0 {
		return fmt.Errorf("%s changed but info.version did not increase (%s -> %s)",
			headPath, base.version, head.version)
	}
	changelog, err := os.ReadFile(changelogPath)
	if err != nil {
		return err
	}
	if !bytes.Contains(changelog, []byte("## ["+head.version+"]")) {
		return fmt.Errorf("%s has no \"## [%s]\" entry", changelogPath, head.version)
	}
	return nil
}

type versioned struct {
	version string
	body    []byte
}

func stripVersion(path string) (versioned, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return versioned{}, err
	}
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return versioned{}, fmt.Errorf("%s: %w", path, err)
	}
	info, _ := generic["info"].(map[string]any)
	version, _ := info["version"].(string)
	delete(info, "version")
	body, err := json.Marshal(generic)
	return versioned{version: version, body: body}, err
}

func compareSemver(a, b string) int {
	pa, pb := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < 3; i++ {
		var x, y int
		if i < len(pa) {
			x, _ = strconv.Atoi(pa[i])
		}
		if i < len(pb) {
			y, _ = strconv.Atoi(pb[i])
		}
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}
