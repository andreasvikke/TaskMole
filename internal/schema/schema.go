// Package schema implements TaskMole's JSON Schema contract.
package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Decode parses exactly one JSON value while retaining number precision.
func Decode(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("contains more than one JSON value")
	}
	return value, nil
}

// Compile parses and compiles a self-contained Draft 2020-12 schema.
func Compile(raw []byte, location string) (*jsonschema.Schema, error) {
	document, err := Decode(raw)
	if err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	if err := compiler.AddResource(location, document); err != nil {
		return nil, err
	}
	return compiler.Compile(location)
}

// Validate compiles a schema and validates one decoded JSON value.
func Validate(raw []byte, location string, value any) error {
	compiled, err := Compile(raw, location)
	if err != nil {
		return err
	}
	return compiled.Validate(value)
}

// CheckReferences rejects schema references that leave the document.
func CheckReferences(value any, depth, maxDepth int) error {
	if depth > maxDepth {
		return fmt.Errorf("schema exceeds the maximum structural depth of %d", maxDepth)
	}
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == "$ref" || key == "$dynamicRef" {
				ref, ok := child.(string)
				if !ok {
					return fmt.Errorf("%s must be a string", key)
				}
				parsed, err := url.Parse(ref)
				if err != nil || parsed.Scheme != "" || parsed.Host != "" || !strings.HasPrefix(ref, "#") {
					return fmt.Errorf("remote %s %q is not allowed", key, ref)
				}
			}
			if err := CheckReferences(child, depth+1, maxDepth); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := CheckReferences(child, depth+1, maxDepth); err != nil {
				return err
			}
		}
	}
	return nil
}
