package kafka

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/hamba/avro/v2"
)

// IMPORTANT:
// This file handles JSON Avro encoding ONLY.
// Avro JSON MUST be driven strictly by schema, NEVER by Go types.
// DO NOT use binary Avro conversion functions (convertFloat64ToInt*, unwrapUnion*, etc.) for JSON Avro.
// Avro Binary and Avro JSON are two distinct encodings with different rules.
// NEVER share conversion logic between them.

// SerializeJSONAvro serializes data into JSON Avro format.
// Converts plain JSON to Avro JSON by wrapping unions strictly according to schema.
// This ensures 100% compatibility with Java Avro consumers (Apicurio/SmallRye).
func (*AvroSerde) SerializeJSONAvro(data any, schema *Schema) ([]byte, *Xk6KafkaError) {
	jsonBytes, err := toJSONBytes(data)
	if err != nil {
		return nil, err
	}

	var input any
	if err := json.Unmarshal(jsonBytes, &input); err != nil {
		return nil, NewXk6KafkaError(failedToEncode, "Invalid JSON input", err)
	}

	avroSchema := schema.Codec()
	if avroSchema == nil {
		return nil, NewXk6KafkaError(failedToEncode, "Invalid Avro schema", nil)
	}

	out, jsonErr := toAvroJSON(input, avroSchema)
	if jsonErr != nil {
		return nil, NewXk6KafkaError(failedToEncode, jsonErr.Error(), jsonErr)
	}

	bytes, marshalErr := json.Marshal(out)
	if marshalErr != nil {
		return nil, NewXk6KafkaError(failedToEncode, "JSON marshal failed", marshalErr)
	}

	return bytes, nil
}

// toAvroJSON recursively converts plain JSON to Avro JSON format.
// Wraps unions strictly according to schema: {"<type>": value}
func toAvroJSON(value any, sch avro.Schema) (any, error) {

	// Resolve references
	if ref, ok := sch.(*avro.RefSchema); ok {
		return toAvroJSON(value, ref.Schema())
	}

	switch s := sch.(type) {

	case *avro.RecordSchema:
		obj, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected record %s", s.FullName())
		}

		out := make(map[string]any)
		for _, f := range s.Fields() {
			v, exists := obj[f.Name()]
			if !exists {
				if f.HasDefault() {
					// Default values must be converted to Avro JSON format
					dv, err := toAvroJSON(f.Default(), f.Type())
					if err != nil {
						return nil, fmt.Errorf("default field %s: %w", f.Name(), err)
					}
					out[f.Name()] = dv
					continue
				}
				return nil, fmt.Errorf("missing field %s", f.Name())
			}

			cv, err := toAvroJSON(v, f.Type())
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", f.Name(), err)
			}
			out[f.Name()] = cv
		}
		return out, nil

	case *avro.UnionSchema:
		if value == nil {
			// In Avro JSON, null is NOT wrapped - just return nil
			for _, branch := range s.Types() {
				if branch.Type() == avro.Null {
					return nil, nil
				}
			}
			return nil, fmt.Errorf("null not allowed in union")
		}

		// Check if value is ALREADY a correctly wrapped union: {"branchName": innerValue}
		// This is STRICT validation - must match schema exactly
		if m, ok := value.(map[string]any); ok && len(m) == 1 {
			for key, val := range m {
				// Check if key matches any branch name in the schema
				for _, branch := range s.Types() {
					if branch.Type() == avro.Null {
						continue
					}
					branchName := unionBranchName(branch)
					if key == branchName {
						// Found matching branch - validate and preserve structure
						cv, err := toAvroJSON(val, branch)
						if err == nil {
							// Already correctly wrapped - return as-is with validated content
							return map[string]any{branchName: cv}, nil
						}
						// Wrapper matches but content invalid - try other branches
					}
				}
			}
		}

		// Not wrapped - try to wrap with first matching branch
		// This matches Java Avro behavior: first successful conversion wins
		// IMPORTANT: Union branches MUST be ordered from most specific to most generic
		// in the schema to avoid ambiguous matches (e.g., ["int", "string"] not ["string", "int"])
		// Otherwise "123" as string will match string branch even if int was intended.
		for _, branch := range s.Types() {
			if branch.Type() == avro.Null {
				continue
			}

			cv, err := toAvroJSON(value, branch)
			if err == nil {
				return map[string]any{
					unionBranchName(branch): cv,
				}, nil
			}
		}

		return nil, fmt.Errorf("value does not match any union branch")

	case *avro.ArraySchema:
		arr, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("expected array")
		}

		out := make([]any, len(arr))
		for i, e := range arr {
			cv, err := toAvroJSON(e, s.Items())
			if err != nil {
				return nil, fmt.Errorf("array[%d]: %w", i, err)
			}
			out[i] = cv
		}
		return out, nil

	case *avro.MapSchema:
		obj, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected map")
		}

		out := make(map[string]any)
		for k, v := range obj {
			cv, err := toAvroJSON(v, s.Values())
			if err != nil {
				return nil, fmt.Errorf("map[%s]: %w", k, err)
			}
			out[k] = cv
		}
		return out, nil

	case *avro.EnumSchema:
		str, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("enum expects string")
		}
		for _, sym := range s.Symbols() {
			if sym == str {
				return str, nil
			}
		}
		return nil, fmt.Errorf("invalid enum value %q", str)

	case *avro.PrimitiveSchema:
		// CRITICAL: Validate type strictly to prevent silent data corruption
		// Java Avro will reject type mismatches - we must do the same
		switch s.Type() {
		case avro.Null:
			if value != nil {
				return nil, fmt.Errorf("expected null, got %T", value)
			}
			return nil, nil
		case avro.Boolean:
			if _, ok := value.(bool); !ok {
				return nil, fmt.Errorf("expected boolean, got %T", value)
			}
			return value, nil
		case avro.Int, avro.Long:
			// JSON numbers are unmarshaled as float64
			if _, ok := value.(float64); !ok {
				return nil, fmt.Errorf("expected number, got %T", value)
			}
			return value, nil
		case avro.Float, avro.Double:
			if _, ok := value.(float64); !ok {
				return nil, fmt.Errorf("expected number, got %T", value)
			}
			return value, nil
		case avro.String:
			if _, ok := value.(string); !ok {
				return nil, fmt.Errorf("expected string, got %T", value)
			}
			return value, nil
		case avro.Bytes:
			str, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("expected base64 string for bytes, got %T", value)
			}
			// Validate base64 encoding (Java will decode it)
			if _, err := base64.StdEncoding.DecodeString(str); err != nil {
				return nil, fmt.Errorf("invalid base64 for bytes: %w", err)
			}
			return str, nil
		default:
			return nil, fmt.Errorf("unsupported primitive type %v", s.Type())
		}

	default:
		return nil, fmt.Errorf("unsupported schema %T", sch)
	}
}

// unionBranchName returns the name to use for union branch wrapping.
// This is the ONLY correct way to wrap unions in Avro JSON for Java compatibility.
//
// CRITICAL: This must match EXACTLY what Java Avro expects:
// - Named types (records, enums, fixed): use FullName (namespace.name)
// - Primitives: use type name (string, int, long, etc.)
// - Complex anonymous types (array, map): use type keyword
//
// Java reference: org.apache.avro.io.JsonEncoder.writeIndex()
// Test with: new JsonDecoder(schema, jsonString) to verify compatibility
func unionBranchName(sch avro.Schema) string {
	if named, ok := sch.(avro.NamedSchema); ok {
		return named.FullName()
	}
	switch sch.Type() {
	case avro.Array:
		return "array"
	case avro.Map:
		return "map"
	case avro.String:
		return "string"
	case avro.Int:
		return "int"
	case avro.Long:
		return "long"
	case avro.Float:
		return "float"
	case avro.Double:
		return "double"
	case avro.Boolean:
		return "boolean"
	case avro.Bytes:
		return "bytes"
	case avro.Null:
		return "null"
	default:
		return "unknown"
	}
}
