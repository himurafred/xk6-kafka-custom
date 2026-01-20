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
			// In Avro JSON, even null must be wrapped
			for _, branch := range s.Types() {
				if branch.Type() == avro.Null {
					return map[string]any{"null": nil}, nil
				}
			}
			return nil, fmt.Errorf("null not allowed in union")
		}

		// Avro JSON: union MUST be wrapped
		// Java behavior: take first matching branch (no ambiguity error)
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
		if s.Type() == avro.Null {
			return nil, nil
		}
		if s.Type() == avro.Bytes {
			str, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("bytes must be base64 string")
			}
			// Validate base64 encoding (Java will decode it)
			if _, err := base64.StdEncoding.DecodeString(str); err != nil {
				return nil, fmt.Errorf("invalid base64 for bytes: %w", err)
			}
			return str, nil
		}
		return value, nil

	default:
		return nil, fmt.Errorf("unsupported schema %T", sch)
	}
}

// unionBranchName returns the name to use for union branch wrapping.
func unionBranchName(sch avro.Schema) string {
	if named, ok := sch.(avro.NamedSchema); ok {
		return named.FullName()
	}
	switch sch.Type() {
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
