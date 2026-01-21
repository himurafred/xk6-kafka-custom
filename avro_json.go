package kafka

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/hamba/avro/v2"
	"github.com/sirupsen/logrus"
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

		// Check if value is already wrapped (e.g., {"TypeName": {...}} or {"array": [...]})
		if m, ok := value.(map[string]any); ok && len(m) == 1 {
			for key, val := range m {
				// Debug: log what we're checking
				logger := logrus.WithFields(logrus.Fields{
					"unionKey":      key,
					"numBranches":   len(s.Types()),
				})
				
				// Try to find a matching branch by name OR by type
				for _, branch := range s.Types() {
					branchName := unionBranchName(branch)
					branchType := unionBranchTypeKey(branch)
					logger.WithFields(logrus.Fields{
						"branchName": branchName,
						"branchType": branchType,
					}).Debug("🔍 Checking union branch")
					
					// Check if wrapped by type name (e.g., "orbis.u.medicalrecord....")
					if key == branchName {
						logger.Info("✅ Found pre-wrapped union value (by name)")
						// Already wrapped - validate the content
						cv, err := toAvroJSON(val, branch)
						if err == nil {
							return map[string]any{branchName: cv}, nil
						}
						logger.WithError(err).Warn("⚠️ Pre-wrapped union validation failed, trying to accept as-is")
						
						// If validation failed but the structure looks like it's already Avro JSON, accept it
						if isAlreadyAvroJSON(val, branch) {
							logger.Info("✅ Value appears to be already in Avro JSON format, accepting as-is")
							return map[string]any{branchName: val}, nil
						}
					}
					
					// Check if wrapped by type key (e.g., "array", "map")
					// This happens when union branches are wrapped with type keywords instead of type names
					if key == branchType && branchType != "unknown" {
						logger.Info("✅ Found pre-wrapped union value (by type)")
						// Accept as-is - already in correct Avro JSON format
						return m, nil
					}
				}
			}
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

// unionBranchTypeKey returns the type-based key for union wrapping.
// This is used when unions are wrapped with type keywords like "array", "map"
// instead of type names like "MyRecordType".
func unionBranchTypeKey(sch avro.Schema) string {
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

// isAlreadyAvroJSON checks if a value appears to already be in Avro JSON format.
// This is specifically for union branches that might be pre-formatted.
// For example, array unions in Avro JSON should be {"array": [...]}, not just [...]
func isAlreadyAvroJSON(value any, sch avro.Schema) bool {
	// Resolve references
	if ref, ok := sch.(*avro.RefSchema); ok {
		return isAlreadyAvroJSON(value, ref.Schema())
	}

	switch s := sch.(type) {
	case *avro.ArraySchema:
		// In Avro JSON, arrays can be represented as {"array": [...]}
		// This is the standard format when arrays are inside a union
		if m, ok := value.(map[string]any); ok {
			if arr, hasArray := m["array"]; hasArray {
				// Check if the array itself looks valid
				if arrSlice, isSlice := arr.([]any); isSlice {
					// Validate all elements
					for _, elem := range arrSlice {
						if !isAlreadyAvroJSON(elem, s.Items()) {
							return false
						}
					}
					return true
				}
			}
		}
		// Also accept plain arrays
		if arr, ok := value.([]any); ok {
			for _, elem := range arr {
				if !isAlreadyAvroJSON(elem, s.Items()) {
					return false
				}
			}
			return true
		}
		return false

	case *avro.MapSchema:
		// Maps in Avro JSON can be {"map": {...}}
		if m, ok := value.(map[string]any); ok {
			if mapVal, hasMap := m["map"]; hasMap {
				if mapObj, isMap := mapVal.(map[string]any); isMap {
					for _, v := range mapObj {
						if !isAlreadyAvroJSON(v, s.Values()) {
							return false
						}
					}
					return true
				}
			}
			// Also accept plain maps (validate all values)
			for _, v := range m {
				if !isAlreadyAvroJSON(v, s.Values()) {
					return false
				}
			}
			return true
		}
		return false

	case *avro.UnionSchema:
		// Union must be wrapped: {"type": value}
		if m, ok := value.(map[string]any); ok && len(m) == 1 {
			for key, val := range m {
				for _, branch := range s.Types() {
					if key == unionBranchName(branch) {
						return isAlreadyAvroJSON(val, branch)
					}
				}
			}
		}
		return false

	case *avro.RecordSchema:
		// Records should be objects with expected fields
		_, ok := value.(map[string]any)
		if !ok {
			return false
		}
		// Just check that it's a map - detailed validation would be too expensive
		return true

	case *avro.PrimitiveSchema:
		// Primitives are straightforward
		switch s.Type() {
		case avro.Null:
			return value == nil
		case avro.Boolean:
			_, ok := value.(bool)
			return ok
		case avro.Int, avro.Long:
			// JSON numbers are float64
			_, ok := value.(float64)
			return ok
		case avro.Float, avro.Double:
			_, ok := value.(float64)
			return ok
		case avro.String:
			_, ok := value.(string)
			return ok
		case avro.Bytes:
			// Bytes should be base64-encoded strings
			str, ok := value.(string)
			if !ok {
				return false
			}
			_, err := base64.StdEncoding.DecodeString(str)
			return err == nil
		}
		return false

	case *avro.EnumSchema:
		str, ok := value.(string)
		if !ok {
			return false
		}
		// Check if it's a valid enum symbol
		for _, sym := range s.Symbols() {
			if sym == str {
				return true
			}
		}
		return false

	default:
		return false
	}
}
