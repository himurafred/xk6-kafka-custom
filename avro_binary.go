package kafka

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"
)

// IMPORTANT:
// This file handles BINARY Avro encoding/decoding ONLY.
// Binary Avro requires strict typing (float64→int32/int64 conversion, union wrapping for named types).
// DO NOT use this code for JSON Avro serialization - see avro_json.go instead.
// Avro Binary and Avro JSON are two distinct encodings with different rules.
// NEVER share conversion logic between them.

var (
	// ErrCannotConvertToByte is returned when a value cannot be converted to byte.
	ErrCannotConvertToByte = errors.New("cannot convert value to byte")
	// ErrCannotConvertToInt32 is returned when a float64 cannot be converted to int32.
	ErrCannotConvertToInt32 = errors.New("cannot convert float64 to int32: not an integer")
	// ErrCannotConvertToInt64 is returned when a float64 cannot be converted to int64.
	ErrCannotConvertToInt64 = errors.New("cannot convert float64 to int64: not an integer")
)

type AvroSerde struct {
	Serdes
}

// convertPrimitiveType converts a primitive value to the correct Avro type.
// Handles float64->int32/int64 conversion and array->bytes conversion.
func convertPrimitiveType(data any, schema avro.Schema) (any, error) {
	switch schema.Type() {
	case avro.Bytes:
		// Convert array of numbers to []byte for bytes fields
		if arr, ok := data.([]any); ok {
			bytes := make([]byte, len(arr))
			for i, v := range arr {
				switch val := v.(type) {
				case float64:
					bytes[i] = byte(val)
				case int:
					bytes[i] = byte(val)
				case int32:
					bytes[i] = byte(val)
				case int64:
					bytes[i] = byte(val)
				default:
					return nil, fmt.Errorf("%w at index %d: %T", ErrCannotConvertToByte, i, v)
				}
			}
			return bytes, nil
		}
		if bytes, ok := data.([]byte); ok {
			return bytes, nil
		}
		return data, nil
	case avro.Int:
		if f, ok := data.(float64); ok {
			if f != float64(int32(f)) {
				return nil, fmt.Errorf("%w: %f", ErrCannotConvertToInt32, f)
			}
			return int32(f), nil
		}
		return data, nil
	case avro.Long:
		if f, ok := data.(float64); ok {
			if f != float64(int64(f)) {
				return nil, fmt.Errorf("%w: %f", ErrCannotConvertToInt64, f)
			}
			return int64(f), nil
		}
		return data, nil
	case avro.Record, avro.Error, avro.Ref, avro.Enum, avro.Array, avro.Map,
		avro.Union, avro.Fixed, avro.String, avro.Float, avro.Double,
		avro.Boolean, avro.Null:
		fallthrough
	default:
		return data, nil
	}
}

// convertUnionField converts a union field value, wrapping named schemas appropriately.
// Binary Avro requires strict union branch selection - the writer MUST provide the correct branch.
func convertUnionField(fieldValue any, unionSchema *avro.UnionSchema) (any, error) {
	if fieldValue == nil {
		//nolint: nilnil // nil is a valid union value
		return nil, nil
	}

	types := unionSchema.Types()

	// Check if already wrapped: {"typeName": value}
	if fieldValueMap, ok := fieldValue.(map[string]any); ok {
		if len(fieldValueMap) == 1 {
			for key, wrappedValue := range fieldValueMap {
				// Try to find matching named schema
				for _, unionType := range types {
					if unionType.Type() == avro.Null {
						continue
					}
					actualType := unionType
					if refSchema, ok := unionType.(*avro.RefSchema); ok {
						actualType = refSchema.Schema()
					}
					if namedSchema, ok := actualType.(avro.NamedSchema); ok && namedSchema.FullName() == key {
						// Already wrapped, convert nested value
						converted, err := convertFloat64ToIntForIntegerFields(wrappedValue, actualType)
						if err != nil {
							return nil, err
						}
						return map[string]any{key: converted}, nil
					}
				}
			}
		}
	}

	// Not wrapped - try each branch strictly
	var matches []any
	for _, unionType := range types {
		if unionType.Type() == avro.Null {
			continue
		}
		actualType := unionType
		if refSchema, ok := unionType.(*avro.RefSchema); ok {
			actualType = refSchema.Schema()
		}

		converted, err := convertFloat64ToIntForIntegerFields(fieldValue, actualType)
		if err == nil {
			// Named schemas need wrapping
			if namedSchema, ok := actualType.(avro.NamedSchema); ok {
				matches = append(matches, map[string]any{namedSchema.FullName(): converted})
			} else {
				matches = append(matches, converted)
			}
		}
	}

	if len(matches) == 0 {
		return nil, fmt.Errorf("value does not match any union branch")
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("value matches multiple union branches (ambiguous)")
	}

	return matches[0], nil
}

// convertFloat64ToIntForIntegerFields converts float64 values to int32/int64 for int/long schema fields.
// This is necessary because JSON unmarshaling converts all numbers to float64,
// but Avro int fields require int32 values and long fields require int64 values.
func convertFloat64ToIntForIntegerFields(data any, schema avro.Schema) (any, error) {
	if schema == nil {
		return data, nil
	}

	// Handle schema references
	if refSchema, ok := schema.(*avro.RefSchema); ok {
		schema = refSchema.Schema()
	}

	switch schema.Type() {
	case avro.Bytes, avro.Int, avro.Long:
		return convertPrimitiveType(data, schema)
	case avro.Record:
		return convertRecordFields(data, schema, func(fieldValue any, fieldType avro.Schema) (any, error) {
			if unionSchema, ok := fieldType.(*avro.UnionSchema); ok {
				return convertUnionField(fieldValue, unionSchema)
			}
			return convertFloat64ToIntForIntegerFields(fieldValue, fieldType)
		})
	case avro.Array:
		arraySchema, ok := schema.(*avro.ArraySchema)
		if !ok {
			return data, nil
		}

		dataArray, ok := data.([]any)
		if !ok {
			return data, nil
		}

		convertedArray := make([]any, len(dataArray))
		for i, item := range dataArray {
			convertedItem, err := convertFloat64ToIntForIntegerFields(item, arraySchema.Items())
			if err != nil {
				return nil, fmt.Errorf("array index %d: %w", i, err)
			}
			convertedArray[i] = convertedItem
		}

		return convertedArray, nil
	case avro.Map:
		mapSchema, ok := schema.(*avro.MapSchema)
		if !ok {
			return data, nil
		}

		dataMap, ok := data.(map[string]any)
		if !ok {
			return data, nil
		}

		convertedMap := make(map[string]any)
		for k, v := range dataMap {
			convertedValue, err := convertFloat64ToIntForIntegerFields(v, mapSchema.Values())
			if err != nil {
				return nil, fmt.Errorf("map key %s: %w", k, err)
			}
			convertedMap[k] = convertedValue
		}

		return convertedMap, nil
	case avro.Union:
		fallthrough
	case avro.Error, avro.Ref, avro.Enum, avro.Fixed, avro.String,
		avro.Float, avro.Double, avro.Boolean, avro.Null:
		fallthrough
	default:
		return data, nil
	}
}

// convertRecordFields processes record fields using the provided field converter function.
func convertRecordFields(data any, schema avro.Schema, convertField func(any, avro.Schema) (any, error)) (any, error) {
	recordSchema, ok := schema.(*avro.RecordSchema)
	if !ok {
		return data, nil
	}

	dataMap, ok := data.(map[string]any)
	if !ok {
		return data, nil
	}

	resultMap := make(map[string]any)
	for _, field := range recordSchema.Fields() {
		fieldName := field.Name()
		fieldValue, exists := dataMap[fieldName]
		if !exists {
			continue
		}

		fieldType := field.Type()
		convertedValue, err := convertField(fieldValue, fieldType)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", fieldName, err)
		}
		resultMap[fieldName] = convertedValue
	}

	// Binary Avro: schema is authoritative, ignore fields not in schema
	return resultMap, nil
}

// Serialize serializes a JSON object into Avro binary.
func (*AvroSerde) Serialize(data any, schema *Schema) ([]byte, *Xk6KafkaError) {
	jsonBytes, err := toJSONBytes(data)
	if err != nil {
		return nil, err
	}

	avroSchema := schema.Codec()
	if avroSchema == nil {
		return nil, NewXk6KafkaError(failedToEncode, "Failed to parse Avro schema", nil)
	}

	// Parse JSON data into a map for marshaling
	var jsonData any
	jsonErr := json.Unmarshal(jsonBytes, &jsonData)
	if jsonErr != nil {
		return nil, NewXk6KafkaError(failedToEncode, "Failed to parse JSON data", jsonErr)
	}

	// Convert float64 to int32/int64 for int/long fields before marshaling
	convertedData, convertErr := convertFloat64ToIntForIntegerFields(jsonData, avroSchema)
	if convertErr != nil {
		return nil, NewXk6KafkaError(failedToEncode,
			fmt.Sprintf("Failed to convert float64 to int32/int64 for integer fields: %v", convertErr),
			convertErr)
	}

	// Marshal to binary using hamba/avro
	bytesData, originalErr := avro.Marshal(avroSchema, convertedData)
	if originalErr != nil {
		return nil, NewXk6KafkaError(failedToEncodeToBinary,
			"Failed to encode data into binary",
			originalErr)
	}

	return bytesData, nil
}

// unwrapUnionValues recursively unwraps union values that are wrapped in the
// {"typeName": value} format returned by hamba/avro for named types in unions.
func unwrapUnionValues(data any, schema avro.Schema) (any, error) {
	if data == nil {
		//nolint: nilnil // nil is a valid value
		return nil, nil
	}

	switch schema.Type() {
	case avro.Record:
		return convertRecordFields(data, schema, func(fieldValue any, fieldType avro.Schema) (any, error) {
			if unionSchema, ok := fieldType.(*avro.UnionSchema); ok {
				return unwrapUnionValue(fieldValue, unionSchema)
			}
			return unwrapUnionValues(fieldValue, fieldType)
		})
	case avro.Array:
		arraySchema, ok := schema.(*avro.ArraySchema)
		if !ok {
			return data, nil
		}

		dataArray, ok := data.([]any)
		if !ok {
			return data, nil
		}

		unwrappedArray := make([]any, len(dataArray))
		for i, item := range dataArray {
			unwrappedItem, err := unwrapUnionValues(item, arraySchema.Items())
			if err != nil {
				return nil, fmt.Errorf("array index %d: %w", i, err)
			}
			unwrappedArray[i] = unwrappedItem
		}

		return unwrappedArray, nil
	case avro.Map:
		mapSchema, ok := schema.(*avro.MapSchema)
		if !ok {
			return data, nil
		}

		dataMap, ok := data.(map[string]any)
		if !ok {
			return data, nil
		}

		unwrappedMap := make(map[string]any)
		for k, v := range dataMap {
			unwrappedValue, err := unwrapUnionValues(v, mapSchema.Values())
			if err != nil {
				return nil, fmt.Errorf("map key %s: %w", k, err)
			}
			unwrappedMap[k] = unwrappedValue
		}

		return unwrappedMap, nil
	case avro.Error, avro.Ref, avro.Enum, avro.Union, avro.Fixed,
		avro.String, avro.Bytes, avro.Int, avro.Long, avro.Float,
		avro.Double, avro.Boolean, avro.Null:
		fallthrough
	default:
		return data, nil
	}
}

// unwrapUnionValue unwraps a single union value if it's wrapped in {"typeName": value} format.
func unwrapUnionValue(value any, unionSchema *avro.UnionSchema) (any, error) {
	if value == nil {
		//nolint: nilnil // nil is a valid union value
		return nil, nil
	}

	// Check if value is wrapped as {"typeName": value}
	if valueMap, ok := value.(map[string]any); ok && len(valueMap) == 1 {
		for key, wrappedValue := range valueMap {
			// Check if key matches any union type's full name
			for _, unionType := range unionSchema.Types() {
				if unionType.Type() == avro.Null {
					continue
				}
				actualType := unionType
				if refSchema, ok := unionType.(*avro.RefSchema); ok {
					actualType = refSchema.Schema()
				}

				if namedSchema, ok := actualType.(avro.NamedSchema); ok && namedSchema.FullName() == key {
					// Found matching type - unwrap and recursively process
					return unwrapUnionValues(wrappedValue, actualType)
				}
			}
		}
	}

	// Not wrapped - try to recursively unwrap nested structures
	// Find the first matching union type that can successfully unwrap the value
	for _, unionType := range unionSchema.Types() {
		if unionType.Type() == avro.Null {
			continue
		}
		actualType := unionType
		if refSchema, ok := unionType.(*avro.RefSchema); ok {
			actualType = refSchema.Schema()
		}

		if unwrapped, err := unwrapUnionValues(value, actualType); err == nil {
			return unwrapped, nil
		}
	}

	// If we can't determine the type, return as-is
	return value, nil
}

// Deserialize deserializes a Avro binary into a JSON object.
func (*AvroSerde) Deserialize(data []byte, schema *Schema) (any, *Xk6KafkaError) {
	avroSchema := schema.Codec()
	if avroSchema == nil {
		return nil, NewXk6KafkaError(failedToDecodeFromBinary, "Failed to parse Avro schema", nil)
	}

	var decodedData any
	err := avro.Unmarshal(avroSchema, data, &decodedData)
	if err != nil {
		return nil, NewXk6KafkaError(
			failedToDecodeFromBinary, "Failed to decode data", err)
	}

	// Unwrap union values that are wrapped in {"typeName": value} format
	unwrappedData, unwrapErr := unwrapUnionValues(decodedData, avroSchema)
	if unwrapErr != nil {
		// Return original data if unwrapping fails
		unwrappedData = decodedData
	}

	if data, ok := unwrappedData.(map[string]any); ok {
		return data, nil
	}
	return unwrappedData, nil
}
