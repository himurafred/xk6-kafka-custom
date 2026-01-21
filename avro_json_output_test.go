package kafka

import (
	"encoding/json"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/require"
)

// TestAvroJSON_NullHandling verifies that null values are NOT wrapped in unions
func TestAvroJSON_NullHandling(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "id", "type": "long"},
			{"name": "name", "type": ["null", "string"]},
			{"name": "age", "type": ["null", "int"]}
		]
	}`

	avroSchema, err := avro.Parse(schema)
	require.NoError(t, err)

	schemaWrapper := &Schema{
		Schema:     schema,
		avroSchema: avroSchema,
	}

	serde := &AvroSerde{}

	input := map[string]any{
		"id":   float64(123),
		"name": nil,
		"age":  nil,
	}

	result, xk6Err := serde.SerializeJSONAvro(input, schemaWrapper)
	require.Nil(t, xk6Err)
	require.NotNil(t, result)

	var output map[string]any
	err = json.Unmarshal(result, &output)
	require.NoError(t, err)

	t.Logf("Output JSON:\n%s", string(result))

	// Verify null is not wrapped
	require.Nil(t, output["name"], "name should be null, not {\"null\": null}")
	require.Nil(t, output["age"], "age should be null, not {\"null\": null}")
}

// TestAvroJSON_MedicalRecordSample shows actual output for a medical record
func TestAvroJSON_MedicalRecordSample(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Entry",
		"namespace": "orbis.u.medicalrecord.serdes.avro.notification",
		"fields": [
			{"name": "eventId", "type": "long"},
			{"name": "eventClass", "type": "string"},
			{"name": "application", "type": ["null", "string"]},
			{"name": "initiator", "type": "string"},
			{"name": "move", "type": ["null", "boolean"]},
			{
				"name": "medicalRecordEntry",
				"type": ["null", {
					"type": "record",
					"name": "MedicalRecordEntry",
					"fields": [
						{"name": "caseIdentifier", "type": "string"},
						{"name": "form", "type": "string"},
						{"name": "description", "type": ["null", "string"]},
						{
							"name": "specialties",
							"type": ["null", {"type": "array", "items": "string"}]
						}
					]
				}]
			}
		]
	}`

	avroSchema, err := avro.Parse(schema)
	require.NoError(t, err)

	schemaWrapper := &Schema{
		Schema:     schema,
		avroSchema: avroSchema,
	}

	serde := &AvroSerde{}

	// Input FULLY in Avro JSON format (all unions wrapped correctly)
	input := map[string]any{
		"eventId":     float64(2196648751),
		"eventClass":  "EDIT",
		"application": nil, // null stays null
		"initiator":   "10000",
		"move": map[string]any{
			"boolean": false, // Union wrapped correctly
		},
		"medicalRecordEntry": map[string]any{
			"orbis.u.medicalrecord.serdes.avro.notification.MedicalRecordEntry": map[string]any{
				"caseIdentifier": "100000243",
				"form":           "OP Bericht",
				"description":    nil, // null stays null
				"specialties": map[string]any{
					"array": []any{"KG INT"}, // Union of array correctly wrapped
				},
			},
		},
	}

	result, xk6Err := serde.SerializeJSONAvro(input, schemaWrapper)
	require.Nil(t, xk6Err)
	require.NotNil(t, result)

	var output map[string]any
	err = json.Unmarshal(result, &output)
	require.NoError(t, err)

	// Pretty print the output
	prettyJSON, _ := json.MarshalIndent(output, "", "  ")
	t.Logf("Output JSON:\n%s", string(prettyJSON))

	// Verify structure
	require.Nil(t, output["application"], "application should be null")

	// move should still be wrapped
	move := output["move"].(map[string]any)
	require.Equal(t, false, move["boolean"], "move should be wrapped boolean")

	medRecEntry := output["medicalRecordEntry"].(map[string]any)
	require.Contains(t, medRecEntry, "orbis.u.medicalrecord.serdes.avro.notification.MedicalRecordEntry")

	actualEntry := medRecEntry["orbis.u.medicalrecord.serdes.avro.notification.MedicalRecordEntry"].(map[string]any)
	require.Nil(t, actualEntry["description"], "description should be null")

	specialties := actualEntry["specialties"].(map[string]any)
	require.Contains(t, specialties, "array")
	require.Equal(t, []any{"KG INT"}, specialties["array"])
}
