package kafka

import (
	"encoding/json"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test with REAL orbis.medicalrecord.entry schema and data
func TestAvroJSON_RealMedicalRecordEntry(t *testing.T) {
	// Simplified schema (just the problematic medicalRecordEntry field)
	schemaStr := `{
		"type": "record",
		"name": "Entry",
		"namespace": "orbis.u.medicalrecord.serdes.avro.notification",
		"fields": [
			{
				"name": "eventId",
				"type": "long"
			},
			{
				"name": "medicalRecordEntry",
				"type": [
					"null",
					{
						"namespace": "orbis.u.medicalrecord.serdes.avro.notification",
						"type": "record",
						"name": "MedicalRecordEntry",
						"fields": [
							{"name": "caseIdentifier", "type": "string"},
							{"name": "patientIdentifier", "type": "string"},
							{"name": "form", "type": "string"}
						]
					}
				]
			}
		]
	}`

	schema, err := avro.Parse(schemaStr)
	require.NoError(t, err)

	serde := &AvroSerde{}

	t.Run("pre-wrapped union like in JSON files", func(t *testing.T) {
		// Data EXACTLY as it appears in orbis.medicalrecord.entry.json
		inputJSON := `{
			"eventId": 2196648751,
			"medicalRecordEntry": {
				"orbis.u.medicalrecord.serdes.avro.notification.MedicalRecordEntry": {
					"caseIdentifier": "100000243",
					"patientIdentifier": "75010000307",
					"form": "OP Bericht"
				}
			}
		}`

		var input map[string]any
		err := json.Unmarshal([]byte(inputJSON), &input)
		require.NoError(t, err)

		t.Logf("Input data: %+v", input)

		// Serialize to JSON Avro
		schemaWrapper := &Schema{Schema: schemaStr, avroSchema: schema}
		result, xk6Err := serde.SerializeJSONAvro(input, schemaWrapper)

		if xk6Err != nil {
			t.Logf("Error: %v", xk6Err)
		}
		require.Nil(t, xk6Err, "Should handle pre-wrapped union without error")
		require.NotNil(t, result)

		// Parse result
		var output map[string]any
		err = json.Unmarshal(result, &output)
		require.NoError(t, err)

		t.Logf("Output data: %+v", output)

		// Verify structure - should keep the wrapper
		medRecEntry, ok := output["medicalRecordEntry"].(map[string]any)
		require.True(t, ok, "medicalRecordEntry should be a map")

		// Should have the type wrapper
		wrappedValue, ok := medRecEntry["orbis.u.medicalrecord.serdes.avro.notification.MedicalRecordEntry"].(map[string]any)
		require.True(t, ok, "Union should still be wrapped with type name")

		assert.Equal(t, "100000243", wrappedValue["caseIdentifier"])
		assert.Equal(t, "75010000307", wrappedValue["patientIdentifier"])
		assert.Equal(t, "OP Bericht", wrappedValue["form"])
	})

	t.Run("null value", func(t *testing.T) {
		inputJSON := `{
			"eventId": 123456,
			"medicalRecordEntry": null
		}`

		var input map[string]any
		err := json.Unmarshal([]byte(inputJSON), &input)
		require.NoError(t, err)

		schemaWrapper := &Schema{Schema: schemaStr, avroSchema: schema}
		result, xk6Err := serde.SerializeJSONAvro(input, schemaWrapper)
		require.Nil(t, xk6Err)
		require.NotNil(t, result)

		var output map[string]any
		err = json.Unmarshal(result, &output)
		require.NoError(t, err)

		// Null should NOT be wrapped in Avro JSON - it stays as null
		assert.Nil(t, output["medicalRecordEntry"], "null should remain null, NOT be wrapped")
	})
}
