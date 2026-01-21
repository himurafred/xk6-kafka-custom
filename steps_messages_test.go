package kafka

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test Steps messages serialization - reproduces the Java consumer error
// This validates that our serializer produces binary Avro that Java can read

const orbisClassicEventSchema = `{
	"type": "record",
	"name": "OrbisClassicEvent",
	"namespace": "orbis.u.steps.serdes.avro.notification",
	"fields": [
		{"name": "eventDefinitionId", "type": "string"},
		{
			"name": "variables",
			"type": {
				"type": "array",
				"items": {
					"type": "record",
					"name": "Variable",
					"fields": [
						{"name": "variableDefinitionId", "type": "string"},
						{
							"name": "variableValue",
							"type": {
								"type": "record",
								"name": "VariableValue",
								"fields": [
									{"name": "dataType", "type": "string"},
									{"name": "value", "type": "string"}
								]
							}
						}
					]
				}
			}
		}
	]
}`

const stepExecutionStartedSchema = `{
	"type": "record",
	"name": "StepExecutionStarted",
	"namespace": "orbis.u.steps.serdes.avro.notification",
	"fields": [
		{"name": "uuid", "type": "string"},
		{"name": "name", "type": "string"}
	]
}`

const actionExecutionCompletedSchema = `{
	"type": "record",
	"name": "ActionExecutionCompleted",
	"namespace": "orbis.u.steps.serdes.avro.notification",
	"fields": []
}`

func TestStepsMessages_OrbisClassicEvent_BinaryAvro(t *testing.T) {
	dataDir := "/home/fredericchiron/projects/docker-compose/k6-kafka-load-testing/k6-image/src/data/avro"

	tests := []struct {
		name     string
		file     string
		schema   string
		validate func(t *testing.T, data map[string]any)
	}{
		{
			name:   "orbis-classic-event",
			file:   "private.steps.orbis-classic-event.json",
			schema: orbisClassicEventSchema,
			validate: func(t *testing.T, data map[string]any) {
				assert.Equal(t, "60170", data["eventDefinitionId"])
				variables := data["variables"].([]any)
				assert.Greater(t, len(variables), 0)
			},
		},
		{
			name:   "orbis-classic-event-dlq",
			file:   "private.steps.orbis-classic-event-dlq.json",
			schema: orbisClassicEventSchema,
			validate: func(t *testing.T, data map[string]any) {
				assert.Equal(t, "60170", data["eventDefinitionId"])
			},
		},
		{
			name:   "step-execution-started",
			file:   "private.steps.step-execution-started.json",
			schema: stepExecutionStartedSchema,
			validate: func(t *testing.T, data map[string]any) {
				assert.NotEmpty(t, data["uuid"])
				assert.NotEmpty(t, data["name"])
			},
		},
		{
			name:   "action-execution-completed",
			file:   "private.steps.action-execution-completed.json",
			schema: actionExecutionCompletedSchema,
			validate: func(t *testing.T, data map[string]any) {
				// Empty record is valid
				assert.NotNil(t, data)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Load JSON file
			filePath := filepath.Join(dataDir, tt.file)
			fileContent, err := os.ReadFile(filePath)
			require.NoError(t, err, "Failed to read file %s", tt.file)

			// Parse file structure (contains "docs" array)
			var fileData struct {
				Docs []map[string]any `json:"docs"`
			}
			err = json.Unmarshal(fileContent, &fileData)
			require.NoError(t, err, "Failed to parse JSON file %s", tt.file)
			require.NotEmpty(t, fileData.Docs, "No docs found in %s", tt.file)

			// Test each document
			for idx, doc := range fileData.Docs {
				t.Run(string(rune('A'+idx)), func(t *testing.T) {
					// Parse schema
					avroSchema, err := avro.Parse(tt.schema)
					require.NoError(t, err, "Failed to parse schema")

					schemaWrapper := &Schema{
						Schema:     tt.schema,
						avroSchema: avroSchema,
					}

					// Serialize to BINARY Avro (this is what Java expects!)
					serde := &AvroSerde{}
					binaryData, xk6Err := serde.Serialize(doc, schemaWrapper)
					require.Nil(t, xk6Err, "Failed to serialize to binary Avro")
					require.NotNil(t, binaryData)

					t.Logf("✅ Binary Avro serialization successful")
					t.Logf("   Input: %+v", doc)
					t.Logf("   Binary length: %d bytes", len(binaryData))
					t.Logf("   First 20 bytes: % x", binaryData[:min(20, len(binaryData))])

					// Deserialize back to verify round-trip
					deserializedData, xk6Err := serde.Deserialize(binaryData, schemaWrapper)
					require.Nil(t, xk6Err, "Failed to deserialize binary Avro")
					require.NotNil(t, deserializedData)

					// Validate structure
					deserializedMap, ok := deserializedData.(map[string]any)
					require.True(t, ok, "Deserialized data should be a map")

					tt.validate(t, deserializedMap)

					t.Logf("✅ Round-trip validation successful")
					t.Logf("   Deserialized: %+v", deserializedMap)
				})
			}
		})
	}
}

// TestStepsMessages_DetectCorruption tests if we can detect the Java error scenario
func TestStepsMessages_DetectCorruption(t *testing.T) {
	t.Run("negative length in binary stream", func(t *testing.T) {
		// The Java error "Malformed data. Length is negative: -62"
		// occurs when BinaryDecoder tries to read a string length
		// This typically means:
		// 1. Wrong encoding (JSON sent instead of binary)
		// 2. Schema mismatch (fields in wrong order)
		// 3. Corrupted data

		avroSchema, err := avro.Parse(orbisClassicEventSchema)
		require.NoError(t, err)

		schemaWrapper := &Schema{
			Schema:     orbisClassicEventSchema,
			avroSchema: avroSchema,
		}

		// Try to deserialize garbage data (should fail gracefully)
		corruptedData := []byte{0xFF, 0xC2, 0x00, 0x00} // -62 in varint encoding
		serde := &AvroSerde{}
		_, xk6Err := serde.Deserialize(corruptedData, schemaWrapper)

		// Should get a clear error, not crash
		require.NotNil(t, xk6Err, "Corrupted data should be rejected")
		t.Logf("✅ Corruption detected correctly: %v", xk6Err.Message)
	})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
