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

// Medical Record Entry schema - simplified version for testing
const medicalRecordEntrySchema = `{
	"type": "record",
	"name": "Entry",
	"namespace": "orbis.u.medicalrecord.serdes.avro.notification",
	"fields": [
		{"name": "orbisEventCreationDate", "type": "long"},
		{"name": "eventId", "type": "long"},
		{"name": "eventClass", "type": "string"},
		{
			"name": "application",
			"type": ["null", "string"]
		},
		{"name": "initiator", "type": "string"},
		{"name": "entryId", "type": "string"},
		{
			"name": "move",
			"type": ["null", "boolean"]
		},
		{
			"name": "cancelOnly",
			"type": ["null", "boolean"]
		},
		{
			"name": "sealed",
			"type": ["null", "boolean"]
		},
		{
			"name": "reason",
			"type": ["null", "string"]
		},
		{
			"name": "medicalRecordEntry",
			"type": [
				"null",
				{
					"type": "record",
					"name": "MedicalRecordEntry",
					"namespace": "orbis.u.medicalrecord.serdes.avro.notification",
					"fields": [
						{"name": "caseIdentifier", "type": "string"},
						{"name": "patientIdentifier", "type": "string"},
						{"name": "form", "type": "string"},
						{"name": "title", "type": "string"},
						{
							"name": "description",
							"type": ["null", "string"]
						},
						{
							"name": "searchContent",
							"type": ["null", "string"]
						},
						{"name": "creatingApplication", "type": "string"},
						{"name": "author", "type": "string"},
						{"name": "medicalDate", "type": "long"},
						{"name": "showDateOnly", "type": "boolean"},
						{
							"name": "version",
							"type": ["null", "string"]
						},
						{"name": "documentStatus", "type": "string"},
						{"name": "workflowStatus", "type": "string"},
						{
							"name": "createdBy",
							"type": ["null", "string"]
						},
						{"name": "updatedBy", "type": "string"},
						{"name": "updatedAt", "type": "long"},
						{
							"name": "deletedAt",
							"type": ["null", "long"]
						},
						{"name": "summary", "type": "string"},
						{"name": "summaryMediaType", "type": "string"},
						{"name": "sealed", "type": "boolean"},
						{
							"name": "additionalInfo",
							"type": ["null", "string"]
						},
						{
							"name": "entryType",
							"type": ["null", "string"]
						},
						{
							"name": "specialties",
							"type": [
								"null",
								{
									"type": "array",
									"items": "string"
								}
							]
						},
						{
							"name": "link",
							"type": {
								"type": "record",
								"name": "Link",
								"namespace": "orbis.u.medicalrecord.serdes.avro.notification",
								"fields": [
									{"name": "module", "type": "string"},
									{
										"name": "editIdentifier",
										"type": ["null", "string"]
									},
									{
										"name": "requestHandler",
										"type": ["null", "string"]
									},
									{
										"name": "component",
										"type": ["null", "string"]
									},
									{"name": "documentId", "type": "string"},
									{
										"name": "path",
										"type": ["null", "string"]
									},
									{"name": "pdfExist", "type": "boolean"},
									{"name": "primitivum", "type": "string"},
									{
										"name": "orderNumber",
										"type": ["null", "string"]
									},
									{
										"name": "stringParameters",
										"type": ["null", "string"]
									}
								]
							}
						}
					]
				}
			]
		}
	]
}`

// TestMedicalRecordEntry_AllDataFiles tests all documents in the medicalrecord entry data file
func TestMedicalRecordEntry_AllDataFiles(t *testing.T) {
	// Parse schema
	schema, err := avro.Parse(medicalRecordEntrySchema)
	require.NoError(t, err, "Schema should parse successfully")

	schemaWrapper := &Schema{
		Schema:     medicalRecordEntrySchema,
		avroSchema: schema,
	}

	serde := &AvroSerde{}

	// Try to find the data file
	dataFilePaths := []string{
		"../k6-kafka-load-testing/k6-image/src/data/avro/orbis.medicalrecord.entry.json",
		"../../k6-kafka-load-testing/k6-image/src/data/avro/orbis.medicalrecord.entry.json",
		"../../../k6-kafka-load-testing/k6-image/src/data/avro/orbis.medicalrecord.entry.json",
	}

	var dataFilePath string
	for _, path := range dataFilePaths {
		absPath, _ := filepath.Abs(path)
		if _, err := os.Stat(absPath); err == nil {
			dataFilePath = absPath
			break
		}
	}

	if dataFilePath == "" {
		t.Skip("Data file not found, skipping test. This test should be run from the xk6-kafka directory")
		return
	}

	t.Logf("Loading data from: %s", dataFilePath)

	// Read the JSON file
	data, err := os.ReadFile(dataFilePath)
	require.NoError(t, err, "Should read data file successfully")

	// Parse the JSON structure
	var jsonData struct {
		Docs []map[string]any `json:"docs"`
	}
	err = json.Unmarshal(data, &jsonData)
	require.NoError(t, err, "Should parse JSON successfully")

	t.Logf("Found %d documents to test", len(jsonData.Docs))

	// Track statistics
	totalDocs := len(jsonData.Docs)
	successCount := 0
	failureCount := 0
	nullEntryCount := 0

	// Test each document
	for i, doc := range jsonData.Docs {
		docNum := i + 1
		t.Run(formatDocName(doc, docNum), func(t *testing.T) {
			// Check if medicalRecordEntry is null
			medRecEntry := doc["medicalRecordEntry"]
			if medRecEntry == nil {
				t.Logf("Doc %d: medicalRecordEntry is null (valid for DELETE/PDF/SEAL events)", docNum)
				nullEntryCount++
				
				// Still try to serialize - this should work
				result, xk6Err := serde.SerializeJSONAvro(doc, schemaWrapper)
				require.Nil(t, xk6Err, "Should serialize document with null medicalRecordEntry")
				require.NotNil(t, result)
				
				successCount++
				return
			}

			// Log the structure
			t.Logf("Doc %d structure:", docNum)
			t.Logf("  eventId: %v", doc["eventId"])
			t.Logf("  eventClass: %v", doc["eventClass"])
			t.Logf("  entryId: %v", doc["entryId"])

			// Check if it's already wrapped
			medRecMap, ok := medRecEntry.(map[string]any)
			if !ok {
				t.Errorf("Doc %d: medicalRecordEntry is not a map: %T", docNum, medRecEntry)
				failureCount++
				return
			}

			// Check for the union wrapper
			wrapper, hasWrapper := medRecMap["orbis.u.medicalrecord.serdes.avro.notification.MedicalRecordEntry"]
			if !hasWrapper {
				t.Errorf("Doc %d: Missing union wrapper key", docNum)
				failureCount++
				return
			}

			actualEntry, ok := wrapper.(map[string]any)
			if !ok {
				t.Errorf("Doc %d: Wrapped value is not a map: %T", docNum, wrapper)
				failureCount++
				return
			}

			t.Logf("  medicalRecordEntry:")
			t.Logf("    caseIdentifier: %v", actualEntry["caseIdentifier"])
			t.Logf("    patientIdentifier: %v", actualEntry["patientIdentifier"])
			t.Logf("    form: %v", actualEntry["form"])

			// Check specialties field - this is where the error occurs
			if specialties, hasSpecialties := actualEntry["specialties"]; hasSpecialties {
				t.Logf("    specialties (raw): %v (type: %T)", specialties, specialties)
				
				// Check if it's already wrapped correctly
				if specMap, isMap := specialties.(map[string]any); isMap {
					if arrayVal, hasArray := specMap["array"]; hasArray {
						t.Logf("    specialties.array: %v", arrayVal)
					} else {
						t.Logf("    specialties map keys: %v", getMapKeys(specMap))
					}
				} else if specArray, isArray := specialties.([]any); isArray {
					t.Logf("    specialties array: %v", specArray)
				}
			}

			// Try to serialize
			result, xk6Err := serde.SerializeJSONAvro(doc, schemaWrapper)
			
			if xk6Err != nil {
				t.Errorf("Doc %d: Serialization failed: %v", docNum, xk6Err)
				t.Logf("Full document: %s", formatJSON(doc))
				failureCount++
				return
			}

			require.NotNil(t, result, "Doc %d: Result should not be nil", docNum)
			
			// Verify the result is valid JSON
			var output map[string]any
			err = json.Unmarshal(result, &output)
			require.NoError(t, err, "Doc %d: Result should be valid JSON", docNum)

			t.Logf("Doc %d: ✅ Serialization successful (%d bytes)", docNum, len(result))
			successCount++
		})
	}

	// Print summary
	t.Logf("\n=== TEST SUMMARY ===")
	t.Logf("Total documents: %d", totalDocs)
	t.Logf("Successful: %d", successCount)
	t.Logf("Failed: %d", failureCount)
	t.Logf("Null entries: %d", nullEntryCount)
	t.Logf("==================")

	// Fail the test if any document failed
	if failureCount > 0 {
		t.Errorf("❌ %d/%d documents failed serialization", failureCount, totalDocs)
	} else {
		t.Logf("✅ All documents serialized successfully!")
	}
}

// formatDocName creates a descriptive test name for each document
func formatDocName(doc map[string]any, docNum int) string {
	eventClass := "UNKNOWN"
	if ec, ok := doc["eventClass"]; ok {
		eventClass = ec.(string)
	}
	
	eventId := "unknown"
	if eid, ok := doc["eventId"]; ok {
		eventId = formatValue(eid)
	}

	hasEntry := "null_entry"
	if doc["medicalRecordEntry"] != nil {
		hasEntry = "has_entry"
	}

	return formatTestName("doc_%03d_%s_%s_%s", docNum, eventClass, eventId, hasEntry)
}

// formatValue converts a value to a string representation
func formatValue(v any) string {
	switch val := v.(type) {
	case float64:
		return formatInt(int64(val))
	case int64:
		return formatInt(val)
	case int:
		return formatInt(int64(val))
	case string:
		return val
	default:
		return "unknown"
	}
}

// formatInt formats an integer as a string
func formatInt(i int64) string {
	return string(rune(i/1000000)) + string(rune((i/1000)%1000)) + string(rune(i%1000))
}

// formatTestName formats a test name
func formatTestName(format string, args ...any) string {
	result := format
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			result = replaceFirst(result, "%s", v)
		case int:
			result = replaceFirst(result, "%d", string(rune(v)))
			result = replaceFirst(result, "%03d", padLeft(string(rune(v)), 3))
		}
	}
	return result
}

// replaceFirst replaces the first occurrence of old with new in s
func replaceFirst(s, old, new string) string {
	for i := 0; i < len(s)-len(old)+1; i++ {
		if s[i:i+len(old)] == old {
			return s[:i] + new + s[i+len(old):]
		}
	}
	return s
}

// padLeft pads a string with zeros on the left
func padLeft(s string, length int) string {
	for len(s) < length {
		s = "0" + s
	}
	return s
}

// getMapKeys returns the keys of a map
func getMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// formatJSON formats a value as pretty JSON
func formatJSON(v any) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "<error formatting JSON>"
	}
	return string(b)
}

// TestMedicalRecordEntry_SpecialtiesFieldIsolated tests the specialties field in isolation
func TestMedicalRecordEntry_SpecialtiesFieldIsolated(t *testing.T) {
	// Minimal schema with just the specialties union
	minimalSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{
				"name": "specialties",
				"type": [
					"null",
					{
						"type": "array",
						"items": "string"
					}
				]
			}
		]
	}`

	schema, err := avro.Parse(minimalSchema)
	require.NoError(t, err)

	schemaWrapper := &Schema{
		Schema:     minimalSchema,
		avroSchema: schema,
	}

	serde := &AvroSerde{}

	tests := []struct {
		name        string
		input       map[string]any
		shouldError bool
	}{
		{
			name: "null_value",
			input: map[string]any{
				"specialties": nil,
			},
			shouldError: false,
		},
		{
			name: "array_with_wrapper",
			input: map[string]any{
				"specialties": map[string]any{
					"array": []any{"KG INT", "CARDIO"},
				},
			},
			shouldError: false,
		},
		{
			name: "plain_array",
			input: map[string]any{
				"specialties": []any{"KG INT", "CARDIO"},
			},
			shouldError: false,
		},
		{
			name: "empty_array_with_wrapper",
			input: map[string]any{
				"specialties": map[string]any{
					"array": []any{},
				},
			},
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, xk6Err := serde.SerializeJSONAvro(tt.input, schemaWrapper)
			
			if tt.shouldError {
				assert.NotNil(t, xk6Err, "Should return an error")
			} else {
				assert.Nil(t, xk6Err, "Should not return an error: %v", xk6Err)
				assert.NotNil(t, result, "Result should not be nil")
				
				var output map[string]any
				err := json.Unmarshal(result, &output)
				require.NoError(t, err)
				t.Logf("Output: %s", formatJSON(output))
			}
		})
	}
}
