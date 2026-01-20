package kafka

import (
	"fmt"

	"github.com/riferrei/srclient"
	"github.com/sirupsen/logrus"
	"go.k6.io/k6/js/common"
)

type Container struct {
	Data              any                 `json:"data"`
	Schema            *Schema             `json:"schema"`
	SchemaType        srclient.SchemaType `json:"schemaType"`
	AvroEncoding      string              `json:"avroEncoding"`
	SchemaRegistryURL string              `json:"schemaRegistryURL"`
	Headers           map[string]any      `json:"headers"`
}

// serialize checks whether the incoming data has a schema or not.
// If the data has a schema, it encodes the data into Avro, JSONSchema or Protocol Buffer.
// Then it adds the wire format prefix and returns the binary to be used in key or value.
// If no schema is passed, it treats the data as a byte array, a string or a JSON object without
// a JSONSchema. Then, it returns the data as a byte array.
// nolint: funlen
func (k *Kafka) serialize(container *Container) []byte {
	// Auto-fetch schema from registry if schema is incomplete but we have registry URL and subject
	if container.Schema != nil && container.Schema.Schema == "" && container.Schema.Subject != "" && container.SchemaRegistryURL != "" {
		logrus.WithFields(logrus.Fields{
			"subject": container.Schema.Subject,
			"version": container.Schema.Version,
		}).Info("🔍 Auto-fetching schema from registry for serialization")

		// Create a schema registry client if not already created
		if k.currentSchemaRegistry == nil {
			config := &SchemaRegistryConfig{
				URL:           container.SchemaRegistryURL,
				EnableCaching: true,
			}
			client := k.schemaRegistryClient(config)
			k.currentSchemaRegistry = client
		}

		// Fetch the schema
		fetchedSchema := k.getSchema(k.currentSchemaRegistry, container.Schema)
		if fetchedSchema != nil {
			container.Schema = fetchedSchema
			logrus.WithFields(logrus.Fields{
				"subject":  container.Schema.Subject,
				"schemaID": container.Schema.ID,
				"version":  container.Schema.Version,
			}).Info("✅ Schema fetched successfully")
		} else {
			err := fmt.Errorf("failed to fetch schema for subject: %s", container.Schema.Subject)
			common.Throw(k.vu.Runtime(), err)
			return nil
		}
	}

	if container.Schema == nil {
		// we are dealing with a byte array, a string or a JSON object without a JSONSchema
		serde, err := GetSerdes(container.SchemaType)
		if err != nil {
			common.Throw(k.vu.Runtime(), err)
			return nil
		}

		data, err := serde.Serialize(container.Data, nil)
		if err != nil {
			common.Throw(k.vu.Runtime(), err)
			return nil
		}
		return data
	}
	// we are dealing with binary data to be encoded with Avro, JSONSchema or Protocol Buffer

	// If the schema was unmarshaled from JSON, it won't have the resolver function.
	// Try to get the schema from cache if caching is enabled,
	// as the cached version will have the resolver.
	if container.Schema != nil {
		if container.Schema.EnableCaching {
			if cachedSchema, ok := k.schemaCache[container.Schema.Subject]; ok {
				container.Schema = cachedSchema
			}
		}

		// If schema doesn't have a resolver but has references,
		// create one using the stored schema registry client
		if container.Schema.resolver == nil && len(container.Schema.References) > 0 {
			if k.currentSchemaRegistry != nil {
				container.Schema.resolver = k.createResolver(
					k.currentSchemaRegistry, container.Schema.EnableCaching)
			}
		}
	}

	switch container.SchemaType {
	case srclient.Avro, srclient.Json:
		// DEBUG: Log encoding detection
		logrus.WithFields(logrus.Fields{
			"schemaType":   container.SchemaType,
			"avroEncoding": container.AvroEncoding,
			"isAvro":       container.SchemaType == srclient.Avro,
			"isJSON":       container.AvroEncoding == "json",
		}).Warn("🔍 DEBUG: Checking encoding type")
		
		// For JSON Avro encoding, use hamba/avro to encode to JSON Avro format
		// This produces proper JSON Avro with union types wrapped correctly
		if container.SchemaType == srclient.Avro && container.AvroEncoding == "json" {
			// Use the AvroSerde to encode to JSON Avro
			serde, err := GetSerdes(container.SchemaType)
			if err != nil {
				common.Throw(k.vu.Runtime(), err)
				return nil
			}
			
			avroSerde, ok := serde.(*AvroSerde)
			if !ok {
				common.Throw(k.vu.Runtime(), fmt.Errorf("failed to get AvroSerde"))
				return nil
			}
			
			// Serialize using SerializeJSONAvro which produces JSON Avro format
			jsonAvroBytes, serdeErr := avroSerde.SerializeJSONAvro(container.Data, container.Schema)
			if serdeErr != nil {
				common.Throw(k.vu.Runtime(), serdeErr)
				return nil
			}

			logrus.WithFields(logrus.Fields{
				"schemaID":   container.Schema.ID,
				"dataLength": len(jsonAvroBytes),
			}).Info("✅ Serialized to JSON Avro (no wire format, will use Apicurio headers)")

			// Return JSON Avro directly without wire format - the schema ID will be in Kafka headers
			return jsonAvroBytes
		}

		// For binary Avro encoding, use the serde (hamba/avro)
		serde, err := GetSerdes(container.SchemaType)
		if err != nil {
			common.Throw(k.vu.Runtime(), err)
			return nil
		}

		bytesData, err := serde.Serialize(container.Data, container.Schema)
		if err != nil {
			common.Throw(k.vu.Runtime(), err)
			return nil
		}

		// Add wire format for binary Avro
		return k.encodeWireFormat(bytesData, container.Schema.ID)
	case srclient.Protobuf:
		common.Throw(k.vu.Runtime(), ErrUnsupportedOperation)
		return nil
	default:
		common.Throw(k.vu.Runtime(), ErrUnsupportedOperation)
		return nil
	}
}

// deserialize checks whether the incoming data has a schema or not.
// If the data has a schema, it removes the wire format prefix and decodes the data into JSON
// using Avro, JSONSchema or Protocol Buffer schemas. It returns the decoded data as JSON object.
// If no schema is passed, it treats the data as a byte array, a string or a JSON object without
// a JSONSchema. Then, it returns the data based on how it can decode it.
// nolint: funlen
func (k *Kafka) deserialize(container *Container) any {
	if container.Schema == nil {
		// we are dealing with a byte array, a string or a JSON object without a JSONSchema
		serde, err := GetSerdes(container.SchemaType)
		if err != nil {
			common.Throw(k.vu.Runtime(), err)
			return nil
		}

		switch data := container.Data.(type) {
		case []byte:
			switch container.SchemaType {
			case String:
				return string(data)
			case srclient.Avro, srclient.Json:
				if isJSON(data) {
					js, err := toMap(data)
					if err != nil {
						common.Throw(k.vu.Runtime(), err)
						return nil
					}
					return js
				}
				return data
			case srclient.Protobuf:
				return data
			default:
				return data
			}
		case string:
			if isBase64Encoded(data) {
				decodedData, err := base64ToBytes(data)
				if err != nil {
					common.Throw(k.vu.Runtime(), err)
					return nil
				}
				result, err := serde.Deserialize(decodedData, nil)
				if err != nil {
					common.Throw(k.vu.Runtime(), err)
					return nil
				}
				return result
			}
			return []byte(data)
		default:
			return container.Data
		}
	} else {
		// we are dealing with binary data to be encoded with Avro, JSONSchema or Protocol Buffer
		runtime := k.vu.Runtime()

		var jsonBytes []byte

		switch data := container.Data.(type) {
		case []byte:
			jsonBytes = data
		case string:
			// Decode the data into JSON bytes from base64-encoded data
			if isBase64Encoded(data) {
				decodedData, err := base64ToBytes(data)
				if err != nil {
					common.Throw(k.vu.Runtime(), err)
					return nil
				}
				jsonBytes = decodedData
			}
		}

		// Remove wire format prefix
		jsonBytes = k.decodeWireFormat(jsonBytes)

		// If the schema was unmarshaled from JSON, it won't have the resolver function.
		// Try to get the schema from cache if caching is enabled,
		// as the cached version will have the resolver.
		if container.Schema != nil {
			if container.Schema.EnableCaching {
				if cachedSchema, ok := k.schemaCache[container.Schema.Subject]; ok {
					// Use the cached schema which has the resolver set
					container.Schema = cachedSchema
				}
			}

			// If schema doesn't have a resolver but has references,
			// create one using the stored schema registry client
			if container.Schema.resolver == nil && len(container.Schema.References) > 0 {
				if k.currentSchemaRegistry != nil {
					container.Schema.resolver = k.createResolver(
						k.currentSchemaRegistry, container.Schema.EnableCaching)
				}
			}
		}

		switch container.SchemaType {
		case srclient.Avro, srclient.Json:
			serde, err := GetSerdes(container.SchemaType)
			if err != nil {
				common.Throw(k.vu.Runtime(), err)
				return nil
			}

			deserialized, err := serde.Deserialize(jsonBytes, container.Schema)
			if err != nil {
				common.Throw(k.vu.Runtime(), err)
				return nil
			}

			if jsonObj, ok := deserialized.(map[string]any); ok {
				return jsonObj
			}
			common.Throw(k.vu.Runtime(), ErrInvalidDataType)
			return nil
		case srclient.Protobuf:
			common.Throw(runtime, ErrUnsupportedOperation)
			return nil
		default:
			common.Throw(runtime, ErrUnsupportedOperation)
			return nil
		}
	}
}
