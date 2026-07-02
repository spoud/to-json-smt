/*
 * Copyright © 2026 SPOUD AG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.spoud.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class ToJson<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final String OVERVIEW_DOC =
            "JSONify the record value, turning it into a JSON string.";

    private interface ConfigName {
        String WRAP_KEY_VALUE = "wrap";
        String PATH = "path";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.WRAP_KEY_VALUE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH,
                    "Whether to transform the key-value pair by wrapping them into an object like {\"key\": ..., \"value\": ...}. If unset, jsonifies just the value.")
            .define(ConfigName.PATH, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "The .-delimited path to the key that is to be jsonified. If not provided, jsonify the entire record.");

    private boolean wrapKeyValue;
    private String[] pathParts;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        wrapKeyValue = config.getBoolean(ConfigName.WRAP_KEY_VALUE);
        String path = config.getString(ConfigName.PATH);
        if (path != null && !path.trim().isEmpty()) {
            pathParts = path.split("\\.");
        }
    }

    @Override
    public R apply(R record) {
        Object valueToConvert;
        Schema schemaToConvert;
        if (wrapKeyValue) {
            Map<String, Object> map = new HashMap<>();
            map.put("key", record.key());
            map.put("value", record.value());
            valueToConvert = map;
            schemaToConvert = null;
        } else {
            valueToConvert = record.value();
            schemaToConvert = record.valueSchema();
        }

        if (pathParts == null || pathParts.length == 0) {
            try {
                var jsonString = objectMapper.writeValueAsString(convertValue(valueToConvert));
                return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, jsonString, record.timestamp());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            SchemaAndValue result = process(schemaToConvert, valueToConvert, pathParts, 0);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), result.schema, result.value, record.timestamp());
        }
    }

    private record SchemaAndValue(Schema schema, Object value) {
    }

    private SchemaAndValue process(Schema schema, Object value, String[] pathParts, int index) {
        if (index == pathParts.length) {
            try {
                Object jsonified = objectMapper.writeValueAsString(convertValue(value));
                Schema s = (schema != null && schema.isOptional()) ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
                return new SchemaAndValue(s, jsonified);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (value == null) {
            return new SchemaAndValue(schema, null);
        }

        String part = pathParts[index];

        if (value instanceof Struct struct) {
            Schema currentSchema = struct.schema();
            Field field = currentSchema.field(part);
            if (field == null) {
                return new SchemaAndValue(schema, value);
            }

            SchemaAndValue res = process(field.schema(), struct.get(part), pathParts, index + 1);

            Schema newSchema = updateSchema(currentSchema, part, res.schema);
            Struct newStruct = new Struct(newSchema);
            for (Field f : currentSchema.fields()) {
                if (f.name().equals(part)) {
                    newStruct.put(f.name(), res.value);
                } else {
                    newStruct.put(f.name(), struct.get(f));
                }
            }
            return new SchemaAndValue(newSchema, newStruct);
        } else if (value instanceof Map<?, ?> map) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> newMap = new HashMap<>((Map<Object, Object>) map);
            Object childValue = map.get(part);

            Schema childSchema = (schema != null && schema.type() == Schema.Type.MAP) ? schema.valueSchema() : null;

            SchemaAndValue res = process(childSchema, childValue, pathParts, index + 1);
            newMap.put(part, res.value);

            return new SchemaAndValue(null, newMap);
        }
        return new SchemaAndValue(schema, value);
    }

    private Schema updateSchema(Schema schema, String fieldName, Schema newFieldSchema) {
        SchemaBuilder builder = SchemaBuilder.struct()
                .name(schema.name())
                .version(schema.version())
                .doc(schema.doc());
        if (schema.parameters() != null) {
            builder.parameters(schema.parameters());
        }
        if (schema.isOptional()) {
            builder.optional();
        }
        for (Field field : schema.fields()) {
            if (field.name().equals(fieldName)) {
                builder.field(field.name(), newFieldSchema);
            } else {
                builder.field(field.name(), field.schema());
            }
        }
        return builder.build();
    }

    private Object convertValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Struct struct) {
            Map<String, Object> map = new HashMap<>();
            for (Field field : struct.schema().fields()) {
                map.put(field.name(), convertValue(struct.get(field)));
            }
            return map;
        } else if (value instanceof Map<?, ?> map) {
            Map<Object, Object> result = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                result.put(entry.getKey(), convertValue(entry.getValue()));
            }
            return result;
        } else if (value instanceof List<?> list) {
            return list.stream().map(this::convertValue).collect(Collectors.toList());
        }
        return value;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }
}