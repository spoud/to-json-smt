package io.spoud.kafka.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ToJsonTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void copySchemaAndInsertUuidField() throws Exception {
        try(var toJson = new ToJson<SourceRecord>()) {
            final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.INT64_SCHEMA).build();
            final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L);

            final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
            final SourceRecord transformedRecord = toJson.apply(record);

            Map<String, Object> result = objectMapper.readValue((String) transformedRecord.value(), new TypeReference<>() {});
            assertEquals(42, result.get("magic"));
        }
    }

    @Test
    public void testNestedStruct() throws Exception {
        try(var toJson = new ToJson<SourceRecord>()) {
            final Schema innerSchema = SchemaBuilder.struct().name("inner").field("foo", Schema.STRING_SCHEMA).build();
            final Schema outerSchema = SchemaBuilder.struct().name("outer").field("inner", innerSchema).field("bar", Schema.INT32_SCHEMA).build();

            final Struct innerStruct = new Struct(innerSchema).put("foo", "hello");
            final Struct outerStruct = new Struct(outerSchema).put("inner", innerStruct).put("bar", 123);

            final SourceRecord record = new SourceRecord(null, null, "test", 0, outerSchema, outerStruct);
            final SourceRecord transformedRecord = toJson.apply(record);

            Map<String, Object> result = objectMapper.readValue((String) transformedRecord.value(), new TypeReference<>() {});
            assertEquals(123, result.get("bar"));
            Map<String, Object> innerResult = (Map<String, Object>) result.get("inner");
            assertEquals("hello", innerResult.get("foo"));
        }
    }

    @Test
    public void testComplexTypes() throws Exception {
        try(var toJson = new ToJson<SourceRecord>()) {
            final Schema structSchema = SchemaBuilder.struct().name("struct").field("field", Schema.STRING_SCHEMA).build();
            final Schema schema = SchemaBuilder.struct().name("complex")
                    .field("list", SchemaBuilder.array(structSchema).build())
                    .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, structSchema).build())
                    .build();

            final Struct struct1 = new Struct(structSchema).put("field", "f1");
            final Struct struct2 = new Struct(structSchema).put("field", "f2");

            final Map<String, Struct> map = new HashMap<>();
            map.put("key", struct1);

            final Struct complexStruct = new Struct(schema)
                    .put("list", Arrays.asList(struct1, struct2))
                    .put("map", map);

            final SourceRecord record = new SourceRecord(null, null, "test", 0, schema, complexStruct);
            final SourceRecord transformedRecord = toJson.apply(record);

            Map<String, Object> result = objectMapper.readValue((String) transformedRecord.value(), new TypeReference<>() {});
            
            var list = (java.util.List<Map<String, Object>>) result.get("list");
            assertEquals(2, list.size());
            assertEquals("f1", list.get(0).get("field"));
            assertEquals("f2", list.get(1).get("field"));

            var resultMap = (Map<String, Map<String, Object>>) result.get("map");
            assertEquals("f1", resultMap.get("key").get("field"));
        }
    }

    @Test
    public void testNullValue() {
        try(var toJson = new ToJson<SourceRecord>()) {
            final SourceRecord record = new SourceRecord(null, null, "test", 0, null, null);
            final SourceRecord transformedRecord = toJson.apply(record);

            assertEquals("null", transformedRecord.value());
        }
    }

    @Test
    public void testWrapFeature() throws Exception {
        try (var toJson = new ToJson<SourceRecord>()) {
            Map<String, Object> props = new HashMap<>();
            props.put("wrap", true);
            toJson.configure(props);

            final Schema keySchema = Schema.STRING_SCHEMA;
            final String key = "my-key";
            final Schema valueSchema = SchemaBuilder.struct().name("value").field("foo", Schema.STRING_SCHEMA).build();
            final Struct value = new Struct(valueSchema).put("foo", "bar");

            final SourceRecord record = new SourceRecord(null, null, "test", 0, keySchema, key, valueSchema, value);
            final SourceRecord transformedRecord = toJson.apply(record);

            Map<String, Object> result = objectMapper.readValue((String) transformedRecord.value(), new TypeReference<>() {});
            assertEquals("my-key", result.get("key"));
            Map<String, Object> valueResult = (Map<String, Object>) result.get("value");
            assertEquals("bar", valueResult.get("foo"));
        }
    }

    @Test
    public void testWrapFeatureWithNullKey() throws Exception {
        try (var toJson = new ToJson<SourceRecord>()) {
            Map<String, Object> props = new HashMap<>();
            props.put("wrap", true);
            toJson.configure(props);

            final Schema valueSchema = SchemaBuilder.struct().name("value").field("foo", Schema.STRING_SCHEMA).build();
            final Struct value = new Struct(valueSchema).put("foo", "bar");

            final SourceRecord record = new SourceRecord(null, null, "test", 0, null, null, valueSchema, value);
            final SourceRecord transformedRecord = toJson.apply(record);

            Map<String, Object> result = objectMapper.readValue((String) transformedRecord.value(), new TypeReference<>() {});
            assertNull(result.get("key"));
            Map<String, Object> valueResult = (Map<String, Object>) result.get("value");
            assertEquals("bar", valueResult.get("foo"));
        }
    }
}