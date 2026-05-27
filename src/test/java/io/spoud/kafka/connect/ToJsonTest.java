package io.spoud.kafka.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ToJsonTest {
    @Test
    public void copySchemaAndInsertUuidField() {
        try(var toJson = new ToJson<SourceRecord>()) {
            final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.INT64_SCHEMA).build();
            final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L);

            final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
            final SourceRecord transformedRecord = toJson.apply(record);

            assertEquals( "{\"magic\":42}", transformedRecord.value());
        }
    }

    @Test
    public void testNestedStruct() {
        try(var toJson = new ToJson<SourceRecord>()) {
            final Schema innerSchema = SchemaBuilder.struct().name("inner").field("foo", Schema.STRING_SCHEMA).build();
            final Schema outerSchema = SchemaBuilder.struct().name("outer").field("inner", innerSchema).field("bar", Schema.INT32_SCHEMA).build();

            final Struct innerStruct = new Struct(innerSchema).put("foo", "hello");
            final Struct outerStruct = new Struct(outerSchema).put("inner", innerStruct).put("bar", 123);

            final SourceRecord record = new SourceRecord(null, null, "test", 0, outerSchema, outerStruct);
            final SourceRecord transformedRecord = toJson.apply(record);

            assertEquals("{\"bar\":123,\"inner\":{\"foo\":\"hello\"}}", transformedRecord.value());
        }
    }

    @Test
    public void testComplexTypes() {
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

            // Jackson might change order of keys in map, but for simple cases it's usually stable or we can check content
            String result = (String) transformedRecord.value();
            assertNotNull(result);
            // Check if it contains expected substrings to be less sensitive to order if necessary, 
            // but for these small objects it should be stable.
            assertEquals("{\"list\":[{\"field\":\"f1\"},{\"field\":\"f2\"}],\"map\":{\"key\":{\"field\":\"f1\"}}}", result);
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
}