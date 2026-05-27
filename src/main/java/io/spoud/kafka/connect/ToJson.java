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
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.WRAP_KEY_VALUE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH,
                    "Whether to transform the key-value pair by wrapping them into an object like {\"key\": ..., \"value\": ...}. If unset, jsonifies just the value.");

    private boolean wrapKeyValue;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        wrapKeyValue = config.getBoolean(ConfigName.WRAP_KEY_VALUE);
    }

    @Override
    public R apply(R record) {
        Object valueToConvert;
        if (wrapKeyValue) {
            Map<String, Object> map = new HashMap<>();
            map.put("key", record.key());
            map.put("value", record.value());
            valueToConvert = map;
        } else {
            valueToConvert = record.value();
        }

        var jsonString = objectMapper.writeValueAsString(convertValue(valueToConvert));
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, jsonString, record.timestamp());
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