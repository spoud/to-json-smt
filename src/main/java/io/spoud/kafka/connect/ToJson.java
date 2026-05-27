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
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class ToJson<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> props) {
    }

    @Override
    public R apply(R record) {
        var jsonString = objectMapper.writeValueAsString(convertValue(record.value()));
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
        return new ConfigDef();
    }

    @Override
    public void close() {
    }
}