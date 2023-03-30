/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.util;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffset;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SinkUtilsTest {

    @Test
    public void testValidateAndParseEmptyPartitionOffsetMap() {
        // expect no exception to be thrown
        Map<TopicPartition, Long> parsedOffsets = SinkUtils.validateAndParseSinkConnectorOffsets(new HashMap<>());
        assertTrue(parsedOffsets.isEmpty());
    }

    @Test
    public void testValidateAndParseInvalidPartition() {
        Map<String, Object> partition = new HashMap<>();
        partition.put(ConnectorOffset.KAFKA_TOPIC_KEY, "topic");
        Map<String, Object> offset = new HashMap<>();
        offset.put(ConnectorOffset.KAFKA_OFFSET_KEY, 100);
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        partitionOffsets.put(partition, offset);

        // missing partition key
        ConnectException e = assertThrows(ConnectException.class, () -> SinkUtils.validateAndParseSinkConnectorOffsets(partitionOffsets));
        assertThat(e.getMessage(), containsString("The partition for sink connector offsets must contain the keys 'kafka_topic' and 'kafka_partition'"));

        partition.put(ConnectorOffset.KAFKA_PARTITION_KEY, "not a number");
        // bad partition key
        e = assertThrows(ConnectException.class, () -> SinkUtils.validateAndParseSinkConnectorOffsets(partitionOffsets));
        assertThat(e.getMessage(), containsString("Failed to parse the following Kafka partition value in the provided offsets: 'not a number'"));

        partition.remove(ConnectorOffset.KAFKA_TOPIC_KEY);
        partition.put(ConnectorOffset.KAFKA_PARTITION_KEY, "5");
        // missing topic key
        e = assertThrows(ConnectException.class, () -> SinkUtils.validateAndParseSinkConnectorOffsets(partitionOffsets));
        assertThat(e.getMessage(), containsString("The partition for sink connector offsets must contain the keys 'kafka_topic' and 'kafka_partition'"));
    }

    @Test
    public void testValidateAndParseInvalidOffset() {
        Map<String, Object> partition = new HashMap<>();
        partition.put(ConnectorOffset.KAFKA_TOPIC_KEY, "topic");
        partition.put(ConnectorOffset.KAFKA_PARTITION_KEY, 10);
        Map<String, Object> offset = new HashMap<>();
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        partitionOffsets.put(partition, offset);

        // missing offset key
        ConnectException e = assertThrows(ConnectException.class, () -> SinkUtils.validateAndParseSinkConnectorOffsets(partitionOffsets));
        assertThat(e.getMessage(), containsString("The offset for sink connectors should either be null or contain the key 'kafka_offset'"));

        // bad offset key
        offset.put(ConnectorOffset.KAFKA_OFFSET_KEY, "not a number");
        e = assertThrows(ConnectException.class, () -> SinkUtils.validateAndParseSinkConnectorOffsets(partitionOffsets));
        assertThat(e.getMessage(), containsString("Failed to parse the following Kafka offset value in the provided offsets: 'not a number'"));
    }

    @Test
    public void testStringPartitionValue() {
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = createPartitionOffsetMap("topic", "10", "100");
        Map<TopicPartition, Long> parsedOffsets = SinkUtils.validateAndParseSinkConnectorOffsets(partitionOffsets);
        assertEquals(1, parsedOffsets.size());
        TopicPartition tp = parsedOffsets.keySet().iterator().next();
        assertEquals(10, tp.partition());
    }

    @Test
    public void testIntegerPartitionValue() {
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = createPartitionOffsetMap("topic", 10, "100");
        Map<TopicPartition, Long> parsedOffsets = SinkUtils.validateAndParseSinkConnectorOffsets(partitionOffsets);
        assertEquals(1, parsedOffsets.size());
        TopicPartition tp = parsedOffsets.keySet().iterator().next();
        assertEquals(10, tp.partition());
    }

    @Test
    public void testStringOffsetValue() {
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = createPartitionOffsetMap("topic", "10", "100");
        Map<TopicPartition, Long> parsedOffsets = SinkUtils.validateAndParseSinkConnectorOffsets(partitionOffsets);
        assertEquals(1, parsedOffsets.size());
        Long offsetValue = parsedOffsets.values().iterator().next();
        assertEquals(100L, offsetValue.longValue());
    }

    @Test
    public void testIntegerOffsetValue() {
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = createPartitionOffsetMap("topic", "10", 100);
        Map<TopicPartition, Long> parsedOffsets = SinkUtils.validateAndParseSinkConnectorOffsets(partitionOffsets);
        assertEquals(1, parsedOffsets.size());
        Long offsetValue = parsedOffsets.values().iterator().next();
        assertEquals(100L, offsetValue.longValue());
    }

    @Test
    public void testNullOffset() {
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = createPartitionOffsetMap("topic", "10", null);
        Map<TopicPartition, Long> parsedOffsets = SinkUtils.validateAndParseSinkConnectorOffsets(partitionOffsets);
        assertEquals(1, parsedOffsets.size());
        assertNull(parsedOffsets.values().iterator().next());
    }

    @Test
    public void testNullPartition() {
        Map<String, Object> partition = null;
        Map<String, Object> offset = new HashMap<>();
        offset.put(ConnectorOffset.KAFKA_OFFSET_KEY, 100);
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        partitionOffsets.put(partition, offset);
        ConnectException e = assertThrows(ConnectException.class, () -> SinkUtils.validateAndParseSinkConnectorOffsets(partitionOffsets));
        assertThat(e.getMessage(), containsString("Partitions cannot have null values"));
    }

    private Map<Map<String, ?>, Map<String, ?>> createPartitionOffsetMap(String topic, Object partition, Object offset) {
        Map<String, Object> partitionMap = new HashMap<>();
        partitionMap.put(ConnectorOffset.KAFKA_TOPIC_KEY, topic);
        partitionMap.put(ConnectorOffset.KAFKA_PARTITION_KEY, partition);
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put(ConnectorOffset.KAFKA_OFFSET_KEY, offset);
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        partitionOffsets.put(partitionMap, offsetMap);
        return partitionOffsets;
    }
}
