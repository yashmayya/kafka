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

import java.util.HashMap;
import java.util.Map;

public final class SinkUtils {

    private SinkUtils() {}

    public static String consumerGroupId(String connector) {
        return "connect-" + connector;
    }

    /**
     * Validate that the partitions (keys in the {@code partitionOffsets} map) look like:
     * <pre>
     *     {
     *       "kafka_topic": "topic"
     *       "kafka_partition": 3
     *     }
     * </pre>
     *
     * and that the offsets (values in the {@code partitionOffsets} map) look like
     * <pre>
     *     {
     *       "kafka_offset": 1000
     *     }
     * </pre>
     *
     * This method then parses them into a mapping from {@link TopicPartition}s to their corresponding {@link Long}
     * valued offsets.
     *
     * @param partitionOffsets the partitions to offset map that needs to be validated and parsed.
     * @return the parsed mapping from {@link TopicPartition} to its corresponding {@link Long} valued offset.
     *
     * @throws ConnectException if the provided offsets aren't in the expected format
     */
    public static Map<TopicPartition, Long> validateAndParseSinkConnectorOffsets(Map<Map<String, ?>, Map<String, ?>> partitionOffsets) {
        Map<TopicPartition, Long> parsedOffsetMap = new HashMap<>();

        for (Map.Entry<Map<String, ?>, Map<String, ?>> partitionOffset : partitionOffsets.entrySet()) {
            Map<String, ?> partitionMap = partitionOffset.getKey();
            if (partitionMap == null) {
                throw new ConnectException("Partitions cannot have null values");
            }
            if (!partitionMap.containsKey(ConnectorOffset.KAFKA_TOPIC_KEY) || !partitionMap.containsKey(ConnectorOffset.KAFKA_PARTITION_KEY)) {
                throw new ConnectException(String.format("The partition for sink connector offsets must contain the keys '%s' and '%s'",
                        ConnectorOffset.KAFKA_TOPIC_KEY, ConnectorOffset.KAFKA_PARTITION_KEY));
            }
            String topic = String.valueOf(partitionMap.get(ConnectorOffset.KAFKA_TOPIC_KEY));
            Integer partition;
            try {
                partition = (Integer) partitionMap.get(ConnectorOffset.KAFKA_PARTITION_KEY);
            } catch (ClassCastException c) {
                // this is because both "10" and 10 should be accepted as valid partition values in the REST API's
                // JSON request payload. If the following line throws an exception, we should propagate it since it's
                // indicative of a badly formatted value.
                try {
                    partition = Integer.parseInt(String.valueOf(partitionMap.get(ConnectorOffset.KAFKA_PARTITION_KEY)));
                } catch (Exception e) {
                    throw new ConnectException("Failed to parse the following Kafka partition value in the provided offsets: '" +
                            partitionMap.get(ConnectorOffset.KAFKA_PARTITION_KEY) + "'. Partition values for sink connectors need " +
                            "to be integers.", e);
                }
            }
            TopicPartition tp = new TopicPartition(topic, partition);

            Map<String, ?> offsetMap = partitionOffset.getValue();

            if (offsetMap == null) {
                // represents an offset reset
                parsedOffsetMap.put(tp, null);
            } else {
                if (!offsetMap.containsKey(ConnectorOffset.KAFKA_OFFSET_KEY)) {
                    throw new ConnectException(String.format("The offset for sink connectors should either be null or contain " +
                            "the key '%s'", ConnectorOffset.KAFKA_OFFSET_KEY));
                }
                Long offset;
                try {
                    offset = (Long) offsetMap.get(ConnectorOffset.KAFKA_OFFSET_KEY);
                } catch (ClassCastException c) {
                    // this is because both "1000" and 1000 should be accepted as valid offset values in the REST API's
                    // JSON request payload. If the following line throws an exception, we should propagate it since
                    // it's indicative of a badly formatted value.
                    try {
                        offset = Long.parseLong(String.valueOf(offsetMap.get(ConnectorOffset.KAFKA_OFFSET_KEY)));
                    } catch (Exception e) {
                        throw new ConnectException("Failed to parse the following Kafka offset value in the provided offsets: '" +
                                offsetMap.get(ConnectorOffset.KAFKA_OFFSET_KEY) + "'. Offset values for sink connectors need " +
                                "to be integers.", e);
                    }
                }
                parsedOffsetMap.put(tp, offset);
            }
        }

        return parsedOffsetMap;
    }
}
