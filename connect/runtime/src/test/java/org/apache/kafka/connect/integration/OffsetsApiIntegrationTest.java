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
package org.apache.kafka.connect.integration;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SourceConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffset;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Integration tests for Kafka Connect's connector offset management REST APIs
 */
@Category(IntegrationTest.class)
public class OffsetsApiIntegrationTest {

    private static final String CONNECTOR_NAME = "test-connector";
    private static final String TOPIC = "test-topic";
    private static final Integer NUM_TASKS = 2;
    private static final long OFFSET_COMMIT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
    private static final int NUM_WORKERS = 3;
    private EmbeddedConnectCluster connect;

    @Before
    public void setup() {
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(OFFSET_COMMIT_INTERVAL_MS));

        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(NUM_WORKERS)
                .workerProps(workerProps)
                .build();
        connect.start();
    }

    @After
    public void tearDown() {
        connect.stop();
    }

    @Test
    public void testAlterNonExistentConnectorOffsets() throws Exception {
        ConnectRestException e = assertThrows(ConnectRestException.class,
                () -> connect.alterConnectorOffsets("non-existent-connector", new ConnectorOffsets(Collections.singletonList(
                        new ConnectorOffset(Collections.emptyMap(), Collections.emptyMap())))));
        assertEquals(404, e.errorCode());
    }

    @Test
    public void testAlterResetsNonStoppedConnector() throws Exception {
        // Create source connector
        connect.configureConnector(CONNECTOR_NAME, baseSourceConnectorConfigs());
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

        List<ConnectorOffset> offsets = new ArrayList<>();
        // The MonitorableSourceConnector has a source partition per task
        for (int i = 0; i < NUM_TASKS; i++) {
            offsets.add(
                    new ConnectorOffset(Collections.singletonMap("task.id", CONNECTOR_NAME + "-" + i),
                            Collections.singletonMap("saved", 5))
            );
        }

        // Try altering offsets for a running connector
        ConnectRestException e = assertThrows(ConnectRestException.class,
                () -> connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsets)));
        assertEquals(400, e.errorCode());

        connect.pauseConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksArePaused(
                CONNECTOR_NAME,
                NUM_TASKS,
                "Connector did not pause in time"
        );

        // Try altering offsets for a paused (not stopped) connector
        e = assertThrows(ConnectRestException.class,
                () -> connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsets)));
        assertEquals(400, e.errorCode());
    }

    @Test
    public void testAlterSinkConnectorOffsets() throws Exception {
        alterAndVerifySinkConnectorOffsets(baseSinkConnectorConfigs(), connect.kafka());
    }

    @Test
    public void testAlterSinkConnectorOffsetsOverriddenConsumerGroupId() throws Exception {
        Map<String, String> connectorConfigs = baseSinkConnectorConfigs();
        connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + CommonClientConfigs.GROUP_ID_CONFIG,
                "overridden-group-id");
        alterAndVerifySinkConnectorOffsets(connectorConfigs, connect.kafka());
    }

    @Test
    public void testAlterSinkConnectorOffsetsDifferentKafkaClusterTargeted() throws Exception {
        EmbeddedKafkaCluster kafkaCluster = new EmbeddedKafkaCluster(1, new Properties());
        kafkaCluster.start();

        Map<String, String> connectorConfigs = baseSinkConnectorConfigs();
        connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                kafkaCluster.bootstrapServers());
        connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                kafkaCluster.bootstrapServers());

        alterAndVerifySinkConnectorOffsets(connectorConfigs, kafkaCluster);

        kafkaCluster.stop();
    }

    private void alterAndVerifySinkConnectorOffsets(Map<String, String> connectorConfigs, EmbeddedKafkaCluster kafkaCluster) throws Exception {
        kafkaCluster.createTopic(TOPIC, 3);

        // Produce 10 messages to each partition
        for (int partition = 0; partition < 3; partition++) {
            for (int message = 0; message < 10; message++) {
                kafkaCluster.produce(TOPIC, partition, "key", "value");
            }
        }
        // Create sink connector
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

//        TestUtils.waitForCondition(() -> {
//            ConnectorOffsets offsets = connect.connectorOffsets(CONNECTOR_NAME);
//            // There should be 3 topic partitions
//            if (offsets.offsets().size() != 3) {
//                return false;
//            }
//            for (ConnectorOffset offset: offsets.offsets()) {
//                assertEquals("test-topic", offset.partition().get(ConnectorOffset.KAFKA_TOPIC_KEY));
//                if ((Integer) offset.offset().get(ConnectorOffset.KAFKA_OFFSET_KEY) != 10) {
//                    return false;
//                }
//            }
//            return true;
//        }, "Sink connector consumer group offsets should catch up to the topic end offsets");

        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(
                CONNECTOR_NAME,
                "Connector did not stop in time"
        );

        List<ConnectorOffset> offsets = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Map<String, Object> partition = new HashMap<>();
            partition.put(ConnectorOffset.KAFKA_TOPIC_KEY, TOPIC);
            partition.put(ConnectorOffset.KAFKA_PARTITION_KEY, i);
            offsets.add(new ConnectorOffset(partition, Collections.singletonMap(ConnectorOffset.KAFKA_OFFSET_KEY, 5)));
        }

        String response = connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsets));
        assertEquals("The Connect framework managed offsets for this connector have been altered successfully. However, if this " +
                "connector manages offsets externally, they will need to be manually altered in the system that the connector uses.", response);

//        TestUtils.waitForCondition(() -> {
//            ConnectorOffsets offsets = connect.connectorOffsets(CONNECTOR_NAME);
//            // There should be 3 topic partitions
//            if (offsets.offsets().size() != 3) {
//                return false;
//            }
//            for (ConnectorOffset offset: offsets.offsets()) {
//                assertEquals("test-topic", offset.partition().get(ConnectorOffset.KAFKA_TOPIC_KEY));
//                if ((Integer) offset.offset().get(ConnectorOffset.KAFKA_OFFSET_KEY) != 5) {
//                    return false;
//                }
//            }
//            return true;
//        }, "Sink connector consumer group offsets should reflect the altered offsets");

        // Alter offsets again while connector is in stopped state
        offsets = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Map<String, Object> partition = new HashMap<>();
            partition.put(ConnectorOffset.KAFKA_TOPIC_KEY, TOPIC);
            partition.put(ConnectorOffset.KAFKA_PARTITION_KEY, i);
            offsets.add(new ConnectorOffset(partition, Collections.singletonMap(ConnectorOffset.KAFKA_OFFSET_KEY, 3)));
        }

        response = connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsets));
        assertEquals("The Connect framework managed offsets for this connector have been altered successfully. However, if this " +
                "connector manages offsets externally, they will need to be manually altered in the system that the connector uses.", response);

//        TestUtils.waitForCondition(() -> {
//            ConnectorOffsets offsets = connect.connectorOffsets(CONNECTOR_NAME);
//            // There should be 3 topic partitions
//            if (offsets.offsets().size() != 3) {
//                return false;
//            }
//            for (ConnectorOffset offset: offsets.offsets()) {
//                assertEquals("test-topic", offset.partition().get(ConnectorOffset.KAFKA_TOPIC_KEY));
//                if ((Integer) offset.offset().get(ConnectorOffset.KAFKA_OFFSET_KEY) != 3) {
//                    return false;
//                }
//            }
//            return true;
//        }, "Sink connector consumer group offsets should reflect the altered offsets");

        // Update the connector's configs; this time expect SinkConnector::alterOffsets to return true
        connectorConfigs.put(MonitorableSinkConnector.ALTER_OFFSETS_RESULT, "true");
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);

        response = connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsets));
        assertEquals("The offsets for this connector have been altered successfully", response);
    }

    @Test
    public void testAlterSourceConnectorOffsets() throws Exception {
        alterAndVerifySourceConnectorOffsets(baseSourceConnectorConfigs());
    }

    @Test
    public void testAlterSourceConnectorOffsetsCustomOffsetsTopic() throws Exception {
        Map<String, String> connectorConfigs = baseSourceConnectorConfigs();
        connectorConfigs.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "custom-offsets-topic");
        alterAndVerifySourceConnectorOffsets(connectorConfigs);
    }

    @Test
    public void testAlterSourceConnectorOffsetsDifferentKafkaClusterTargeted() throws Exception {
        EmbeddedKafkaCluster kafkaCluster = new EmbeddedKafkaCluster(1, new Properties());
        kafkaCluster.start();

        Map<String, String> connectorConfigs = baseSourceConnectorConfigs();
        connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                kafkaCluster.bootstrapServers());
        connectorConfigs.put(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                kafkaCluster.bootstrapServers());

        alterAndVerifySourceConnectorOffsets(baseSourceConnectorConfigs());
    }

    public void alterAndVerifySourceConnectorOffsets(Map<String, String> connectorConfigs) throws Exception {
        // Create source connector
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS,
                "Connector tasks did not start in time.");

//        TestUtils.waitForCondition(() -> {
//            ConnectorOffsets offsets = connect.connectorOffsets(CONNECTOR_NAME);
//            // The MonitorableSourceConnector has a source partition per task
//            if (offsets.offsets().size() != NUM_TASKS) {
//                return false;
//            }
//            for (ConnectorOffset offset : offsets.offsets()) {
//                assertTrue(((String) offset.partition().get("task.id")).startsWith(CONNECTOR_NAME));
//                if ((Integer) offset.offset().get("saved") != 10) {
//                    return false;
//                }
//            }
//            return true;
//        }, "Source connector offsets should reflect the expected number of records produced");

        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(
                CONNECTOR_NAME,
                "Connector did not stop in time"
        );

        List<ConnectorOffset> offsets = new ArrayList<>();
        // The MonitorableSourceConnector has a source partition per task
        for (int i = 0; i < NUM_TASKS; i++) {
            offsets.add(
                    new ConnectorOffset(Collections.singletonMap("task.id", CONNECTOR_NAME + "-" + i),
                            Collections.singletonMap("saved", 5))
            );
        }

        String response = connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsets));
        assertEquals("The Connect framework managed offsets for this connector have been altered successfully. However, if this " +
                "connector manages offsets externally, they will need to be manually altered in the system that the connector uses.", response);

//        TestUtils.waitForCondition(() -> {
//            ConnectorOffsets offsets = connect.connectorOffsets(CONNECTOR_NAME);
//            // The MonitorableSourceConnector has a source partition per task
//            if (offsets.offsets().size() != NUM_TASKS) {
//                return false;
//            }
//            for (ConnectorOffset offset : offsets.offsets()) {
//                assertTrue(((String) offset.partition().get("task.id")).startsWith(CONNECTOR_NAME));
//                if ((Integer) offset.offset().get("saved") != 5) {
//                    return false;
//                }
//            }
//            return true;
//        }, "Source connector offsets should reflect the altered offsets");

        // Alter offsets again while connector is in stopped state
        offsets = new ArrayList<>();
        // The MonitorableSourceConnector has a source partition per task
        for (int i = 0; i < NUM_TASKS; i++) {
            offsets.add(
                    new ConnectorOffset(Collections.singletonMap("task.id", CONNECTOR_NAME + "-" + i),
                            Collections.singletonMap("saved", 7))
            );
        }

        response = connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsets));
        assertEquals("The Connect framework managed offsets for this connector have been altered successfully. However, if this " +
                "connector manages offsets externally, they will need to be manually altered in the system that the connector uses.", response);

//        TestUtils.waitForCondition(() -> {
//            ConnectorOffsets offsets = connect.connectorOffsets(CONNECTOR_NAME);
//            // The MonitorableSourceConnector has a source partition per task
//            if (offsets.offsets().size() != NUM_TASKS) {
//                return false;
//            }
//            for (ConnectorOffset offset : offsets.offsets()) {
//                assertTrue(((String) offset.partition().get("task.id")).startsWith(CONNECTOR_NAME));
//                if ((Integer) offset.offset().get("saved") != 5) {
//                    return false;
//                }
//            }
//            return true;
//        }, "Source connector offsets should reflect the altered offsets");

        // Update the connector's configs; this time expect SinkConnector::alterOffsets to return true
        connectorConfigs.put(MonitorableSourceConnector.ALTER_OFFSETS_RESULT, "true");
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);

        response = connect.alterConnectorOffsets(CONNECTOR_NAME, new ConnectorOffsets(offsets));
        assertEquals("The offsets for this connector have been altered successfully", response);
    }

    // TODO: Rebase once https://github.com/apache/kafka/pull/13434 is merged and use GET /offsets API here. Refactor get and verify offsets with waitForCondition into separate method

    private Map<String, String> baseSinkConnectorConfigs() {
        Map<String, String> configs = new HashMap<>();
        configs.put(CONNECTOR_CLASS_CONFIG, MonitorableSinkConnector.class.getSimpleName());
        configs.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        configs.put(TOPICS_CONFIG, TOPIC);
        configs.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        configs.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return configs;
    }

    private Map<String, String> baseSourceConnectorConfigs() {
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPIC_CONFIG, TOPIC);
        props.put(MonitorableSourceConnector.MESSAGES_PER_POLL_CONFIG, "3");
        props.put(MonitorableSourceConnector.MAX_MESSAGES_PRODUCED_CONFIG, "10");
        props.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, "1");
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, "1");
        return props;
    }
}