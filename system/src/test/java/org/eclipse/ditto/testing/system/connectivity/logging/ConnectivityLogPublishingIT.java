/*
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.ditto.testing.system.connectivity.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.testing.common.TestConstants.API_V_2;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.LogCategory;
import org.eclipse.ditto.connectivity.model.LogLevel;
import org.eclipse.ditto.connectivity.model.LogType;
import org.eclipse.ditto.connectivity.model.SshTunnel;
import org.eclipse.ditto.connectivity.model.UserPasswordCredentials;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonFieldDefinition;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.policies.model.SubjectType;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ServiceEnvironment;
import org.eclipse.ditto.testing.common.Solution;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.ThingsSubjectIssuer;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.common.matcher.RestMatcherConfigurer;
import org.eclipse.ditto.testing.common.matcher.RestMatcherFactory;
import org.eclipse.ditto.testing.system.connectivity.ConnectionModelFactory;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityFactory;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityTestConfig;
import org.eclipse.ditto.testing.system.connectivity.kafka.KafkaConnectivityWorker;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.events.AttributeCreated;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.response.Response;

/**
 * System test verifying that connection log publishing works by consuming published connection logs via a Kafka topic.
 * The "Fluent Bit" docker container in the system tests is configured to forward the logs to a Kafka topic
 * {@link #KAFKA_TOPIC_FLUENTBIT_LOG_OUTPUT},
 */
@RunIf(DockerEnvironment.class)
public class ConnectivityLogPublishingIT extends IntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectivityLogPublishingIT.class);

    private static final ConnectivityTestConfig CONFIG = ConnectivityTestConfig.getInstance();

    private static final SshTunnel DISABLED_SSH_TUNNEL = ConnectivityModelFactory.newSshTunnel(false,
            UserPasswordCredentials.newInstance("dummy", "dummy"), "ssh://localhost:22");

    private static final String KAFKA_TEST_CLIENT_ID = ConnectivityLogPublishingIT.class.getSimpleName();
    private static final String KAFKA_TEST_HOSTNAME = CONFIG.getKafkaHostname();
    private static final String KAFKA_TEST_USERNAME = CONFIG.getKafkaUsername();
    private static final String KAFKA_TEST_PASSWORD = CONFIG.getKafkaPassword();
    private static final int KAFKA_TEST_PORT = CONFIG.getKafkaPort();

    private static final String KAFKA_SERVICE_HOSTNAME = CONFIG.getKafkaHostname();
    private static final int KAFKA_SERVICE_PORT = CONFIG.getKafkaPort();

    private static final String KAFKA_TOPIC_FLUENTBIT_LOG_OUTPUT = "fluentbit-log-output";
    private static final Duration KAFKA_POLL_TIMEOUT = Duration.ofSeconds(5);

    private static final String RESPONSES_ADDRESS = "_responses";

    private static TestingContext testingContext;
    private static String preAuthenticatedConnectionSubject;
    private static ThingId testThingId;

    private String connectionName;
    private ConnectionId createdConnectionId;
    private TopicPartition topicPartition;
    private KafkaConsumer<String, byte[]> kafkaLogOutputConsumer;
    private KafkaProducer<String, byte[]> kafkaProducer;

    private static String getConnectionUri(final boolean tunnel, final boolean basicAuth) {
        // tunneling not implemented for kafka
        return "tcp://" + KAFKA_TEST_USERNAME + ":" + KAFKA_TEST_PASSWORD +
                "@" + KAFKA_SERVICE_HOSTNAME + ":" + KAFKA_SERVICE_PORT;
    }

    private static Map<String, String> getSpecificConfig() {
        return Map.of("bootstrapServers", KAFKA_SERVICE_HOSTNAME + ":" + KAFKA_SERVICE_PORT,
                "consumerOffsetReset", "earliest");
    }

    private static String defaultTargetAddress(final String suffix) {
        return "test-target-" + suffix;
    }

    private static String defaultSourceAddress(final String suffix) {
        return "test-source-" + suffix;
    }

    @BeforeClass
    public static void createSolution() {
        final Solution solution = ServiceEnvironment.createSolutionWithRandomUsernameRandomNamespace();
        testingContext = TestingContext.withGeneratedMockClient(solution, TEST_CONFIG);

        preAuthenticatedConnectionSubject = SubjectIssuer.INTEGRATION +
                ":" + testingContext.getSolution().getUsername() + ":" + TestingContext.DEFAULT_SCOPE;

        testThingId = ThingId.of(
                idGenerator(testingContext.getSolution().getDefaultNamespace()).withRandomName()
        );

        final Thing thing = Thing.newBuilder()
                .setId(testThingId)
                .build();

        final Policy policy = Policy.newBuilder(PolicyId.of(testThingId))
                .setSubjectsFor("Default", Subjects.newInstance(
                        Subject.newInstance(preAuthenticatedConnectionSubject, SubjectType.GENERATED),
                        Subject.newInstance(ThingsSubjectIssuer.DITTO, testingContext.getSolution().getUsername())))
                .setGrantedPermissionsFor("Default", PoliciesResourceType.thingResource("/"), "READ", "WRITE")
                .setGrantedPermissionsFor("Default", PoliciesResourceType.policyResource("/"), "READ", "WRITE")
                .setGrantedPermissionsFor("Default", PoliciesResourceType.messageResource("/"), "READ", "WRITE")
                .build();

        putThingWithPolicy(API_V_2, thing, policy, JsonSchemaVersion.V_2)
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        configureRestMatchers(testingContext.getOAuthClient());
    }

    private static void configureRestMatchers(final AuthClient authClient) {
        restMatcherConfigurer = RestMatcherConfigurer.withJwt(authClient.getAccessToken());
        restMatcherFactory = new RestMatcherFactory(restMatcherConfigurer);
    }

    @Before
    public void setupTest() throws ExecutionException, InterruptedException, TimeoutException {
        connectionName = "Kafka-" + UUID.randomUUID();

        LOGGER.info("Preparing Kafka at {}:{}", KAFKA_TEST_HOSTNAME, KAFKA_TEST_PORT);
        KafkaConnectivityWorker.setupKafka(KAFKA_TEST_CLIENT_ID, KAFKA_TEST_HOSTNAME, KAFKA_TEST_PORT,
                KAFKA_TEST_USERNAME, KAFKA_TEST_PASSWORD,
                List.of(defaultTargetAddress(connectionName), defaultSourceAddress(connectionName),
                        KAFKA_TOPIC_FLUENTBIT_LOG_OUTPUT), LOGGER);

        kafkaLogOutputConsumer = KafkaConnectivityWorker.setupKafkaConsumer(KAFKA_TEST_HOSTNAME, KAFKA_TEST_PORT,
                KAFKA_TEST_USERNAME, KAFKA_TEST_PASSWORD, connectionName);
        topicPartition = new TopicPartition(KAFKA_TOPIC_FLUENTBIT_LOG_OUTPUT, 0);
        kafkaLogOutputConsumer.assign(List.of(topicPartition));
        kafkaLogOutputConsumerSeekToEnd();

        kafkaProducer = KafkaConnectivityWorker.setupKafkaProducer(KAFKA_TEST_HOSTNAME, KAFKA_TEST_PORT,
                KAFKA_TEST_USERNAME, KAFKA_TEST_PASSWORD, connectionName);

        final ConnectivityFactory connectivityFactory =
                ConnectivityFactory.of(ConnectivityLogPublishingIT.class.getSimpleName(),
                        new ConnectionModelFactory((username, suffix) -> preAuthenticatedConnectionSubject),
                        ConnectionType.KAFKA,
                        ConnectivityLogPublishingIT::getConnectionUri,
                        ConnectivityLogPublishingIT::getSpecificConfig,
                        ConnectivityLogPublishingIT::defaultTargetAddress,
                        ConnectivityLogPublishingIT::defaultSourceAddress,
                        connectionId -> null,
                        () -> DISABLED_SSH_TUNNEL
                ).withSolutionSupplier(() -> testingContext.getSolution());

        final Response response = connectivityFactory.setupSingleConnection(connectionName)
                .get(50, TimeUnit.SECONDS);
        final Connection connection = ConnectivityModelFactory.connectionFromJson(
                JsonFactory.newObject(response.getBody().asString()));
        createdConnectionId = connection.getId();
    }

    private void kafkaLogOutputConsumerSeekToEnd() {
        kafkaLogOutputConsumer.seekToEnd(List.of(topicPartition));
        kafkaLogOutputConsumer.position(topicPartition);
    }

    @After
    public void cleanupAfterTest() {
        kafkaLogOutputConsumer.close();
        if (null != createdConnectionId) {
            connectionsClient().deleteConnection(createdConnectionId)
                    .withDevopsAuth()
                    .expectingHttpStatus(HttpStatus.NO_CONTENT)
                    .fire();
        }
    }

    @Test
    public void createKafkaConnectionAndCheckIfConnectionLogIsReceived() {
        final List<TestLogEntry> consumedRecords = pollLogEntries().stream()
                .filter(logEntry -> logEntry.connectionId.equals(createdConnectionId))
                .collect(Collectors.toList());

        assertThat(consumedRecords)
                .allMatch(logEntry ->
                        logEntry.connectionId.equals(createdConnectionId)
                )
                .anyMatch(ensureLogEntry(LogLevel.SUCCESS, LogCategory.CONNECTION, LogType.OTHER,
                        null, "Connection successful"));
    }

    @Test
    public void createKafkaConnectionAndCheckIfSourceLogsArrive() {
        final String topic = defaultSourceAddress(connectionName);
        final String correlationId = UUID.randomUUID().toString();
        final ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(topic, null, connectionName, "Hello!".getBytes(StandardCharsets.UTF_8),
                        List.of(
                                new RecordHeader("response-required", "false".getBytes(StandardCharsets.UTF_8)),
                                new RecordHeader("correlation-id", correlationId.getBytes(StandardCharsets.UTF_8))
                        )
                );

        try {
            LOGGER.info("Sending record to topic {}: {}", topic, new String(record.value(), StandardCharsets.UTF_8));
            final RecordMetadata recordMetadata = kafkaProducer.send(record).get(100, TimeUnit.MILLISECONDS);
            LOGGER.debug("Record Metadata: {}", recordMetadata.toString());
        } catch (final Exception e) {
            LOGGER.error("Failed to send message to topic {}: {}", topic, new String(record.value(), StandardCharsets.UTF_8));
        }

        final List<TestLogEntry> consumedRecords = pollLogEntries().stream()
                .filter(logEntry -> logEntry.connectionId.equals(createdConnectionId))
                .filter(logEntry -> !logEntry.category.equals(LogCategory.CONNECTION))
                .filter(logEntry -> correlationId.equals(logEntry.correlationId))
                .collect(Collectors.toList());

        assertThat(consumedRecords)
                .anyMatch(ensureLogEntry(LogLevel.SUCCESS, LogCategory.SOURCE, LogType.CONSUMED,
                        topic, "Message was consumed"))
                .anyMatch(ensureLogEntry(LogLevel.SUCCESS, LogCategory.SOURCE, LogType.ACKNOWLEDGED,
                        topic, "Message was acknowledged"))
                .anyMatch(ensureLogEntry(LogLevel.FAILURE, LogCategory.SOURCE, LogType.MAPPED,
                        topic, "Got exception"))
                .anyMatch(ensureLogEntry(LogLevel.FAILURE, LogCategory.RESPONSE, LogType.DISPATCHED,
                        RESPONSES_ADDRESS, "Failure while message was dispatched:"))
                .anyMatch(ensureLogEntry(LogLevel.SUCCESS, LogCategory.RESPONSE, LogType.MAPPED,
                        RESPONSES_ADDRESS, "Mapped outgoing signal with mapper"))
                .anyMatch(ensureLogEntry(LogLevel.SUCCESS, LogCategory.RESPONSE, LogType.DROPPED,
                        RESPONSES_ADDRESS, "Signal dropped, target address unresolved"));
    }

    @Test
    public void createKafkaConnectionAndCheckIfTargetLogsArrive() {
        final String targetTopic = defaultTargetAddress(connectionName);

        final KafkaConsumer<String, byte[]> eventConsumer =
                KafkaConnectivityWorker.setupKafkaConsumer(KAFKA_TEST_HOSTNAME, KAFKA_TEST_PORT,
                        KAFKA_TEST_USERNAME, KAFKA_TEST_PASSWORD, connectionName + "-eventConsumer");
        eventConsumer.subscribe(List.of(targetTopic));

        final String correlationId = UUID.randomUUID().toString();

        putAttribute(API_V_2, testThingId, "new-attr", "true")
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .withHeader("correlation-id", correlationId)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final ConsumerRecords<String, byte[]> eventRecords = eventConsumer.poll(KAFKA_POLL_TIMEOUT);
        final List<Signal<?>> parsedEventRecords = StreamSupport.stream(eventRecords.spliterator(), false)
                .filter(cr -> targetTopic.equals(cr.topic()))
                .map(ConsumerRecord::value)
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .map(JsonFactory::readFrom)
                .map(JsonValue::asObject)
                .map(json -> DittoProtocolAdapter.newInstance().fromAdaptable(ProtocolFactory.jsonifiableAdaptableFromJson(json)))
                .collect(Collectors.toList());

        assertThat(parsedEventRecords)
                .anyMatch(signal ->
                        signal instanceof AttributeCreated &&
                                ((AttributeCreated) signal).getEntityId().equals(testThingId) &&
                                ((AttributeCreated) signal).getAttributePointer().equals(JsonPointer.of("new-attr")) &&
                                ((AttributeCreated) signal).getAttributeValue().equals(JsonValue.of(true))
                );

        final List<TestLogEntry> consumedRecords = pollLogEntries().stream()
                .filter(logEntry -> logEntry.connectionId.equals(createdConnectionId))
                .filter(logEntry -> !logEntry.category.equals(LogCategory.CONNECTION))
                .filter(logEntry -> correlationId.equals(logEntry.correlationId))
                .collect(Collectors.toList());

        assertThat(consumedRecords)
                .anyMatch(ensureLogEntry(LogLevel.SUCCESS, LogCategory.TARGET, LogType.DISPATCHED,
                        targetTopic, "Message was dispatched"))
                .anyMatch(ensureLogEntry(LogLevel.SUCCESS, LogCategory.TARGET, LogType.FILTERED,
                        targetTopic, "Message was filtered"))
                .anyMatch(ensureLogEntry(LogLevel.SUCCESS, LogCategory.TARGET, LogType.MAPPED,
                        targetTopic, "Mapped outgoing signal with mapper"))
                .anyMatch(ensureLogEntry(LogLevel.SUCCESS, LogCategory.TARGET, LogType.PUBLISHED,
                        targetTopic, "Message was published"));
    }

    private List<TestLogEntry> pollLogEntries() {
        final List<TestLogEntry> combinedLogRecords = new ArrayList<>();
        while (true) {
            final ConsumerRecords<String, byte[]> topicRecords = kafkaLogOutputConsumer.poll(KAFKA_POLL_TIMEOUT);
            // poll until no records received any longer:
            if (topicRecords.isEmpty()) {
                break;
            }

            final List<TestLogEntry> parsedLogRecords = StreamSupport.stream(topicRecords.spliterator(), false)
                    .filter(cr -> KAFKA_TOPIC_FLUENTBIT_LOG_OUTPUT.equals(cr.topic()))
                    .map(ConsumerRecord::value)
                    .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                    .map(TestLogEntry::fromJsonString)
                    .collect(Collectors.toList());
            kafkaLogOutputConsumer.commitSync();
            LOGGER.info("Polled <{}> records from Kafka topic <{}>:\n[\n\t{}\n]", parsedLogRecords.size(),
                    KAFKA_TOPIC_FLUENTBIT_LOG_OUTPUT, parsedLogRecords.stream()
                            .map(TestLogEntry::toString)
                            .collect(Collectors.joining("\n\t"))
            );
            combinedLogRecords.addAll(parsedLogRecords);
        }

        return combinedLogRecords.stream()
                .sorted(Comparator.comparing(l -> l.timestamp))
                .collect(Collectors.toList());
    }

    private static Predicate<TestLogEntry> ensureLogEntry(final LogLevel level,
            final LogCategory category,
            final LogType type,
            @Nullable final String address,
            final String message) {
        return logEntry ->
                logEntry.level.equals(level) &&
                        logEntry.category.equals(category) &&
                        logEntry.type.equals(type) &&
                        (null == address || address.equals(logEntry.address)) &&
                        logEntry.message.startsWith(message);
    }

    private final static class TestLogEntry {

        public static final JsonFieldDefinition<String> TIMESTAMP =
                JsonFactory.newStringFieldDefinition("@timestamp");

        public static final JsonFieldDefinition<String> CONNECTION_ID =
                JsonFactory.newStringFieldDefinition("connectionId");

        public static final JsonFieldDefinition<String> LEVEL =
                JsonFactory.newStringFieldDefinition("level");

        public static final JsonFieldDefinition<String> CATEGORY =
                JsonFactory.newStringFieldDefinition("category");

        public static final JsonFieldDefinition<String> TYPE =
                JsonFactory.newStringFieldDefinition("type");

        public static final JsonFieldDefinition<String> ADDRESS =
                JsonFactory.newStringFieldDefinition("address");

        public static final JsonFieldDefinition<String> CORRELATION_ID =
                JsonFactory.newStringFieldDefinition("correlationId");

        public static final JsonFieldDefinition<String> MESSAGE =
                JsonFactory.newStringFieldDefinition("message");

        public static final JsonFieldDefinition<String> INSTANCE_ID =
                JsonFactory.newStringFieldDefinition("instanceId");

        private final Instant timestamp;
        private final ConnectionId connectionId;
        private final LogLevel level;
        private final LogCategory category;
        private final LogType type;
        @Nullable private final String address;
        @Nullable private final String correlationId;
        private final String message;
        private final String instanceId;

        private TestLogEntry(final Instant timestamp,
                final ConnectionId connectionId,
                final LogLevel level,
                final LogCategory category,
                final LogType type,
                @Nullable final String address,
                @Nullable final String correlationId,
                final String message,
                final String instanceId) {
            this.timestamp = timestamp;
            this.connectionId = connectionId;
            this.level = level;
            this.category = category;
            this.type = type;
            this.address = address;
            this.correlationId = correlationId;
            this.message = message;
            this.instanceId = instanceId;
        }

        static TestLogEntry fromJsonString(final String jsonString) {
            return fromJson(JsonFactory.readFrom(jsonString).asObject());
        }

        static TestLogEntry fromJson(final JsonObject jsonObject) {
            final Instant timestamp = Instant.parse(jsonObject.getValueOrThrow(TIMESTAMP));
            final ConnectionId connectionId = ConnectionId.of(jsonObject.getValueOrThrow(CONNECTION_ID));
            final LogLevel level = LogLevel.forLevel(jsonObject.getValueOrThrow(LEVEL)).orElseThrow();
            final LogCategory category = LogCategory.forName(jsonObject.getValueOrThrow(CATEGORY)).orElseThrow();
            final LogType type = LogType.forType(jsonObject.getValueOrThrow(TYPE)).orElseThrow();
            final String address = jsonObject.getValue(ADDRESS)
                    .orElse(null);
            final String correlationId = jsonObject.getValue(CORRELATION_ID)
                    .filter(cId -> !cId.equals("<not-provided>"))
                    .orElse(null);
            final String message = jsonObject.getValueOrThrow(MESSAGE);
            final String instanceId = jsonObject.getValueOrThrow(INSTANCE_ID);

            return new TestLogEntry(timestamp, connectionId, level, category, type, address, correlationId, message,
                    instanceId);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TestLogEntry that = (TestLogEntry) o;
            return Objects.equals(timestamp, that.timestamp) &&
                    Objects.equals(connectionId, that.connectionId) && level == that.level &&
                    category == that.category && type == that.type && Objects.equals(address, that.address) &&
                    Objects.equals(correlationId, that.correlationId) &&
                    Objects.equals(message, that.message) &&
                    Objects.equals(instanceId, that.instanceId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, connectionId, level, category, type, address, correlationId, message,
                    instanceId);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + " [" +
                    "timestamp=" + timestamp +
                    ", connectionId=" + connectionId +
                    ", level=" + level +
                    ", category=" + category +
                    ", type=" + type +
                    ", address=" + address +
                    ", correlationId=" + correlationId +
                    ", message=" + message +
                    ", instanceId=" + instanceId +
                    "]";
        }
    }

}
