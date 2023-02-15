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
package org.eclipse.ditto.testing.system.connectivity.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION_HONO;
import static org.eclipse.ditto.testing.system.connectivity.ConnectivityConstants.TARGET_SUFFIX;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.Enforcement;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.testing.common.client.ConnectionsClient;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityITCommon;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityWorker;
import org.eclipse.ditto.testing.system.connectivity.ConnectionCategory;
import org.eclipse.ditto.testing.system.connectivity.Connections;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * System tests for Connectivity of Hono connections.
 * Failsafe won't run this directly because it does not end in *IT.java.
 */
@RunIf(DockerEnvironment.class)
@NotThreadSafe
public final class HonoConnectivityIT extends
        AbstractConnectivityITCommon<BlockingQueue<ConsumerRecord<String, byte[]>>, ConsumerRecord<String, byte[]>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HonoConnectivityIT.class);

    private static final String TARGET_ADDRESS_ALIAS = "command";
    private static final String SOURCE_ADDRESS_ALIAS = "event";
    private static final String REPLY_TARGET_ADDRESS_ALIAS = "command";

    private static final long KAFKA_POLL_TIMEOUT_MS = 600_000L; // practically infinite, poll forever
    private static final long WAIT_TIMEOUT_MS = 10_000L;
    // if after 10secs waiting time no message arrived, handle as "no message consumed"
    private static final ConnectionType CONNECTION_TYPE = ConnectionType.HONO;

    private static final Enforcement KAFKA_ENFORCEMENT =
            ConnectivityModelFactory.newEnforcement("{{ header:device_id }}", "{{ thing:id }}",
                    "{{ policy:id }}");

    private static final String KAFKA_TEST_HOSTNAME = CONFIG.getKafkaHostname();
    private static final int KAFKA_TEST_PORT = CONFIG.getKafkaPort();
    private static final String KAFKA_TEST_USERNAME = CONFIG.getKafkaUsername();
    private static final String KAFKA_TEST_PASSWORD = CONFIG.getKafkaPassword();

    @Rule
    public final TestWatcher watchman = new DefaultTestWatcher(LOGGER);

    @Rule
    public TestName testName = new TestName();

    private static String tenantId;
    private final KafkaConnectivityWorker kafkaConnectivityWorker;
    private KafkaConsumer<String, byte[]> kafkaConsumer;
    private int consumerCounter = 0;
    private int producerCounter = 0;

    public HonoConnectivityIT() {
        super(ConnectivityFactory.of(
                        "Hono",
                        connectionModelFactory,
                        CONNECTION_TYPE,
                        HonoConnectivityIT::getConnectionUri,
                        HonoConnectivityIT::getSpecificConfig,
                        (name) -> TARGET_ADDRESS_ALIAS,
                        (name) -> SOURCE_ADDRESS_ALIAS,
                        id -> KAFKA_ENFORCEMENT,
                        () -> SSH_TUNNEL_CONFIG)
                .withDefaultHeaderMapping(
                        Map.of(
                                "correlation-id", "{{ header:correlation-id }}",
                                "content-type", "{{ header:content-type }}"
                        )
                )
                .withReplyTargetAddress(connectionName -> REPLY_TARGET_ADDRESS_ALIAS));
        kafkaConnectivityWorker =
                new KafkaConnectivityWorker(LOGGER,
                        this::createKafkaConsumer,
                        this::createKafkaProducer,
                        HonoConnectivityIT::defaultTargetAddress,
                        HonoConnectivityIT::defaultSourceAddress,
                        HonoConnectivityIT::defaultReplyTargetAddress,
                        Duration.ofMillis(KAFKA_POLL_TIMEOUT_MS),
                        Duration.ofMillis(WAIT_TIMEOUT_MS));
    }

    protected AbstractConnectivityWorker<BlockingQueue<ConsumerRecord<String, byte[]>>, ConsumerRecord<String, byte[]>> getConnectivityWorker() {
        return kafkaConnectivityWorker;
    }

    private static String defaultTargetAddress(final String suffix) {
        return "hono." + TARGET_ADDRESS_ALIAS + "." + tenantId;
    }

    private static String defaultSourceAddress(final String suffix) {
        return "hono." + SOURCE_ADDRESS_ALIAS + "." + tenantId;
    }

    private static String defaultReplyTargetAddress(final String suffix) {
        return "hono." + REPLY_TARGET_ADDRESS_ALIAS + "." + tenantId;
    }

    @Before
    @Override
    public void setupConnectivity() throws Exception {
        super.setupConnectivity();
        tenantId = cf.connectionHonoName;

        LOGGER.info("Preparing Kafka at {}:{}", KAFKA_TEST_HOSTNAME, KAFKA_TEST_PORT);

        final String itestsName = cf.connectionNameWithAuthPlaceholderOnHEADER_ID + "_itests";
        final List<String> newTopics =
                Stream.concat(
                                Stream.of(itestsName),
                                connectionsWatcher.getConnections()
                                        .keySet().stream()
                                        .map(cf::getConnectionName)
                                        .flatMap(names -> Stream.of(
                                                defaultTargetAddress(names),
                                                defaultSourceAddress(names),
                                                defaultReplyTargetAddress(names))))
                        .collect(Collectors.toList());

        KafkaConnectivityWorker.setupKafka(getClass().getSimpleName() + "_" + testName + "_init",
                KAFKA_TEST_HOSTNAME, KAFKA_TEST_PORT, KAFKA_TEST_USERNAME, KAFKA_TEST_PASSWORD,
                newTopics, LOGGER);

        kafkaConsumer = createKafkaConsumer();
        kafkaConsumer.subscribe(newTopics);

        cf.setUpConnections(connectionsWatcher.getConnections());
    }

    private KafkaConsumer<String, byte[]> createKafkaConsumer() {
        return KafkaConnectivityWorker.setupKafkaConsumer(KAFKA_TEST_HOSTNAME, KAFKA_TEST_PORT,
                KAFKA_TEST_USERNAME, KAFKA_TEST_PASSWORD,
                testName.getMethodName() + ++consumerCounter);
    }

    private KafkaProducer<String, byte[]> createKafkaProducer() {
        return KafkaConnectivityWorker.setupKafkaProducer(KAFKA_TEST_HOSTNAME, KAFKA_TEST_PORT,
                KAFKA_TEST_USERNAME, KAFKA_TEST_PASSWORD,
                testName.getMethodName() + ++producerCounter);
    }

    @After
    public void closeConsumerAndDeleteTopics() throws Exception {
        LOGGER.info("Closing Kafka consumers.");
        final List<KafkaConsumer<?, ?>> stoppedConsumers = kafkaConnectivityWorker.stopPolling();
        Stream.concat(stoppedConsumers.stream(), Optional.ofNullable(kafkaConsumer).stream())
                .forEach(consumer -> {
                    try {
                        consumer.close(Duration.ofSeconds(5L));
                    } catch (final Exception e) {
                        LOGGER.error("Got error closing kafka consumer.", e);
                    }
                });
        cleanupConnections(testingContextWithRandomNs.getSolution().getUsername());
    }

    private static String getConnectionUri(final boolean tunnel, final boolean basicAuth) {
        return "";
    }

    private static Map<String, String> getSpecificConfig() {
        return Map.of();
    }

    @Override
    protected String targetAddressForTargetPlaceHolderSubstitution() {
        return String.format("%s%s", defaultTargetAddress(cf.connectionNameWithAuthPlaceholderOnHEADER_ID),
                TARGET_SUFFIX);
    }

    @Test
    @Connections(CONNECTION_HONO)
    public void testSpecificConfigAndSourceAndReplyTargetAddresses() {
        // create a thing with READ permission for the default solution
        final ThingId thingId = sendCreateThingAndEnsureResponseIsSentBack(cf.connectionHonoName,
                ThingBuilder.FromScratch::build);

        // send modify command to a thing attribute
        final JsonPointer attributeKey = JsonPointer.of("test2");
        final String attributeValue = "value2";
        sendSignal(cf.connectionHonoName, modifyAttribute(thingId, attributeKey, attributeValue));

        final int expectedArrivals = 2;
        // expect all messages are processed
        getThing(2, thingId)
                .withParam("fields", "_revision")
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .useAwaitility(Awaitility.await()
                        .pollInterval(Duration.ofSeconds(1))
                        .atMost(Duration.ofMinutes(1)))
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(satisfies(body -> {
                    final Long revision = JsonFactory.newObject(body).getValueOrThrow(Thing.JsonFields.REVISION);
                    assertThat(revision).isEqualTo(expectedArrivals);
                }))
                .fire();
    }

    @Test
    @Connections(ConnectionCategory.NONE)
    public void testRejectNonAliasSources() {
        testRejectNonAlias("badSource");
        testRejectNonAlias("badTarget");
    }

    public void testRejectNonAlias(final String parameter) {
        final var username = testingContextWithRandomNs.getSolution().getUsername();
        final String connectionName = UUID.randomUUID().toString();
        final Connection connection = connectionModelFactory.buildConnectionModelWithHeaderMapping(
                username,
                connectionName,
                ConnectionType.HONO,
                "tcp://" + KAFKA_TEST_HOSTNAME + ":9092",
                Map.of("bootstrapServers", KAFKA_TEST_HOSTNAME +":" + KAFKA_TEST_PORT),
                parameter.equals("badSource") ? "topic1" : "event",
                parameter.equals("badTarget") ? "topic2" : "command");

        final JsonObject connectionWithoutId = ConnectivityFactory.removeIdFromJson(connection.toJson());

        ConnectionsClient.getInstance()
                .postConnection(connectionWithoutId)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("connectivity:connection.configuration.invalid")
                .expectingBody(Matchers.containsString("is invalid. It should be "))
                .fire();
    }

}
