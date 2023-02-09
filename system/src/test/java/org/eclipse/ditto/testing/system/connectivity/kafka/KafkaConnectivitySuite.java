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
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION1;
import static org.eclipse.ditto.testing.system.connectivity.ConnectivityConstants.TARGET_SUFFIX;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.Enforcement;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityITBase;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityITestCases;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityWorker;
import org.eclipse.ditto.testing.system.connectivity.Connections;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * System tests for Connectivity of Kafka connections.
 * Failsafe won't run this directly because it does not end in *IT.java.
 */
public final class KafkaConnectivitySuite extends
        AbstractConnectivityITestCases<BlockingQueue<ConsumerRecord<String, byte[]>>, ConsumerRecord<String, byte[]>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectivitySuite.class);

    private static final String TARGET_ADDRESS_PREFIX = "test-target-";
    private static final String SOURCE_ADDRESS_PREFIX = "test-source-";

    private static final long KAFKA_POLL_TIMEOUT_MS = 600_000L; // practically infinite, poll forever
    private static final long WAIT_TIMEOUT_MS = 10_000L;
    // if after 10secs waiting time no message arrived, handle as "no message consumed"
    private static final ConnectionType CONNECTION_TYPE = ConnectionType.KAFKA;

    private static final Enforcement KAFKA_ENFORCEMENT =
            ConnectivityModelFactory.newEnforcement("{{ header:device_id }}", "{{ thing:id }}",
                    "{{ policy:id }}");

    private static final String KAFKA_TEST_HOSTNAME = CONFIG.getKafkaHostname();
    private static final int KAFKA_TEST_PORT = CONFIG.getKafkaPort();
    private static final String KAFKA_TEST_USERNAME = CONFIG.getKafkaUsername();
    private static final String KAFKA_TEST_PASSWORD = CONFIG.getKafkaPassword();

    private static final String KAFKA_SERVICE_HOSTNAME = CONFIG.getKafkaHostname();
    private static final int KAFKA_SERVICE_PORT = CONFIG.getKafkaPort();

    @Rule
    public final TestWatcher watchman = new AbstractConnectivityITBase.DefaultTestWatcher(LOGGER);

    @Rule
    public TestName testName = new TestName();

    private final KafkaConnectivityWorker kafkaConnectivityWorker;
    private KafkaConsumer<String, byte[]> kafkaConsumer;
    private int consumerCounter = 0;
    private int producerCounter = 0;

    public KafkaConnectivitySuite() {
        super(ConnectivityFactory.of(
                        "Kafka",
                        connectionModelFactory,
                        CONNECTION_TYPE,
                        KafkaConnectivitySuite::getConnectionUri,
                        KafkaConnectivitySuite::getSpecificConfig,
                        KafkaConnectivitySuite::defaultTargetAddress,
                        KafkaConnectivitySuite::defaultSourceAddress,
                        id -> KAFKA_ENFORCEMENT,
                        () -> SSH_TUNNEL_CONFIG)
                .withDefaultHeaderMapping(
                        Map.of(
                                "correlation-id", "{{ header:correlation-id }}",
                                "content-type", "{{ header:content-type }}"
                        )
                )
                .withReplyTargetAddress(connectionName -> defaultSourceAddress(connectionName) + "-reply"));
        kafkaConnectivityWorker =
                new KafkaConnectivityWorker(LOGGER,
                        this::createKafkaConsumer,
                        this::createKafkaProducer,
                        KafkaConnectivitySuite::defaultTargetAddress,
                        KafkaConnectivitySuite::defaultSourceAddress,
                        KafkaConnectivitySuite::defaultReplyTargetAddress,
                        Duration.ofMillis(KAFKA_POLL_TIMEOUT_MS),
                        Duration.ofMillis(WAIT_TIMEOUT_MS));
    }

    protected AbstractConnectivityWorker<BlockingQueue<ConsumerRecord<String, byte[]>>, ConsumerRecord<String, byte[]>> getConnectivityWorker() {
        return kafkaConnectivityWorker;
    }

    private static String defaultTargetAddress(final String suffix) {
        return TARGET_ADDRESS_PREFIX + suffix;
    }

    private static String defaultSourceAddress(final String suffix) {
        return SOURCE_ADDRESS_PREFIX + suffix;
    }

    private static String defaultReplyTargetAddress(final String suffix) {
        return defaultSourceAddress(suffix) + "-reply";
    }

    @Before
    @Override
    public void setupConnectivity() throws Exception {
        super.setupConnectivity();
        LOGGER.info("Preparing Kafka at {}:{}", KAFKA_TEST_HOSTNAME, KAFKA_TEST_PORT);

        final String itestsName = cf.connectionNameWithAuthPlaceholderOnHEADER_ID + "_itests";
        final List<String> newTopics =
                Stream.concat(
                                Stream.of(itestsName),
                                connectionsWatcher.getConnections()
                                        .keySet().stream()
                                        .map(cf::getConnectionName)
                                        .flatMap(names -> Stream.of(defaultTargetAddress(names), defaultSourceAddress(names),
                                                defaultSourceAddress(names) + "-reply")))
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
                KAFKA_TEST_USERNAME, KAFKA_TEST_PASSWORD, testName.getMethodName() + ++producerCounter);
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
        // tunneling not implemented for kafka
        return "tcp://" + URLEncoder.encode(KAFKA_TEST_USERNAME, StandardCharsets.UTF_8) + ":" +
                URLEncoder.encode(KAFKA_TEST_PASSWORD, StandardCharsets.UTF_8) + "@" + KAFKA_TEST_HOSTNAME + ":" + KAFKA_TEST_PORT;
    }

    private static Map<String, String> getSpecificConfig() {
        return Map.of("bootstrapServers", KAFKA_SERVICE_HOSTNAME + ":" + KAFKA_SERVICE_PORT,
                "consumerOffsetReset", "earliest");
    }

    @Override
    protected String targetAddressForTargetPlaceHolderSubstitution() {
        return String.format("%s%s", defaultTargetAddress(cf.connectionNameWithAuthPlaceholderOnHEADER_ID),
                TARGET_SUFFIX);
    }

    @Test
    @Connections(CONNECTION1)
    public void sendManyThingModifyCommandsAndVerifyProcessingIsThrottled() {

        // tolerance for timing inaccuracy
        final double timingTolerance = 0.05;

        // maximum number of lost messages
        final int maxMessageLoss = 0;

        // consumer is throttled to 10 messages per second.
        final double expectedPerMessageDelayMs = 100;

        // create thing with READ permission for the default solution
        final ThingId thingId = sendCreateThingAndEnsureResponseIsSentBack(cf.connectionName1,
                ThingBuilder.FromScratch::build);

        final JsonPointer attributeKey = JsonPointer.of("test");

        final int numberOfMessages = 400;

        final double startTime = System.currentTimeMillis();

        for (int i = 0; i < numberOfMessages; ++i) {
            final String connectionName = cf.connectionName1;
            final String attributeValue = "value " + i;
            sendSignal(connectionName, modifyAttribute(thingId, attributeKey, attributeValue));
        }
        final int expectedArrivals = numberOfMessages - maxMessageLoss;

        // expect all messages are processed
        getThing(2, thingId)
                .withParam("fields", "_revision")
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .useAwaitility(Awaitility.await()
                        .pollInterval(Duration.ofSeconds(1))
                        .atMost(Duration.ofMinutes(1)))
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(satisfies(body ->
                        assertThat(JsonFactory.newObject(body).getValueOrThrow(Thing.JsonFields.REVISION))
                                .isGreaterThan(expectedArrivals)))
                .fire();

        final double endTime = System.currentTimeMillis();
        final double delay = endTime - startTime;
        final double expectedDelay = expectedArrivals * expectedPerMessageDelayMs;
        LOGGER.info("Total processing time of {} ModifyAttribute was {}ms (expected: {}ms)", expectedArrivals,
                delay, expectedDelay);

        assertThat(delay).isGreaterThan(expectedDelay * (1.0 - timingTolerance));
    }

}
