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
package org.eclipse.ditto.testing.system.things.ws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.asynchttpclient.proxy.ProxyServer;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.commands.streaming.RequestFromStreamingSubscription;
import org.eclipse.ditto.base.model.signals.commands.streaming.SubscribeForPersistedEvents;
import org.eclipse.ditto.base.model.signals.events.streaming.StreamingSubscriptionComplete;
import org.eclipse.ditto.base.model.signals.events.streaming.StreamingSubscriptionCreated;
import org.eclipse.ditto.base.model.signals.events.streaming.StreamingSubscriptionFailed;
import org.eclipse.ditto.base.model.signals.events.streaming.StreamingSubscriptionHasNext;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.ws.ThingsWebsocketClient;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.exceptions.ThingNotAccessibleException;
import org.eclipse.ditto.things.model.signals.events.AttributeModified;
import org.eclipse.ditto.things.model.signals.events.ThingCreated;
import org.eclipse.ditto.things.model.signals.events.ThingEvent;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StreamPersistedEventsIT extends IntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamPersistedEventsIT.class);

    private static final int TOTAL_COUNTER_UPDATES = 100;
    private static final DittoProtocolAdapter DITTO_PROTOCOL_ADAPTER = DittoProtocolAdapter.newInstance();

    private ThingsWebsocketClient client;
    private ThingsWebsocketClient client2;
    private ThingsWebsocketClient client3;
    private static final long LATCH_TIMEOUT_SECONDS = 10;

    final static Thing thing = Thing.newBuilder()
            .setId(ThingId.of(idGenerator().withPrefixedRandomName("001")))
            .setAttribute(JsonPointer.of("counter"), JsonValue.of(0))
            .build();

    final static Thing thing2 = Thing.newBuilder()
            .setId(ThingId.of(idGenerator().withPrefixedRandomName("002")))
            .setAttribute(JsonPointer.of("counter"), JsonValue.of(0))
            .build();

    final static Thing thing3 = Thing.newBuilder()
            .setId(ThingId.of(serviceEnv.getTesting2NamespaceName(),
                    "003_" + UUID.randomUUID()))
            .setAttribute(JsonPointer.of("counter"), JsonValue.of(0))
            .build();

    private static AuthClient clientForDefaultContext;
    private static AuthClient clientForContext2;
    private static AuthClient secondClientForDefaultContext;

    @Before
    public void createWebsocketClients() {
        client = newClient(clientForDefaultContext.getAccessToken(), Collections.emptyMap());
        client2 = newClient(secondClientForDefaultContext.getAccessToken(), Collections.emptyMap());
        client3 = newClient(clientForContext2.getAccessToken(), Collections.emptyMap());
        client.connect("ThingsWebsocketClient1-" + UUID.randomUUID());
        client2.connect("ThingsWebsocketClient2-" + UUID.randomUUID());
        client3.connect("ThingsWebsocketClient3-" + UUID.randomUUID());
    }

    @BeforeClass
    public static void createTestData() {
        clientForDefaultContext = serviceEnv.getDefaultTestingContext().getOAuthClient();
        final TestingContext testingContext =
                TestingContext.withGeneratedMockClient(serviceEnv.getDefaultTestingContext().getSolution(),
                        TEST_CONFIG);
        secondClientForDefaultContext = testingContext.getOAuthClient();

        clientForContext2 = serviceEnv.getTestingContext2().getOAuthClient();

        putThing(2, thing, JsonSchemaVersion.V_2)
                .withHeader("If-None-Match", "*")
                .expectingStatusCodeSuccessful()
                .fire();

        putThing(2, thing2, JsonSchemaVersion.V_2)
                .withHeader("If-None-Match", "*")
                .expectingStatusCodeSuccessful()
                .fire();

        // create thing3 in a different solution
        putThing(2, thing3, JsonSchemaVersion.V_2)
                .withHeader("If-None-Match", "*")
                .withJWT(clientForContext2.getAccessToken())
                .expectingStatusCodeSuccessful()
                .fire();

        for (int i = 1; i <= TOTAL_COUNTER_UPDATES; i++) {
            putAttribute(2, thing.getEntityId().orElseThrow(), "counter", String.valueOf(i))
                    .expectingStatusCodeSuccessful()
                    .fire();
            if (i % 2 == 0) {
                putAttribute(2, thing2.getEntityId().orElseThrow(), "counter", String.valueOf(i))
                        .expectingStatusCodeSuccessful()
                        .fire();
            }
            if (i % 10 == 0) {
                putAttribute(2, thing3.getEntityId().orElseThrow(), "counter", String.valueOf(i))
                        .withJWT(clientForContext2.getAccessToken())
                        .expectingStatusCodeSuccessful()
                        .fire();
            }
        }
    }

    @AfterClass
    public static void deleteThingsAndPolicies() {
        deleteThing(2, thing.getEntityId().orElseThrow()).fire();
        deleteThing(2, thing2.getEntityId().orElseThrow()).fire();
        deleteThing(2, thing3.getEntityId().orElseThrow())
                .withJWT(clientForContext2.getAccessToken())
                .fire();

        deletePolicy(thing.getEntityId().orElseThrow()).fire();
        deletePolicy(thing2.getEntityId().orElseThrow()).fire();
        deletePolicy(thing3.getEntityId().orElseThrow())
                .withJWT(clientForContext2.getAccessToken())
                .fire();
    }

    @After
    public void stopWebsocketClients() {
        if (client != null) {
            client.disconnect();
        }
        if (client2 != null) {
            client2.disconnect();
        }
        if (client3 != null) {
            client3.disconnect();
        }
    }

    private static ThingsWebsocketClient newClient(final String suiteAuthToken,
            final Map<String, String> additionalHttpHeaders) {
        final ProxyServer proxyServer;
        if (TEST_CONFIG.isHttpProxyEnabled()) {
            proxyServer =
                    new ProxyServer.Builder(TEST_CONFIG.getHttpProxyHost(), TEST_CONFIG.getHttpProxyPort()).build();
        } else {
            proxyServer = null;
        }

        return ThingsWebsocketClient.newInstance(dittoWsUrl(JsonSchemaVersion.V_2.toInt()), suiteAuthToken,
                additionalHttpHeaders, proxyServer, ThingsWebsocketClient.JwtAuthMethod.HEADER);
    }

    @Test
    public void subscribeForAllPersistedEvents() throws Exception {

        final String correlationId = UUID.randomUUID().toString();
        final CountDownLatch protocolLatch = new CountDownLatch(2);
        final CountDownLatch nextLatch = new CountDownLatch(1 + TOTAL_COUNTER_UPDATES); // +1 for the created event
        final AtomicReference<String> subscriptionId = new AtomicReference<>();
        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof StreamingSubscriptionCreated subscriptionCreated) {
                subscriptionId.set(subscriptionCreated.getSubscriptionId());
                client.send(RequestFromStreamingSubscription.of(
                        thing.getEntityId().orElseThrow(),
                        JsonPointer.empty(),
                        subscriptionCreated.getSubscriptionId(),
                        TOTAL_COUNTER_UPDATES + 1,
                        DittoHeaders.empty()
                ));
                protocolLatch.countDown();
            } else if (event instanceof StreamingSubscriptionHasNext subscriptionHasNext) {
                final JsonValue item = subscriptionHasNext.getItem();
                assertThat(item.isObject()).isTrue();

                final Signal<?> historicalSignal = DITTO_PROTOCOL_ADAPTER.fromAdaptable(
                        ProtocolFactory.jsonifiableAdaptableFromJson(item.asObject()));

                assertThat(historicalSignal).isInstanceOf(ThingEvent.class);
                final ThingEvent<?> thingEvent = (ThingEvent<?>) historicalSignal;
                if (thingEvent.getRevision() == 1L) {
                    assertThat(thingEvent).isInstanceOf(ThingCreated.class);
                    final ThingCreated thingCreated = (ThingCreated) thingEvent;
                    assertThat(thingCreated.getThing().getAttributes()).isPresent();
                    assertThat(thingCreated.getThing().getAttributes().orElseThrow().getValue("counter"))
                            .contains(JsonValue.of(0));
                } else {
                    assertThat(thingEvent).isInstanceOf(AttributeModified.class);
                    final AttributeModified attributeModified = (AttributeModified) historicalSignal;
                    assertThat((CharSequence) attributeModified.getAttributePointer())
                            .isEqualTo(JsonPointer.of("counter"));
                    assertThat(attributeModified.getAttributeValue())
                            .isEqualTo(JsonValue.of(thingEvent.getRevision() - 1));
                }
                nextLatch.countDown();
            } else if (event instanceof StreamingSubscriptionComplete streamingSubscriptionComplete) {
                assertThat(streamingSubscriptionComplete.getSubscriptionId())
                        .isEqualTo(subscriptionId.get());
                protocolLatch.countDown();
            }
        });

        client.send(SubscribeForPersistedEvents.of(thing.getEntityId().orElseThrow(), JsonPointer.empty(),
                0L,
                null,
                null,
                null,
                DittoHeaders.newBuilder().correlationId(correlationId).build()
        ));

        assertThat(nextLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void subscribeForPersistedEventsInRevisionRange() throws Exception {

        final String correlationId = UUID.randomUUID().toString();
        final CountDownLatch protocolLatch = new CountDownLatch(2);
        final CountDownLatch nextLatch = new CountDownLatch(5);
        final AtomicReference<String> subscriptionId = new AtomicReference<>();
        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof StreamingSubscriptionCreated subscriptionCreated) {
                subscriptionId.set(subscriptionCreated.getSubscriptionId());
                client.send(RequestFromStreamingSubscription.of(
                        thing2.getEntityId().orElseThrow(),
                        JsonPointer.empty(),
                        subscriptionCreated.getSubscriptionId(),
                        10L,
                        DittoHeaders.empty()
                ));
                protocolLatch.countDown();
            } else if (event instanceof StreamingSubscriptionHasNext subscriptionHasNext) {
                final JsonValue item = subscriptionHasNext.getItem();
                assertThat(item.isObject()).isTrue();

                final Signal<?> historicalSignal = DITTO_PROTOCOL_ADAPTER.fromAdaptable(
                        ProtocolFactory.jsonifiableAdaptableFromJson(item.asObject()));

                assertThat(historicalSignal).isInstanceOf(ThingEvent.class);
                final ThingEvent<?> thingEvent = (ThingEvent<?>) historicalSignal;

                assertThat(thingEvent).isInstanceOf(AttributeModified.class);
                final AttributeModified attributeModified = (AttributeModified) historicalSignal;
                assertThat((CharSequence) attributeModified.getAttributePointer())
                        .isEqualTo(JsonPointer.of("counter"));
                assertThat(attributeModified.getAttributeValue())
                        .isIn(List.of(
                                JsonValue.of(8),
                                JsonValue.of(10),
                                JsonValue.of(12),
                                JsonValue.of(14),
                                JsonValue.of(16)
                        ));

                nextLatch.countDown();
            } else if (event instanceof StreamingSubscriptionComplete streamingSubscriptionComplete) {
                assertThat(streamingSubscriptionComplete.getSubscriptionId())
                        .isEqualTo(subscriptionId.get());
                protocolLatch.countDown();
            }
        });

        client.send(SubscribeForPersistedEvents.of(thing2.getEntityId().orElseThrow(), JsonPointer.empty(),
                5L,
                9L,
                null,
                null,
                DittoHeaders.newBuilder().correlationId(correlationId).build()
        ));

        assertThat(protocolLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(nextLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void subscribeForPersistedEventsInTimestampRange() throws Exception {

        final Instant startTs = Instant.now(); // at this point, all the test data was already created

        final String correlationId = UUID.randomUUID().toString();
        final CountDownLatch protocolLatch = new CountDownLatch(2);
        final CountDownLatch nextLatch = new CountDownLatch(1 + TOTAL_COUNTER_UPDATES / 10); // modulo 10
        final AtomicReference<String> subscriptionId = new AtomicReference<>();
        client3.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof StreamingSubscriptionCreated subscriptionCreated) {
                subscriptionId.set(subscriptionCreated.getSubscriptionId());
                client3.send(RequestFromStreamingSubscription.of(
                        thing3.getEntityId().orElseThrow(),
                        JsonPointer.empty(),
                        subscriptionCreated.getSubscriptionId(),
                        100L,
                        DittoHeaders.empty()
                ));
                protocolLatch.countDown();
            } else if (event instanceof StreamingSubscriptionHasNext subscriptionHasNext) {
                final JsonValue item = subscriptionHasNext.getItem();
                assertThat(item.isObject()).isTrue();

                final Signal<?> historicalSignal = DITTO_PROTOCOL_ADAPTER.fromAdaptable(
                        ProtocolFactory.jsonifiableAdaptableFromJson(item.asObject()));

                assertThat(historicalSignal).isInstanceOf(ThingEvent.class);
                final ThingEvent<?> thingEvent = (ThingEvent<?>) historicalSignal;
                if (thingEvent.getRevision() == 1L) {
                    assertThat(thingEvent).isInstanceOf(ThingCreated.class);
                    final ThingCreated thingCreated = (ThingCreated) thingEvent;
                    assertThat(thingCreated.getThing().getAttributes()).isPresent();
                    assertThat(thingCreated.getThing().getAttributes().orElseThrow().getValue("counter"))
                            .contains(JsonValue.of(0));
                } else {
                    assertThat(thingEvent).isInstanceOf(AttributeModified.class);
                    final AttributeModified attributeModified = (AttributeModified) historicalSignal;
                    assertThat((CharSequence) attributeModified.getAttributePointer())
                            .isEqualTo(JsonPointer.of("counter"));
                    assertThat(attributeModified.getAttributeValue())
                            .isIn(List.of(
                                    JsonValue.of(10),
                                    JsonValue.of(20),
                                    JsonValue.of(30),
                                    JsonValue.of(40),
                                    JsonValue.of(50),
                                    JsonValue.of(60),
                                    JsonValue.of(70),
                                    JsonValue.of(80),
                                    JsonValue.of(90),
                                    JsonValue.of(100)
                            ));
                }

                nextLatch.countDown();
            } else if (event instanceof StreamingSubscriptionComplete streamingSubscriptionComplete) {
                assertThat(streamingSubscriptionComplete.getSubscriptionId())
                        .isEqualTo(subscriptionId.get());
                protocolLatch.countDown();
            }
        });

        client3.send(SubscribeForPersistedEvents.of(thing3.getEntityId().orElseThrow(), JsonPointer.empty(),
                startTs.minus(5, ChronoUnit.MINUTES),
                startTs,
                DittoHeaders.newBuilder().correlationId(correlationId).build()
        ));

        assertThat(protocolLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(nextLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void subscribeForPersistedEventsWithoutPermission() throws Exception {

        final String correlationId = UUID.randomUUID().toString();
        final CountDownLatch protocolLatch = new CountDownLatch(1);
        final CountDownLatch errorLatch = new CountDownLatch(1);
        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof StreamingSubscriptionCreated subscriptionCreated) {
                client.send(RequestFromStreamingSubscription.of(
                        thing3.getEntityId().orElseThrow(),
                        JsonPointer.empty(),
                        subscriptionCreated.getSubscriptionId(),
                        1L,
                        DittoHeaders.empty()
                ));
                protocolLatch.countDown();
            } else if (event instanceof StreamingSubscriptionHasNext) {
                fail("Accessing persisted events of thing without permission must not be possible");
            } else if (event instanceof StreamingSubscriptionFailed subscriptionFailed) {
                assertThat((CharSequence) subscriptionFailed.getEntityId())
                        .contains(thing3.getEntityId().orElseThrow());
                assertThat(subscriptionFailed.getError())
                        .isInstanceOf(ThingNotAccessibleException.class);
                errorLatch.countDown();
            } else if (event instanceof StreamingSubscriptionComplete) {
                fail("Completion after failure must not longer be sent");
            }
        });

        client.send(SubscribeForPersistedEvents.of(thing3.getEntityId().orElseThrow(), JsonPointer.empty(),
                0L,
                null,
                null,
                null,
                DittoHeaders.newBuilder().correlationId(correlationId).build()
        ));

        assertThat(protocolLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(errorLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void requestSubscriptionOfOtherClient() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        client2.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof StreamingSubscriptionFailed fail) {
                assertThat(fail.getError().getErrorCode()).isEqualTo("streaming.subscription.not.found");
                latch.countDown();
            }
        });

        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof StreamingSubscriptionCreated subscriptionCreated) {
                client2.send(RequestFromStreamingSubscription.of(
                        thing.getEntityId().orElseThrow(),
                        JsonPointer.empty(),
                        subscriptionCreated.getSubscriptionId(),
                        1L,
                        DittoHeaders.empty()
                ));
            }
        });

        client.send(SubscribeForPersistedEvents.of(thing.getEntityId().orElseThrow(), JsonPointer.empty(),
                0L,
                null,
                null,
                null,
                DittoHeaders.empty()
        ));

        assertThat(latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

}
