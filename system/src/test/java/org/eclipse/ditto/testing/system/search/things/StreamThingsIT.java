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
package org.eclipse.ditto.testing.system.search.things;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.asynchttpclient.proxy.ProxyServer;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.ws.ThingsWebsocketClient;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.thingsearch.model.signals.commands.subscription.CancelSubscription;
import org.eclipse.ditto.thingsearch.model.signals.commands.subscription.CreateSubscription;
import org.eclipse.ditto.thingsearch.model.signals.commands.subscription.RequestFromSubscription;
import org.eclipse.ditto.thingsearch.model.signals.events.SubscriptionCreated;
import org.eclipse.ditto.thingsearch.model.signals.events.SubscriptionFailed;
import org.eclipse.ditto.thingsearch.model.signals.events.SubscriptionHasNextPage;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamThingsIT extends IntegrationTest {

    /**
     * Select thing ID only in search results to keep frame size down.
     */
    private static final JsonFieldSelector THING_ID_SELECTOR = JsonFieldSelector.newInstance("thingId");

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamThingsIT.class);

    /**
     * Integration test for Thing search protocol.
     */
    private ThingsWebsocketClient client;
    private ThingsWebsocketClient client2;
    private static final long LATCH_TIMEOUT_SECONDS = 60;

    final static Thing thing = Thing.newBuilder()
            .setId(ThingId.of(idGenerator().withPrefixedRandomName("001")))
            .setAttribute(JsonPointer.of("counter"), JsonValue.of(10))
            .build();

    final static Thing thing2 = Thing.newBuilder()
            .setId(ThingId.of(idGenerator().withPrefixedRandomName("002")))
            .setAttribute(JsonPointer.of("foo"), JsonValue.of(10))
            .build();

    final static Thing thing3 = Thing.newBuilder()
            .setId(ThingId.of(serviceEnv.getTesting2NamespaceName(),
                    "thingIdWithWrongNamespace003" + UUID.randomUUID()))
            .setAttribute(JsonPointer.of("counter"), JsonValue.of(10))
            .build();

    final static Thing thing4 = Thing.newBuilder()
            .setId(ThingId.of(serviceEnv.getTesting2NamespaceName(),
                    "thingIdWithWrongNamespace004" + UUID.randomUUID()))
            .setAttribute(JsonPointer.of("counter"), JsonValue.of(10))
            .build();

    final static Thing thing5 = Thing.newBuilder()
            .setId(ThingId.of(idGenerator().withPrefixedRandomName("005")))
            .setAttribute(JsonPointer.of("counter"), JsonValue.of(10))
            .build();

    private static final String ISOLATION_FILTER = String.format(
            "in(thingId,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")",
            thing.getEntityId().orElseThrow(),
            thing2.getEntityId().orElseThrow(),
            thing3.getEntityId().orElseThrow(),
            thing4.getEntityId().orElseThrow(),
            thing5.getEntityId().orElseThrow()
    );

    private static AuthClient clientForDefaultSolution;
    private static AuthClient clientForSolution2;
    private static AuthClient secondClientForDefaultSolution;

    @Before
    public void createWebsocketClients() {
        client = newClient(clientForDefaultSolution.getAccessToken(), Collections.emptyMap());
        client2 = newClient(secondClientForDefaultSolution.getAccessToken(), Collections.emptyMap());
        client.connect("ThingsWebsocketClient1-" + UUID.randomUUID());
        client2.connect("ThingsWebsocketClient2-" + UUID.randomUUID());
    }

    @BeforeClass
    public static void createTestData() {
        clientForDefaultSolution = serviceEnv.getDefaultTestingContext().getOAuthClient();
        final TestingContext testingContext =
                TestingContext.withGeneratedMockClient(serviceEnv.getDefaultTestingContext().getSolution(),
                        TEST_CONFIG);
        secondClientForDefaultSolution = testingContext.getOAuthClient();

        clientForSolution2 = serviceEnv.getTestingContext2().getOAuthClient();

        final var acksHeaderKey = "requested-acks";
        final var acksHeaderVal = "[\"search-persisted\"]";

        putThing(2, thing, JsonSchemaVersion.V_2)
                .withHeader(acksHeaderKey, acksHeaderVal)
                .expectingStatusCodeSuccessful()
                .fire();

        putThing(2, thing2, JsonSchemaVersion.V_2)
                .withHeader(acksHeaderKey, acksHeaderVal)
                .expectingStatusCodeSuccessful()
                .fire();

        // create thing3 and thing4 with a different solution
        putThing(2, thing3, JsonSchemaVersion.V_2)
                .withHeader(acksHeaderKey, acksHeaderVal)
                .withJWT(clientForSolution2.getAccessToken())
                .expectingStatusCodeSuccessful()
                .fire();

        putThing(2, thing4, JsonSchemaVersion.V_2)
                .withHeader(acksHeaderKey, acksHeaderVal)
                .withJWT(clientForSolution2.getAccessToken())
                .expectingStatusCodeSuccessful()
                .fire();

        // create thing5 as user2 over REST because user1 lacks permission
        putThing(2, thing5, JsonSchemaVersion.V_2)
                .withHeader(acksHeaderKey, acksHeaderVal)
                .withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingStatusCodeSuccessful()
                .fire();
    }

    @AfterClass
    public static void deleteThingsAndPolicies() {
        deleteThing(2, thing.getEntityId().orElseThrow()).fire();
        deleteThing(2, thing2.getEntityId().orElseThrow()).fire();
        deleteThing(2, thing3.getEntityId().orElseThrow())
                .withJWT(clientForSolution2.getAccessToken())
                .fire();
        deleteThing(2, thing4.getEntityId().orElseThrow())
                .withJWT(clientForSolution2.getAccessToken())
                .fire();
        deleteThing(2, thing5.getEntityId().orElseThrow())
                .withJWT(secondClientForDefaultSolution.getAccessToken())
                .fire();

        deletePolicy(thing.getEntityId().orElseThrow()).fire();
        deletePolicy(thing2.getEntityId().orElseThrow()).fire();
        deletePolicy(thing3.getEntityId().orElseThrow())
                .withJWT(clientForSolution2.getAccessToken())
                .fire();
        deletePolicy(thing4.getEntityId().orElseThrow())
                .withJWT(clientForSolution2.getAccessToken())
                .fire();
        deletePolicy(thing5.getEntityId().orElseThrow())
                .withJWT(secondClientForDefaultSolution.getAccessToken())
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

        return ThingsWebsocketClient.newInstance(thingsWsUrl(JsonSchemaVersion.V_2.toInt()), suiteAuthToken,
                additionalHttpHeaders, proxyServer, ThingsWebsocketClient.JwtAuthMethod.HEADER);
    }

    @Test
    public void subscribeSearchAndRequestForResults() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof final SubscriptionCreated subscriptionCreated) {
                client.send(
                        RequestFromSubscription.of(subscriptionCreated.getSubscriptionId(), 5, DittoHeaders.empty()));
            }
            if (event instanceof final SubscriptionHasNextPage subscriptionHasNextPage) {
                assertThat(subscriptionHasNextPage.getItems().toString())
                        .contains(thing.getEntityId().orElseThrow().toString());
                latch.countDown();
            }
        });

        client.send(CreateSubscription.of(ISOLATION_FILTER, null, THING_ID_SELECTOR, null, DittoHeaders.empty()));

        assertThat(latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void subscribeSearchAndRequestForResultsWithFilter() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof final SubscriptionCreated subscriptionCreated) {
                client.send(
                        RequestFromSubscription.of(subscriptionCreated.getSubscriptionId(), 5, DittoHeaders.empty()));
            }
            if (event instanceof final SubscriptionHasNextPage subscriptionHasNextPage) {
                assertThat(subscriptionHasNextPage.getItems().toString())
                        .contains(thing.getEntityId().orElseThrow().toString())
                        .doesNotContain(thing2.getEntityId().orElseThrow().toString());
                latch.countDown();
            }
        });

        client.send(CreateSubscription.of(isolate("exists(attributes/counter)"), null, THING_ID_SELECTOR, null,
                DittoHeaders.empty()));

        assertThat(latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void subscribeSearchForSpecificNamespace() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof final SubscriptionCreated subscriptionCreated) {
                client.send(
                        RequestFromSubscription.of(subscriptionCreated.getSubscriptionId(), 5, DittoHeaders.empty()));
            }
            if (event instanceof final SubscriptionHasNextPage subscriptionHasNextPage) {
                assertThat(subscriptionHasNextPage.getItems().toString()).contains(
                        thing.getEntityId().orElseThrow().toString());
                assertThat(subscriptionHasNextPage.getItems().toString()).doesNotContain(
                        thing3.getEntityId().orElseThrow().toString());
                latch.countDown();
            }
        });

        client.send(
                CreateSubscription.of(ISOLATION_FILTER, "size(3)", THING_ID_SELECTOR,
                        Collections.singleton(serviceEnv.getDefaultNamespaceName()), DittoHeaders.empty()));

        assertThat(latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void subscribeWithOptionsTooLarge() throws Exception {

        final CountDownLatch failedLatch = new CountDownLatch(1);
        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof SubscriptionCreated) {
                client.send(RequestFromSubscription.of(((SubscriptionCreated) event).getSubscriptionId(), 5,
                        DittoHeaders.empty()));
            } else if (event instanceof SubscriptionFailed fail) {
                assertThat(fail.getError().getErrorCode()).isEqualTo("thing-search:search.option.invalid");
                failedLatch.countDown();
            }
        });

        final String subscribeMessage = "{" +
                "\"topic\": \"_/_/things/twin/search/subscribe\", " +
                "\"path\": \"/\", " +
                "\"value\": {\"options\": \"size(300)\"}" +
                "}";
        client.sendWithoutResponse(subscribeMessage);

        assertThat(failedLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void subscribeWithWrongOptions() throws Exception {

        final CountDownLatch failedLatch = new CountDownLatch(1);
        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof SubscriptionCreated) {
                client.send(RequestFromSubscription.of(((SubscriptionCreated) event).getSubscriptionId(), 5,
                        DittoHeaders.empty()));
            } else if (event instanceof SubscriptionFailed fail) {
                assertThat(fail.getError().getErrorCode()).isEqualTo("thing-search:search.option.invalid");
                failedLatch.countDown();
            }
        });

        final String subscribeMessage = "{" +
                "\"topic\": \"_/_/things/twin/search/subscribe\", " +
                "\"path\": \"/\", " +
                "\"value\": {\"options\": \"size=3\"}" +
                "}";

        client.sendWithoutResponse(subscribeMessage);

        assertThat(failedLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void subscribeWithWrongFilter() throws Exception {

        final CountDownLatch failedLatch = new CountDownLatch(1);
        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof SubscriptionCreated) {
                client.send(RequestFromSubscription.of(((SubscriptionCreated) event).getSubscriptionId(), 5,
                        DittoHeaders.empty()));
            } else if (event instanceof SubscriptionFailed fail) {
                assertThat(fail.getError().getErrorCode()).isEqualTo("rql.expression.invalid");
                failedLatch.countDown();
            }
        });

        final String subscribeMessage = "{" +
                "\"topic\": \"_/_/things/twin/search/subscribe\", " +
                "\"path\": \"/\", " +
                "\"value\": {\"filter\": \"exist=attribute/lol\"}" +
                "}";

        client.sendWithoutResponse(subscribeMessage);

        assertThat(failedLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void requestSubscriptionWithWrongSubscriptionId() throws Exception {

        final CountDownLatch failedLatch = new CountDownLatch(1);
        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof SubscriptionFailed fail) {
                assertThat(fail.getError().getErrorCode()).isEqualTo("thing-search:subscription.not.found");
                failedLatch.countDown();
            }
        });

        final String requestMessage = "{" +
                "\"topic\": \"_/_/things/twin/search/request\", " +
                "\"path\": \"/\", " +
                "\"value\": {\"subscriptionId\": \"" + Math.random() * 100 + "\",\"demand\":1}" +
                "}";

        client.sendWithoutResponse(requestMessage);

        assertThat(failedLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void subscribeSearchWithOtherSolutions() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof final SubscriptionCreated subscriptionCreated) {
                client.send(
                        RequestFromSubscription.of(subscriptionCreated.getSubscriptionId(), 5, DittoHeaders.empty()));
            }
            if (event instanceof final SubscriptionHasNextPage subscriptionHasNextPage) {
                assertThat(subscriptionHasNextPage.getItems().toString())
                        .contains(thing.getEntityId().orElseThrow().toString());
                assertThat(subscriptionHasNextPage.getItems().toString())
                        .doesNotContain(thing4.getEntityId().orElseThrow().toString());
                latch.countDown();
            }
        });

        client.send(CreateSubscription.of(ISOLATION_FILTER, "size(3)", THING_ID_SELECTOR, null, DittoHeaders.empty()));

        assertThat(latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void requestSubscriptionOfOtherSolution() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        client2.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof SubscriptionFailed fail) {
                assertThat(fail.getError().getErrorCode()).isEqualTo("thing-search:subscription.not.found");
                latch.countDown();
            }
        });

        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof final SubscriptionCreated subscriptionCreated) {
                client2.send(
                        RequestFromSubscription.of(subscriptionCreated.getSubscriptionId(), 5, DittoHeaders.empty()));
            }
        });

        client.send(CreateSubscription.of(ISOLATION_FILTER, "size(3)", THING_ID_SELECTOR, null, DittoHeaders.empty()));

        assertThat(latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void requestSubscriptionWithInvalidDemand() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof final SubscriptionCreated subscriptionCreated) {
                client.send(
                        RequestFromSubscription.of(subscriptionCreated.getSubscriptionId(), -3, DittoHeaders.empty()));
            }
            if (event instanceof SubscriptionFailed fail) {
                assertThat(fail.getError().getErrorCode()).isEqualTo("thing-search:subscription.protocol.error");
                latch.countDown();
            }
        });

        client.send(CreateSubscription.of(ISOLATION_FILTER, "size(3)", THING_ID_SELECTOR, null, DittoHeaders.empty()));

        assertThat(latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void cancelSubscriptionOfOtherSolution() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        client2.onSignal(event -> LOGGER.info("Got event {}", event));

        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof final SubscriptionCreated subscriptionCreated) {
                client2.send(CancelSubscription.of(subscriptionCreated.getSubscriptionId(), DittoHeaders.empty()));
                client.send(
                        RequestFromSubscription.of(subscriptionCreated.getSubscriptionId(), 5, DittoHeaders.empty()));
            }
            if (event instanceof SubscriptionHasNextPage) {
                latch.countDown();
            }
        });

        client.send(CreateSubscription.of(ISOLATION_FILTER, "size(3)", THING_ID_SELECTOR, null, DittoHeaders.empty()));

        assertThat(latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void assureSubscriptionTimeout() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof SubscriptionFailed fail) {
                assertThat(fail.getError().getErrorCode()).isEqualTo("thing-search:subscription.timeout");
                latch.countDown();
            }
        });

        client.send(CreateSubscription.of(ISOLATION_FILTER, "size(3)", THING_ID_SELECTOR, null, DittoHeaders.empty()));

        assertThat(latch.await(120, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void subscribeThingsWithoutPermission() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);

        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            if (event instanceof final SubscriptionCreated subscriptionCreated) {
                client.send(
                        RequestFromSubscription.of(subscriptionCreated.getSubscriptionId(), 5, DittoHeaders.empty()));
            }
            if (event instanceof final SubscriptionHasNextPage subscriptionHasNextPage) {
                assertThat(subscriptionHasNextPage.getItems().toString())
                        .contains(thing.getEntityId().orElseThrow().toString());
                assertThat(subscriptionHasNextPage.getItems().toString())
                        .doesNotContain(thing5.getEntityId().orElseThrow().toString());
                latch.countDown();
            }
        });

        client.send(CreateSubscription.of(ISOLATION_FILTER, "size(3)", THING_ID_SELECTOR,
                Collections.singleton(serviceEnv.getDefaultNamespaceName()), DittoHeaders.empty()));

        assertThat(latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void assureTopicPathTwinForSubscription() throws Exception {
        final CountDownLatch failedLatch = new CountDownLatch(1);
        client.onSignal(event -> {
            LOGGER.info("Got event {}", event);
            assertThat(event).isNotInstanceOf(SubscriptionCreated.class);
            failedLatch.countDown();
        });

        final String createMessage1 = "{" +
                "\"topic\": \"_/_/things/live/search/request\", " +
                "\"path\": \"/\", " +
                "\"value\": {}" +
                "}";

        final String createMessage2 = "{" +
                "\"topic\": \"_/_/none/live/search/request\", " +
                "\"path\": \"/\", " +
                "\"value\": {}" +
                "}";

        client.sendWithoutResponse(createMessage1);
        client.sendWithoutResponse(createMessage2);

        assertThat(failedLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    private static String isolate(final String filter) {
        return String.format("and(%s,%s)", filter, ISOLATION_FILTER);
    }

}
