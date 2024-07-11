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
package org.eclipse.ditto.testing.system.things.sse;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.eclipse.ditto.base.model.assertions.DittoBaseAssertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import org.assertj.core.api.Assertions;
import org.asynchttpclient.AsyncHttpClient;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.messages.model.MessageDirection;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ServiceEnvironment;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.client.BasicAuth;
import org.eclipse.ditto.testing.common.client.http.AsyncHttpClientFactory;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for the SSE endpoint of Things entity at {@code /api/2/things}.
 */
public final class ThingsServerSentEventIT extends IntegrationTest {

    private static String interestingNamespace;
    private static String anotherNamespace;

    private AsyncHttpClient client;

    @BeforeClass
    public static void setupNamespaces() {
        interestingNamespace = ServiceEnvironment.createRandomDefaultNamespace();
        anotherNamespace = serviceEnv.getSecondaryNamespaceName();
    }


    @Before
    public void setup() {
        client = AsyncHttpClientFactory.newInstance(TEST_CONFIG);
    }

    @After
    public void teardown() {
        AsyncHttpClientFactory.close(client);
    }

    @Category(Acceptance.class)
    @Test
    public void thingCreation() {
        final int expectedMessagesCount = 1;

        final SseTestDriver driver = createTestDriver(expectedMessagesCount,
                getDefaultPath("namespaces=" + interestingNamespace));
        driver.connect();

        final ThingId thingId = ThingId.generateRandom(interestingNamespace);
        putThing(TestConstants.API_V_2, Thing.newBuilder().setId(thingId).build(), JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        assertThat(driver.getMessages()).hasSize(expectedMessagesCount);
    }

    @Test
    public void featureModification() {
        final ThingId thingId = ThingId.generateRandom(interestingNamespace);
        putThing(TestConstants.API_V_2, Thing.newBuilder().setId(thingId).build(), JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final String featureId = "foo";

        final int expectedMessagesCount = 1;
        final SseTestDriver driver = createTestDriver(expectedMessagesCount,
                getDefaultPath("namespaces=" + interestingNamespace));
        driver.connect();

        final FeatureProperties featureProperties = FeatureProperties.newBuilder()
                .set("one", 1)
                .set("two", true)
                .build();

        final Feature featureFoo = Feature.newBuilder().properties(featureProperties).withId(featureId).build();
        putFeature(TestConstants.API_V_2, thingId, featureId, featureFoo.toJsonString())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final List<SseTestHandler.Message> actualMessages = driver.getMessages();
        assertThat(actualMessages).hasSize(expectedMessagesCount);
        final Thing thing = actualMessages.stream()
                .map(SseTestHandler.Message::getData)
                .map(ThingsModelFactory::newThing)
                .findFirst()
                .get();
        assertThat(thing.getFeatures()).isPresent();
        assertThat(thing.getFeatures().get().getFeature(featureId)).isPresent();
        assertThat(thing.getFeatures().get().getFeature(featureId).get().getProperty("two"))
                .contains(JsonValue.of(true));
    }

    @Test
    public void featureModificationWithSelectingFields() {
        final String thingId = parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        final String featureId = "foo";

        final int expectedMessagesCount = 2;
        final var path = getDefaultPath("fields=features/" + featureId + "/properties/matching");
        final SseTestDriver driver = createTestDriver(expectedMessagesCount, path);
        driver.connect();

        final Feature featureFoo = Feature.newBuilder().properties(FeatureProperties.newBuilder()
                .set("one", 1)
                .set("matching", "hello there!") // should be emitted to SSE
                .build()
        ).withId(featureId).build();
        putFeature(TestConstants.API_V_2, thingId, featureId, featureFoo.toJsonString())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final Feature featureFoo1 = Feature.newBuilder().properties(FeatureProperties.newBuilder()
                .set("one", 1)
                .set("two", true)
                .build()
        ).withId(featureId).build();
        putFeature(TestConstants.API_V_2, thingId, featureId, featureFoo1.toJsonString())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final Feature featureFoo2 = Feature.newBuilder().properties(FeatureProperties.newBuilder()
                .set("matching", 1) // should be emitted to SSE
                .build()
        ).withId(featureId).build();
        putFeature(TestConstants.API_V_2, thingId, featureId, featureFoo2.toJsonString())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final Feature featureFoo3 = Feature.newBuilder().properties(FeatureProperties.newBuilder()
                .set("not-matching", "hello")
                .build()
        ).withId(featureId).build();
        putFeature(TestConstants.API_V_2, thingId, featureId, featureFoo3.toJsonString())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final List<SseTestHandler.Message> actualMessages = driver.getMessages();
        assertThat(actualMessages).hasSize(expectedMessagesCount);
        actualMessages.stream()
                .map(SseTestHandler.Message::getData)
                .map(ThingsModelFactory::newThing)
                .forEach(thing -> {
                    assertThat(thing.getFeatures()).isPresent();
                    assertThat(thing.getFeatures().get().getFeature(featureId)).isPresent();
                    assertThat(thing.getFeatures()
                            .get()
                            .getFeature(featureId)
                            .get()
                            .getProperty("matching"))
                            .isPresent();
                });
    }

    @Test
    public void featurePropertyModificationWithNamespaces() {
        final String featureId = "foo";

        final ThingId thingId1 = ThingId.of(idGenerator(interestingNamespace).withRandomName());
        final Thing thing1 = Thing.newBuilder()
                .setId(thingId1)
                .setFeature(featureId)
                .build();

        putThing(TestConstants.API_V_2, thing1, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final ThingId thingId2 = ThingId.of(idGenerator(anotherNamespace).withRandomName());
        final Thing thing2 = Thing.newBuilder()
                .setId(thingId2)
                .setFeature(featureId)
                .build();

        putThing(TestConstants.API_V_2, thing2, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final int expectedMessagesCount = 1;
        final var path = getDefaultPath("namespaces=" + interestingNamespace);
        final SseTestDriver driver = createTestDriver(expectedMessagesCount, path);
        driver.connect();

        putProperty(TestConstants.API_V_2, thingId1, featureId, "matching", "true")
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putProperty(TestConstants.API_V_2, thingId2, featureId, "matching", "true")
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final List<SseTestHandler.Message> actualMessages = driver.getMessages();
        assertThat(actualMessages).hasSize(expectedMessagesCount);
        actualMessages.stream()
                .map(SseTestHandler.Message::getData)
                .map(ThingsModelFactory::newThing)
                .forEach(thing -> {
                    Assertions.assertThat(thing.getEntityId()).contains(thingId1);
                    Assertions.assertThat(thing.getFeatures()).isPresent();
                    Assertions.assertThat(thing.getFeatures().get().getFeature(featureId)).isPresent();
                });
    }

    @Test
    public void openingSseFailsWithMalformedRqlFilter() {
        final var path = getDefaultPath("filter=ge(features/foo/properties/matching/num,20");
        final SseTestDriver driver = createTestDriver(0, path);
        assertThatExceptionOfType(CompletionException.class)
                .isThrownBy(driver::connect)
                .withCauseInstanceOf(IllegalStateException.class);
        assertThatExceptionOfType(Exception.class).isThrownBy(driver::getMessages);
    }

    @Test
    public void featurePropertyModificationWithRqlFilter() {
        final String featureId = "foo";

        final ThingId thingId1 = ThingId.of(idGenerator(interestingNamespace).withRandomName());
        final Thing thing1 = Thing.newBuilder()
                .setId(thingId1)
                .setFeature(featureId)
                .build();

        putThing(TestConstants.API_V_2, thing1, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final ThingId thingId2 = ThingId.of(idGenerator(interestingNamespace).withRandomName());
        final Thing thing2 = Thing.newBuilder()
                .setId(thingId2)
                .setFeature(featureId)
                .build();

        putThing(TestConstants.API_V_2, thing2, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final int expectedMessagesCount = 3;
        final var path = getDefaultPath("filter=ge(features/" + featureId + "/properties/matching/num,20)");
        final SseTestDriver driver = createTestDriver(expectedMessagesCount, path);
        driver.connect();

        putProperty(TestConstants.API_V_2, thingId1, featureId, "matching/num", "10")
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putProperty(TestConstants.API_V_2, thingId2, featureId, "matching/num", "20")
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putProperty(TestConstants.API_V_2, thingId1, featureId, "matching/num", "30")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putProperty(TestConstants.API_V_2, thingId2, featureId, "matching/num", "40")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final List<SseTestHandler.Message> actualMessages = driver.getMessages();
        assertThat(actualMessages).hasSize(expectedMessagesCount);
        actualMessages.stream()
                .map(SseTestHandler.Message::getData)
                .map(ThingsModelFactory::newThing)
                .forEach(thing -> {
                    assertThat(thing.getFeatures()).isPresent();
                    assertThat(thing.getFeatures().get().getFeature(featureId)).isPresent();
                    assertThat(thing.getFeatures().get().getFeature(featureId).get()
                            .getProperty("matching/num")).isPresent();
                    assertThat(thing.getFeatures().get().getFeature(featureId).get()
                            .getProperty("matching/num").get().asInt()).isGreaterThanOrEqualTo(20);
                });
    }

    @Test
    public void featurePropertyModificationWithExtraFieldsAndRqlFilter() {
        final String featureId = "foo";

        final ThingId thingId1 = ThingId.of(idGenerator(interestingNamespace).withRandomName());
        final Thing thing1 = Thing.newBuilder()
                .setId(thingId1)
                .setFeature(featureId)
                .build();

        putThing(TestConstants.API_V_2, thing1, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final int expectedMessagesCount = 2;
        final String propertyPath = "features/" + featureId + "/properties/matching/num";
        final var path = getDefaultPath(String.format("filter=ge(%s,20)&extraFields=%s", propertyPath, propertyPath));
        final SseTestDriver driver = createTestDriver(expectedMessagesCount, path);
        driver.connect();

        // Excluded: filter does not match.
        putProperty(TestConstants.API_V_2, thingId1, featureId, "matching/num", "10")
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        // Excluded: filter still does not match.
        putProperty(TestConstants.API_V_2, thingId1, featureId, "non-matching/num", "20")
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        // Included: filter matches.
        putProperty(TestConstants.API_V_2, thingId1, featureId, "matching/num", "30")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        // Included: filter matches due to extra fields
        putProperty(TestConstants.API_V_2, thingId1, featureId, "non-matching/num", "40")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final List<SseTestHandler.Message> actualMessages = driver.getMessages();
        assertThat(actualMessages).hasSize(expectedMessagesCount);
        actualMessages.stream()
                .map(SseTestHandler.Message::getData)
                .map(ThingsModelFactory::newThing)
                .forEach(thing -> {
                    assertThat(thing.getFeatures()).isPresent();
                    assertThat(thing.getFeatures().get().getFeature(featureId)).isPresent();
                    assertThat(thing.getFeatures().get().getFeature(featureId).get()
                            .getProperty("matching/num")).isPresent();
                    assertThat(thing.getFeatures().get().getFeature(featureId).get()
                            .getProperty("matching/num").get().asInt()).isEqualTo(30);
                });
    }

    @Test
    public void attributeModificationWithExtraFieldsContainingSameAttribute() {
        final ThingId thingId1 = ThingId.of(idGenerator(interestingNamespace).withRandomName());
        final String counterKey = "counter";
        final Attributes attributes = Attributes.newBuilder()
                .set(counterKey, 0)
                .build();

        final Thing thing1 = Thing.newBuilder()
                .setId(thingId1)
                .setAttributes(attributes)
                .build();

        putThing(TestConstants.API_V_2, thing1, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final int expectedMessagesCount = 2;
        final var path = getDefaultPath("namespaces=" + interestingNamespace + "&extraFields=attributes/counter");
        final SseTestDriver driver = createTestDriver(expectedMessagesCount, path);
        driver.connect();

        // Don't set counter:
        putAttribute(TestConstants.API_V_2, thingId1, "other-attribute", "\"foobar42\"")
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        // Set counter to 1:
        putAttribute(TestConstants.API_V_2, thingId1, counterKey, "1")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        // Set counter to 2:
        putAttribute(TestConstants.API_V_2, thingId1, counterKey, "2")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final List<SseTestHandler.Message> actualMessages = driver.getMessages();
        assertThat(actualMessages).hasSize(expectedMessagesCount);
        final AtomicInteger expectedCounter = new AtomicInteger(0);
        actualMessages.stream()
                .map(SseTestHandler.Message::getData)
                .map(ThingsModelFactory::newThing)
                .forEach(thing -> {
                    assertThat(thing.getAttributes()).isPresent();
                    assertThat(thing.getAttributes().get().getValue(JsonPointer.of(counterKey))).isPresent();
                    assertThat(thing.getAttributes().get().getValue(JsonPointer.of(counterKey)).get().asInt())
                            .isEqualTo(expectedCounter.getAndIncrement());
                });
    }

    @Test
    public void featureCreationModificationDeletionEnsuringEventOrder() throws InterruptedException {
        final ThingId thingId = ThingId.generateRandom(interestingNamespace);
        putThing(TestConstants.API_V_2, Thing.newBuilder().setId(thingId).build(), JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final String featureId = "foo";
        final String counterPointer = "counter";

        final FeatureProperties featureProperties = FeatureProperties.newBuilder()
                .set(counterPointer, 0)
                .build();

        final Feature featureFoo = Feature.newBuilder().properties(featureProperties).withId(featureId).build();
        putFeature(TestConstants.API_V_2, thingId, featureId, featureFoo.toJsonString())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final int expectedMessagesCount = 25;
        final CountDownLatch latch = new CountDownLatch(expectedMessagesCount);
        final SseTestDriver driver = createTestDriver(expectedMessagesCount,
                getDefaultPath("namespaces=" + interestingNamespace));
        driver.connect();

        for (int i = 1; i <= expectedMessagesCount; i++) {
            putProperty(TestConstants.API_V_2, thingId, featureId, counterPointer, String.valueOf(i))
                    .expectingHttpStatus(HttpStatus.NO_CONTENT)
                    .fire();
        }

        final List<SseTestHandler.Message> actualMessages = driver.getMessages();
        assertThat(actualMessages).hasSize(expectedMessagesCount);

        final AtomicInteger updateCounter = new AtomicInteger(0);
        for (final SseTestHandler.Message message : actualMessages) {
            final int currentCounter = ThingsModelFactory.newThing(message.getData())
                    .getFeatures().get()
                    .getFeature(featureId).get()
                    .getProperty(counterPointer).get()
                    .asInt();
            final int expectedCounter = updateCounter.incrementAndGet();
            assertThat(currentCounter).isEqualTo(expectedCounter);
            latch.countDown();
        }

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void featurePropertyModificationWithExtraFieldsAndRqlFilterUsingTopicPath() {
        final String featureId = "foo";

        final ThingId thingId1 = ThingId.of(idGenerator(interestingNamespace).withRandomName());
        final Thing thing1 = Thing.newBuilder()
                .setId(thingId1)
                .setFeature(featureId)
                .build();
        final ThingId thingId2 = ThingId.of(idGenerator(interestingNamespace).withRandomName());
        final Thing thing2 = Thing.newBuilder()
                .setId(thingId2)
                .setAttributes(Attributes.newBuilder()
                        .set("marked", true)
                        .build()
                )
                .setFeature(featureId)
                .build();

        final int expectedMessagesCount = 3;
        final String propertyPath = "features/" + featureId + "/properties/matching/num";
        final var path = getDefaultPath(String.format("filter=" +
                "or(" +
                "and(ne(topic:action,'created'),ge(%s,20))," +
                "and(eq(topic:action,'created'),exists(attributes/marked))" +
                ")" +
                "&extraFields=%s", propertyPath, propertyPath));
        final SseTestDriver driver = createTestDriver(expectedMessagesCount, path);
        driver.connect();

        // Excluded: created events are excluded by filter
        putThing(TestConstants.API_V_2, thing1, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        // Included: the thing2 created events should be included because of the 'marked' attribute
        putThing(TestConstants.API_V_2, thing2, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        // Excluded: filter is not matching "ge" part
        putProperty(TestConstants.API_V_2, thingId1, featureId, "matching/num", "10")
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        // Excluded: filter is not matching "ge" part, as different propertyJsonPointer is changed
        putProperty(TestConstants.API_V_2, thingId1, featureId, "non-matching/num", "20")
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        // Included: filter matches.
        putProperty(TestConstants.API_V_2, thingId1, featureId, "matching/num", "30")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        // Included: filter matches due to extra fields
        putProperty(TestConstants.API_V_2, thingId1, featureId, "non-matching/num", "40")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final List<SseTestHandler.Message> actualMessages = driver.getMessages();
        assertThat(actualMessages).hasSize(expectedMessagesCount);
        actualMessages.stream()
                .map(SseTestHandler.Message::getData)
                .map(ThingsModelFactory::newThing)
                .forEach(thing -> {
                    assertThat(thing.getFeatures()).isPresent();
                    assertThat(thing.getFeatures().get().getFeature(featureId)).isPresent();
                });
    }

    @Test
    public void singleFeatureModification() {
        final ThingId thingId = ThingId.generateRandom(interestingNamespace);
        putThing(TestConstants.API_V_2, Thing.newBuilder().setId(thingId).build(), JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final String featureId = "foo";

        final int expectedMessagesCount = 1;
        final var path = "/things/" + thingId + "/features/" + featureId;
        final SseTestDriver driver = createTestDriver(expectedMessagesCount, path);
        driver.connect();

        final FeatureProperties featureProperties = FeatureProperties.newBuilder()
                .set("one", 1)
                .set("two", true)
                .build();

        final Feature featureFoo = Feature.newBuilder().properties(featureProperties).withId(featureId).build();
        putFeature(TestConstants.API_V_2, thingId, featureId, featureFoo.toJsonString())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final Feature featureBar = Feature.newBuilder().properties(featureProperties).withId("bar").build();
        putFeature(TestConstants.API_V_2, thingId, "bar", featureBar.toJsonString())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final List<SseTestHandler.Message> actualMessages = driver.getMessages();
        assertThat(actualMessages).hasSize(expectedMessagesCount);

        final Feature feature = actualMessages.stream()
                .map(SseTestHandler.Message::getData)
                .map(featureJsonString -> ThingsModelFactory.newFeatureBuilder(featureJsonString).useId(featureId).build())
                .findFirst()
                .get();
        assertThat(feature.getProperty("two")).contains(JsonValue.of(true));
    }

    @Test
    public void messageToThingInbox() {
        final ThingId thingId = ThingId.generateRandom(interestingNamespace);
        putThing(TestConstants.API_V_2, Thing.newBuilder().setId(thingId).build(), JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final int expectedMessagesCount = 2;
        final var path = "/things/" + thingId + "/inbox/messages";
        final SseTestDriver driver = createTestDriver(expectedMessagesCount, path);
        driver.connect();

        final var contentType = "text/plain";
        final var subject1 = "bumlux-one";
        final var payload1 = "test-one";
        postMessage(TestConstants.API_V_2, thingId, MessageDirection.TO, subject1, contentType, payload1.getBytes(StandardCharsets.UTF_8))
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                .fire();

        final var subject2 = "bumlux-one";
        final var payload2 = "test-one";
        postMessage(TestConstants.API_V_2, thingId, MessageDirection.TO, subject2, contentType, payload2.getBytes(StandardCharsets.UTF_8))
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                .fire();

        final List<SseTestHandler.Message> actualMessages = driver.getMessages();
        assertThat(actualMessages).hasSize(expectedMessagesCount);

        assertThat(actualMessages).containsExactly(
                new SseTestHandler.Message(payload1, "event:" + subject1),
                new SseTestHandler.Message(payload2, "event:" + subject2)
        );
    }

    @Test
    public void messageToFeatureInbox() {
        final ThingId thingId = ThingId.generateRandom(interestingNamespace);
        final var featureId = "foo";
        final var thing = Thing.newBuilder()
                .setId(thingId)
                .setFeature(Feature.newBuilder().withId(featureId).build())
                .build();

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final int expectedMessagesCount = 1;
        final var subject = "bumlux";
        final var path = "/things/" + thingId + "/features/" + featureId + "/inbox/messages/" + subject;
        final SseTestDriver driver = createTestDriver(expectedMessagesCount, path);
        driver.connect();

        final var payload = "test";
        final var contentType = "text/plain";
        postMessage(TestConstants.API_V_2, thingId, featureId, MessageDirection.TO, subject, contentType, payload.getBytes(StandardCharsets.UTF_8))
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                .fire();

        final List<SseTestHandler.Message> actualMessages = driver.getMessages();
        assertThat(actualMessages).hasSize(expectedMessagesCount);

        final var message = actualMessages.stream()
                .findFirst()
                .get();
        assertThat(message.getData()).isEqualTo(payload);
        assertThat(message.getType()).contains("event:" + subject);
    }

    private SseTestDriver createTestDriver(final int expectedMessagesCount, final String path) {
        final String url = dittoUrl(TestConstants.API_V_2, path);
        LOGGER.info("Opening SSE on url '{}'", url);

        final String authorization;
        final BasicAuth basicAuth = serviceEnv.getDefaultTestingContext().getBasicAuth();
        if (basicAuth.isEnabled()) {
            final String credentials = basicAuth.getUsername() + ":" + basicAuth.getPassword();
            authorization = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes());
        } else {
            authorization = "Bearer " + serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken();
        }

        return new SseTestDriver(client, url, authorization, expectedMessagesCount);
    }

    private String getDefaultPath() {
        return getDefaultPath(null);
    }

    private String getDefaultPath(@Nullable final String queryParams) {
        return (queryParams != null && !queryParams.isEmpty()) ? "/things?" + queryParams : "/things";
    }

}
