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
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.messages.model.MessageDirection;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ServiceEnvironment;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.client.BasicAuth;
import org.eclipse.ditto.testing.common.client.http.AsyncHttpClientFactory;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.policies.api.Permission;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.ThingsSubjectIssuer;
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

    @Test
    @Category(Acceptance.class)
    public void partialAccessEventsAreFilteredViaSse() {
        final ThingId thingId = ThingId.of(idGenerator(interestingNamespace).withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final String clientId1 = serviceEnv.getDefaultTestingContext().getOAuthClient().getClientId();
        final String clientId2 = serviceEnv.getTestingContext2().getOAuthClient().getClientId();

        final Policy policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("owner")
                .setSubject(ThingsSubjectIssuer.DITTO, clientId1, org.eclipse.ditto.policies.model.SubjectType.GENERATED)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), Permission.WRITE, Permission.READ)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), Permission.WRITE, Permission.READ)
                .forLabel("partial")
                .setSubject(ThingsSubjectIssuer.DITTO, clientId2, org.eclipse.ditto.policies.model.SubjectType.GENERATED)
                .setGrantedPermissions("thing", "/attributes/public", "READ")
                .setGrantedPermissions("thing", "/attributes/shared", "READ")
                .setGrantedPermissions("thing", "/features/temperature/properties/value", "READ")
                .build();

        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setAttribute(JsonPointer.of("public"), JsonValue.of("public-value"))
                .setAttribute(JsonPointer.of("private"), JsonValue.of("private-value"))
                .setAttribute(JsonPointer.of("shared"), JsonValue.of("shared-value"))
                .setFeature("temperature", FeatureProperties.newBuilder()
                        .set("value", JsonValue.of(25.5))
                        .set("unit", JsonValue.of("celsius"))
                        .build())
                .setFeature("humidity", FeatureProperties.newBuilder()
                        .set("value", JsonValue.of(60.0))
                        .build())
                .build();

        putPolicy(policyId, policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final int expectedMessagesCount = 1;
        final String path = "/things/" + thingId;
        final SseTestDriver driver = createTestDriverForPartialAccess(expectedMessagesCount, path);
        driver.connect();

        putProperty(TestConstants.API_V_2, thingId, "temperature", "value", "26.0")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        putAttribute(TestConstants.API_V_2, thingId, "private", "\"updated-private\"")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final List<SseTestHandler.Message> actualMessages = driver.getMessages();
        assertThat(actualMessages).hasSize(expectedMessagesCount);

        actualMessages.stream()
                .map(SseTestHandler.Message::getData)
                .map(JsonFactory::readFrom)
                .map(JsonValue::asObject)
                .forEach(payload -> {
                    assertThat(payload.getValue(JsonPointer.of("/features/temperature/properties/value"))).isPresent();
                    assertThat(payload.getValue(JsonPointer.of("/attributes/private"))).isEmpty();
                    assertThat(payload.getValue(JsonPointer.of("/features/humidity"))).isEmpty();
                });

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    private SseTestDriver createTestDriverForPartialAccess(final int expectedMessagesCount, final String path) {
        final String url = dittoUrl(TestConstants.API_V_2, path);
        LOGGER.info("Opening SSE on url '{}'", url);

        final String authorization;
        final BasicAuth basicAuth = serviceEnv.getTestingContext2().getBasicAuth();
        if (basicAuth.isEnabled()) {
            final String credentials = basicAuth.getUsername() + ":" + basicAuth.getPassword();
            authorization = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes());
        } else {
            authorization = "Bearer " + serviceEnv.getTestingContext2().getOAuthClient().getAccessToken();
        }

        return new SseTestDriver(client, url, authorization, expectedMessagesCount);
    }

    private SseTestDriver createTestDriverForUser(final int expectedMessagesCount, final String path, 
            final TestingContext testingContext) {
        final String url = dittoUrl(TestConstants.API_V_2, path);

        final String authorization;
        final BasicAuth basicAuth = testingContext.getBasicAuth();
        if (basicAuth.isEnabled()) {
            final String credentials = basicAuth.getUsername() + ":" + basicAuth.getPassword();
            authorization = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes());
        } else {
            authorization = "Bearer " + testingContext.getOAuthClient().getAccessToken();
        }

        return new SseTestDriver(client, url, authorization, expectedMessagesCount);
    }

    @Test
    @Category(Acceptance.class)
    public void partialAccessEventsFilteredForRevokedPathsStrictMatchingViaSse() {
        final String ATTR_TYPE = "type";
        final String ATTR_HIDDEN = "hidden";
        final String ATTR_COMPLEX = "complex";
        final String ATTR_COMPLEX_SOME = "complex/some";
        final String ATTR_COMPLEX_SECRET = "complex/secret";
        final String COMPLEX_FIELD_SOME = "some";
        final String COMPLEX_FIELD_SECRET = "secret";

        final String FEATURE_SOME = "some";
        final String FEATURE_OTHER = "other";

        final String PROP_PROPERTIES = "properties";
        final String PROP_CONFIGURATION = "configuration";
        final String PROP_FOO = "foo";
        final String PROP_BAR = "bar";
        final String PROP_PUBLIC = "public";

        final JsonPointer ATTR_PTR_TYPE = JsonPointer.of(ATTR_TYPE);
        final JsonPointer ATTR_PTR_HIDDEN = JsonPointer.of(ATTR_HIDDEN);
        final JsonPointer ATTR_PTR_COMPLEX = JsonPointer.of(ATTR_COMPLEX);

        final ThingId thingId = ThingId.of(idGenerator(interestingNamespace).withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final String clientId1 = serviceEnv.getDefaultTestingContext().getOAuthClient().getClientId();
        final String clientId2 = serviceEnv.getTestingContext2().getOAuthClient().getClientId();

        final Policy policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("owner")
                .setSubject(ThingsSubjectIssuer.DITTO, clientId1, org.eclipse.ditto.policies.model.SubjectType.GENERATED)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), Permission.WRITE, Permission.READ)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), Permission.WRITE, Permission.READ)
                .forLabel("partial1")
                .setSubject(ThingsSubjectIssuer.DITTO, clientId1, org.eclipse.ditto.policies.model.SubjectType.GENERATED)
                .setGrantedPermissions("thing", "/attributes/" + ATTR_TYPE, "READ")
                .setGrantedPermissions("thing", "/attributes/" + ATTR_COMPLEX_SOME, "READ")
                .setGrantedPermissions("thing", "/features/" + FEATURE_SOME, "READ")
                .setRevokedPermissions(PoliciesResourceType.thingResource("/attributes/" + ATTR_HIDDEN), Permission.READ)
                .setRevokedPermissions(PoliciesResourceType.thingResource("/attributes/" + ATTR_COMPLEX_SECRET), Permission.READ)
                .forLabel("partial2")
                .setSubject(ThingsSubjectIssuer.DITTO, clientId2, org.eclipse.ditto.policies.model.SubjectType.GENERATED)
                .setGrantedPermissions("thing", "/attributes/" + ATTR_COMPLEX, "READ")
                .setGrantedPermissions("thing", "/attributes/" + ATTR_COMPLEX_SOME, "READ")
                .setGrantedPermissions("thing", "/features/" + FEATURE_OTHER + "/properties/properties/" + PROP_PUBLIC, "READ")
                .setRevokedPermissions(PoliciesResourceType.thingResource("/attributes/" + ATTR_COMPLEX_SECRET), Permission.READ)
                .build();

        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setAttribute(ATTR_PTR_TYPE, JsonValue.of("LORAWAN_GATEWAY"))
                .setAttribute(ATTR_PTR_HIDDEN, JsonValue.of(false))
                .setAttribute(ATTR_PTR_COMPLEX, JsonFactory.newObjectBuilder()
                        .set(COMPLEX_FIELD_SOME, JsonValue.of(100))
                        .set(COMPLEX_FIELD_SECRET, JsonValue.of("secret-value"))
                        .build())
                .setFeature(FEATURE_SOME, FeatureProperties.newBuilder()
                        .set(PROP_PROPERTIES, JsonFactory.newObjectBuilder()
                                .set(PROP_CONFIGURATION, JsonFactory.newObjectBuilder()
                                        .set(PROP_FOO, JsonValue.of(123))
                                        .build())
                                .build())
                        .build())
                .setFeature(FEATURE_OTHER, FeatureProperties.newBuilder()
                        .set(PROP_PROPERTIES, JsonFactory.newObjectBuilder()
                                .set(PROP_PUBLIC, JsonValue.of("public-value"))
                                .set(PROP_BAR, JsonValue.of(true))
                                .build())
                        .build())
                .build();

        putPolicy(policyId, policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final String path = "/things/" + thingId;
        final int user1ExpectedCount = 4;
        final SseTestDriver user1Driver = createTestDriverForUser(user1ExpectedCount, path, 
                serviceEnv.getDefaultTestingContext());
        user1Driver.connect();

        final int user2ExpectedCount = 3;
        final SseTestDriver user2Driver = createTestDriverForUser(user2ExpectedCount, path, 
                serviceEnv.getTestingContext2());
        user2Driver.connect();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        putAttribute(TestConstants.API_V_2, thingId, ATTR_TYPE, "\"LORAWAN_GATEWAY_V2\"")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putAttribute(TestConstants.API_V_2, thingId, ATTR_COMPLEX_SOME, "42")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putAttribute(TestConstants.API_V_2, thingId, ATTR_COMPLEX_SECRET, "\"super-secret\"")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putAttribute(TestConstants.API_V_2, thingId, ATTR_HIDDEN, "false")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putProperty(TestConstants.API_V_2, thingId, FEATURE_SOME, PROP_PROPERTIES + "/" + PROP_CONFIGURATION + "/" + PROP_FOO, "456")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putProperty(TestConstants.API_V_2, thingId, FEATURE_OTHER, PROP_PROPERTIES + "/" + PROP_PUBLIC, "\"updated public value\"")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putProperty(TestConstants.API_V_2, thingId, FEATURE_OTHER, PROP_PROPERTIES + "/" + PROP_BAR, "false")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        final Thing updatedThing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setAttribute(ATTR_PTR_TYPE, JsonValue.of("LORAWAN_GATEWAY_V3"))
                .setAttribute(ATTR_PTR_HIDDEN, JsonValue.of(true))
                .setAttribute(ATTR_PTR_COMPLEX, JsonFactory.newObjectBuilder()
                        .set(COMPLEX_FIELD_SOME, JsonValue.of(100))
                        .set(COMPLEX_FIELD_SECRET, JsonValue.of("new-secret"))
                        .build())
                .setFeature(FEATURE_SOME, FeatureProperties.newBuilder()
                        .set(PROP_PROPERTIES, JsonFactory.newObjectBuilder()
                                .set(PROP_CONFIGURATION, JsonFactory.newObjectBuilder()
                                        .set(PROP_FOO, JsonValue.of(999))
                                        .build())
                                .build())
                        .build())
                .setFeature(FEATURE_OTHER, FeatureProperties.newBuilder()
                        .set(PROP_PROPERTIES, JsonFactory.newObjectBuilder()
                                .set(PROP_PUBLIC, JsonValue.of("full update public"))
                                .set(PROP_BAR, JsonValue.of(false))
                                .build())
                        .build())
                .build();
        putThing(TestConstants.API_V_2, updatedThing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        final List<SseTestHandler.Message> user1Messages = user1Driver.getMessages();
        final List<SseTestHandler.Message> user2Messages = user2Driver.getMessages();

        user1Messages.stream()
                .map(SseTestHandler.Message::getData)
                .filter(data -> data != null && !data.isEmpty())
                .map(data -> {
                    try {
                        return JsonFactory.readFrom(data);
                    } catch (Exception e) {
                        LOGGER.warn("Failed to parse JSON message, skipping: {}", data);
                        return null;
                    }
                })
                .filter(jsonValue -> jsonValue != null)
                .map(JsonValue::asObject)
                .forEach(payload -> {
                    assertThat(payload.getValue(JsonPointer.of("/attributes/" + ATTR_HIDDEN))).isEmpty();
                    if (payload.getValue(JsonPointer.of("/attributes/" + ATTR_COMPLEX)).isPresent()) {
                        final JsonValue complexValue = payload.getValue(JsonPointer.of("/attributes/" + ATTR_COMPLEX)).get();
                        if (complexValue.isObject()) {
                            assertThat(complexValue.asObject().getValue(JsonPointer.of(COMPLEX_FIELD_SECRET))).isEmpty();
                        }
                    }
                    assertThat(payload.getValue(JsonPointer.of("/features/" + FEATURE_OTHER))).isEmpty();
                });

        user2Messages.stream()
                .map(SseTestHandler.Message::getData)
                .filter(data -> data != null && !data.isEmpty())
                .map(data -> {
                    try {
                        return JsonFactory.readFrom(data);
                    } catch (Exception e) {
                        LOGGER.warn("Failed to parse JSON message, skipping: {}", data);
                        return null;
                    }
                })
                .filter(jsonValue -> jsonValue != null)
                .map(JsonValue::asObject)
                .forEach(payload -> {
                    assertThat(payload.getValue(JsonPointer.of("/attributes/" + ATTR_TYPE))).isEmpty();
                    assertThat(payload.getValue(JsonPointer.of("/attributes/" + ATTR_HIDDEN))).isEmpty();
                    if (payload.getValue(JsonPointer.of("/attributes/" + ATTR_COMPLEX)).isPresent()) {
                        final JsonValue complexValue = payload.getValue(JsonPointer.of("/attributes/" + ATTR_COMPLEX)).get();
                        if (complexValue.isObject()) {
                            assertThat(complexValue.asObject().getValue(JsonPointer.of(COMPLEX_FIELD_SECRET))).isEmpty();
                        }
                    }
                    assertThat(payload.getValue(JsonPointer.of("/features/" + FEATURE_SOME))).isEmpty();
                    if (payload.getValue(JsonPointer.of("/features/" + FEATURE_OTHER + "/" + PROP_PROPERTIES)).isPresent()) {
                        final JsonValue propsValue = payload.getValue(JsonPointer.of("/features/" + FEATURE_OTHER + "/" + PROP_PROPERTIES)).get();
                        if (propsValue.isObject()) {
                            assertThat(propsValue.asObject().getValue(JsonPointer.of(PROP_BAR))).isEmpty();
                        }
                    }
                });

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    @Category(Acceptance.class)
    public void partialAccessComprehensiveTestAllScenariosViaSse() {
        final String ATTR_TYPE = "type";
        final String ATTR_HIDDEN = "hidden";
        final String ATTR_COMPLEX = "complex";
        final String ATTR_COMPLEX_SOME = "complex/some";
        final String ATTR_COMPLEX_SECRET = "complex/secret";
        final String COMPLEX_FIELD_SOME = "some";
        final String COMPLEX_FIELD_SECRET = "secret";
        final String COMPLEX_FIELD_NEW = "newField";
        final String ATTR_NEW = "newAttr";

        final String FEATURE_SOME = "some";
        final String FEATURE_OTHER = "other";
        final String FEATURE_SHARED = "shared";

        final String PROP_PROPERTIES = "properties";
        final String PROP_CONFIGURATION = "configuration";
        final String PROP_FOO = "foo";
        final String PROP_BAR = "bar";
        final String PROP_PUBLIC = "public";
        final String PROP_VALUE = "value";
        final String PROP_SECRET = "secret";

        final JsonPointer ATTR_PTR_TYPE = JsonPointer.of(ATTR_TYPE);
        final JsonPointer ATTR_PTR_HIDDEN = JsonPointer.of(ATTR_HIDDEN);
        final JsonPointer ATTR_PTR_COMPLEX = JsonPointer.of(ATTR_COMPLEX);
        final JsonPointer ATTR_PTR_COMPLEX_SOME = JsonPointer.of(ATTR_COMPLEX_SOME);
        final JsonPointer ATTR_PTR_COMPLEX_SECRET = JsonPointer.of(ATTR_COMPLEX_SECRET);

        final ThingId thingId = ThingId.of(idGenerator(interestingNamespace).withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final String clientId1 = serviceEnv.getDefaultTestingContext().getOAuthClient().getClientId();
        final String clientId2 = serviceEnv.getTestingContext2().getOAuthClient().getClientId();

        final Policy policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("owner")
                .setSubject(ThingsSubjectIssuer.DITTO, clientId1, org.eclipse.ditto.policies.model.SubjectType.GENERATED)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), Permission.WRITE, Permission.READ)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), Permission.WRITE, Permission.READ)
                .forLabel("partial1")
                .setSubject(ThingsSubjectIssuer.DITTO, clientId1, org.eclipse.ditto.policies.model.SubjectType.GENERATED)
                .setGrantedPermissions("thing", "/attributes/" + ATTR_TYPE, "READ")
                .setGrantedPermissions("thing", "/attributes/" + ATTR_COMPLEX_SOME, "READ")
                .setGrantedPermissions("thing", "/features/" + FEATURE_SOME, "READ")
                .setGrantedPermissions("thing", "/features/" + FEATURE_SOME + "/properties/properties/" + PROP_CONFIGURATION + "/" + PROP_FOO, "READ")
                .setGrantedPermissions("thing", "/features/" + FEATURE_SHARED, "READ")
                .setGrantedPermissions("thing", "/features/" + FEATURE_SHARED + "/properties/" + PROP_VALUE, "READ")
                .setRevokedPermissions(PoliciesResourceType.thingResource("/attributes/" + ATTR_HIDDEN), Permission.READ)
                .setRevokedPermissions(PoliciesResourceType.thingResource("/attributes/" + ATTR_COMPLEX_SECRET), Permission.READ)
                .setRevokedPermissions(PoliciesResourceType.thingResource("/features/" + FEATURE_OTHER), Permission.READ)
                .setRevokedPermissions(PoliciesResourceType.thingResource("/features/" + FEATURE_SHARED + "/properties/" + PROP_SECRET), Permission.READ)
                .forLabel("partial2")
                .setSubject(ThingsSubjectIssuer.DITTO, clientId2, org.eclipse.ditto.policies.model.SubjectType.GENERATED)
                .setGrantedPermissions("thing", "/attributes/" + ATTR_COMPLEX, "READ")
                .setGrantedPermissions("thing", "/attributes/" + ATTR_COMPLEX_SOME, "READ")
                .setGrantedPermissions("thing", "/features/" + FEATURE_OTHER, "READ")
                .setGrantedPermissions("thing", "/features/" + FEATURE_OTHER + "/properties/properties/" + PROP_PUBLIC, "READ")
                .setGrantedPermissions("thing", "/features/" + FEATURE_OTHER + "/properties/" + PROP_PUBLIC, "READ")
                .setGrantedPermissions("thing", "/features/" + FEATURE_SHARED, "READ")
                .setGrantedPermissions("thing", "/features/" + FEATURE_SHARED + "/properties/" + PROP_VALUE, "READ")
                .setGrantedPermissions("thing", "/features/" + FEATURE_SHARED + "/properties/" + PROP_SECRET, "READ")
                .setRevokedPermissions(PoliciesResourceType.thingResource("/features/" + FEATURE_SOME), Permission.READ)
                .setRevokedPermissions(PoliciesResourceType.thingResource("/attributes/" + ATTR_COMPLEX_SECRET), Permission.READ)
                .build();

        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setAttribute(ATTR_PTR_TYPE, JsonValue.of("LORAWAN_GATEWAY"))
                .setAttribute(ATTR_PTR_HIDDEN, JsonValue.of(false))
                .setAttribute(ATTR_PTR_COMPLEX, JsonFactory.newObjectBuilder()
                        .set(COMPLEX_FIELD_SOME, JsonValue.of(41))
                        .set(COMPLEX_FIELD_SECRET, JsonValue.of("pssst"))
                        .build())
                .setFeature(FEATURE_SOME, FeatureProperties.newBuilder()
                        .set(PROP_PROPERTIES, JsonFactory.newObjectBuilder()
                                .set(PROP_CONFIGURATION, JsonFactory.newObjectBuilder()
                                        .set(PROP_FOO, JsonValue.of(123))
                                        .build())
                                .build())
                        .build())
                .setFeature(FEATURE_OTHER, FeatureProperties.newBuilder()
                        .set(PROP_PROPERTIES, JsonFactory.newObjectBuilder()
                                .set(PROP_BAR, JsonValue.of(false))
                                .set(PROP_PUBLIC, JsonValue.of("here you go, buddy"))
                                .build())
                        .build())
                .setFeature(FEATURE_SHARED, FeatureProperties.newBuilder()
                        .set(PROP_PROPERTIES, JsonFactory.newObjectBuilder()
                                .set(PROP_VALUE, JsonValue.of("shared-value"))
                                .set(PROP_SECRET, JsonValue.of("shared-secret-value"))
                                .build())
                        .build())
                .build();

        putPolicy(policyId, policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final String path = "/things/" + thingId;
        final int user1ExpectedCount = 10;
        final SseTestDriver user1Driver = createTestDriverForUser(user1ExpectedCount, path,
                serviceEnv.getDefaultTestingContext());
        user1Driver.connect();

        final int user2ExpectedCount = 8;
        final SseTestDriver user2Driver = createTestDriverForUser(user2ExpectedCount, path,
                serviceEnv.getTestingContext2());
        user2Driver.connect();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        putAttribute(TestConstants.API_V_2, thingId, ATTR_TYPE, "\"LORAWAN_GATEWAY_V2\"")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putAttribute(TestConstants.API_V_2, thingId, ATTR_COMPLEX_SOME, "42")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putAttribute(TestConstants.API_V_2, thingId, ATTR_COMPLEX_SECRET, "\"super-secret\"")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putAttribute(TestConstants.API_V_2, thingId, ATTR_HIDDEN, "false")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putProperty(TestConstants.API_V_2, thingId, FEATURE_SOME, PROP_PROPERTIES + "/" + PROP_CONFIGURATION + "/" + PROP_FOO, "456")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putProperty(TestConstants.API_V_2, thingId, FEATURE_OTHER, PROP_PROPERTIES + "/" + PROP_PUBLIC, "\"updated public value\"")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putProperty(TestConstants.API_V_2, thingId, FEATURE_OTHER, PROP_PROPERTIES + "/" + PROP_BAR, "false")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        final JsonObject complexAttributes = JsonFactory.newObjectBuilder()
                .set(COMPLEX_FIELD_SOME, JsonValue.of(100))
                .set(COMPLEX_FIELD_SECRET, JsonValue.of("new-secret"))
                .set(COMPLEX_FIELD_NEW, JsonValue.of("new-value"))
                .build();
        patchThing(TestConstants.API_V_2, thingId, JsonPointer.of("attributes/" + ATTR_COMPLEX), complexAttributes)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        final JsonObject allAttributesJson = JsonFactory.newObjectBuilder()
                .set(ATTR_TYPE, JsonValue.of("LORAWAN_GATEWAY_V3"))
                .set(ATTR_HIDDEN, JsonValue.of(true))
                .set(ATTR_COMPLEX, JsonFactory.newObjectBuilder()
                        .set(COMPLEX_FIELD_SOME, JsonValue.of(200))
                        .set(COMPLEX_FIELD_SECRET, JsonValue.of("modify-secret"))
                        .build())
                .set(ATTR_NEW, JsonValue.of("new-attribute-value"))
                .build();
        putAttributes(TestConstants.API_V_2, thingId, allAttributesJson.toString())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        final Thing updatedThing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setAttribute(ATTR_PTR_TYPE, JsonValue.of("LORAWAN_GATEWAY_V3"))
                .setAttribute(ATTR_PTR_HIDDEN, JsonValue.of(true))
                .setAttribute(ATTR_PTR_COMPLEX, JsonFactory.newObjectBuilder()
                        .set(COMPLEX_FIELD_SOME, JsonValue.of(100))
                        .set(COMPLEX_FIELD_SECRET, JsonValue.of("new-secret"))
                        .build())
                .setFeature(FEATURE_SOME, FeatureProperties.newBuilder()
                        .set(PROP_PROPERTIES, JsonFactory.newObjectBuilder()
                                .set(PROP_CONFIGURATION, JsonFactory.newObjectBuilder()
                                        .set(PROP_FOO, JsonValue.of(999))
                                        .build())
                                .build())
                        .build())
                .setFeature(FEATURE_OTHER, FeatureProperties.newBuilder()
                        .set(PROP_PROPERTIES, JsonFactory.newObjectBuilder()
                                .set(PROP_PUBLIC, JsonValue.of("full update public"))
                                .set(PROP_BAR, JsonValue.of(false))
                                .build())
                        .build())
                .setFeature(FEATURE_SHARED, FeatureProperties.newBuilder()
                        .set(PROP_PROPERTIES, JsonFactory.newObjectBuilder()
                                .set(PROP_VALUE, JsonValue.of("preserved-shared-value"))
                                .set(PROP_SECRET, JsonValue.of("preserved-shared-secret"))
                                .build())
                        .build())
                .build();
        putThing(TestConstants.API_V_2, updatedThing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        final JsonObject mergedThingJson = JsonFactory.newObjectBuilder()
                .set("attributes", JsonFactory.newObjectBuilder()
                        .set(ATTR_TYPE, JsonValue.of("LORAWAN_GATEWAY_V4"))
                        .set(ATTR_COMPLEX, JsonFactory.newObjectBuilder()
                                .set(COMPLEX_FIELD_SOME, JsonValue.of(150))
                                .build())
                        .build())
                .set("features", JsonFactory.newObjectBuilder()
                        .set(FEATURE_SOME, JsonFactory.newObjectBuilder()
                                .set(PROP_PROPERTIES, JsonFactory.newObjectBuilder()
                                        .set(PROP_CONFIGURATION, JsonFactory.newObjectBuilder()
                                                .set(PROP_FOO, JsonValue.of(8888))
                                                .build())
                                        .build())
                                .build())
                        .set(FEATURE_OTHER, JsonFactory.newObjectBuilder()
                                .set(PROP_PROPERTIES, JsonFactory.newObjectBuilder()
                                        .set(PROP_PUBLIC, JsonValue.of("nested-public-value"))
                                        .build())
                                .build())
                        .set(FEATURE_SHARED, JsonFactory.newObjectBuilder()
                                .set(PROP_PROPERTIES, JsonFactory.newObjectBuilder()
                                        .set(PROP_VALUE, JsonValue.of("merged-shared-value"))
                                        .set(PROP_SECRET, JsonValue.of("merged-shared-secret"))
                                        .build())
                                .build())
                        .build())
                .build();
        patchThing(TestConstants.API_V_2, thingId, JsonPointer.empty(), mergedThingJson)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putProperty(TestConstants.API_V_2, thingId, FEATURE_OTHER, PROP_PROPERTIES + "/" + PROP_PUBLIC, "\"nested-public-value\"")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putProperty(TestConstants.API_V_2, thingId, FEATURE_OTHER, PROP_PROPERTIES + "/" + PROP_BAR, "true")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putProperty(TestConstants.API_V_2, thingId, FEATURE_SOME, PROP_PROPERTIES + "/" + PROP_CONFIGURATION + "/" + PROP_FOO, "8888")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putProperty(TestConstants.API_V_2, thingId, FEATURE_SHARED, PROP_PROPERTIES + "/" + PROP_VALUE, "\"updated-shared-value\"")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        putProperty(TestConstants.API_V_2, thingId, FEATURE_SHARED, PROP_PROPERTIES + "/" + PROP_SECRET, "\"updated-shared-secret\"")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final List<SseTestHandler.Message> user1Messages = user1Driver.getMessages();
        final List<SseTestHandler.Message> user2Messages = user2Driver.getMessages();

        user1Messages.stream()
                .map(SseTestHandler.Message::getData)
                .filter(data -> data != null && !data.isEmpty())
                .map(data -> {
                    try {
                        return JsonFactory.readFrom(data);
                    } catch (Exception e) {
                        LOGGER.warn("Failed to parse JSON message, skipping: {}", data);
                        return null;
                    }
                })
                .filter(jsonValue -> jsonValue != null)
                .map(JsonValue::asObject)
                .forEach(payload -> {
                    assertThat(payload.getValue(JsonPointer.of("/attributes/" + ATTR_HIDDEN))).isEmpty();
                    if (payload.getValue(JsonPointer.of("/attributes/" + ATTR_COMPLEX)).isPresent()) {
                        final JsonValue complexValue = payload.getValue(JsonPointer.of("/attributes/" + ATTR_COMPLEX)).get();
                        if (complexValue.isObject()) {
                            assertThat(complexValue.asObject().getValue(JsonPointer.of(COMPLEX_FIELD_SECRET))).isEmpty();
                        }
                    }
                    assertThat(payload.getValue(JsonPointer.of("/features/" + FEATURE_OTHER))).isEmpty();
                    if (payload.getValue(JsonPointer.of("/features/" + FEATURE_SHARED + "/" + PROP_PROPERTIES)).isPresent()) {
                        final JsonValue propsValue = payload.getValue(JsonPointer.of("/features/" + FEATURE_SHARED + "/" + PROP_PROPERTIES)).get();
                        if (propsValue.isObject()) {
                            assertThat(propsValue.asObject().getValue(JsonPointer.of(PROP_SECRET))).isEmpty();
                        }
                    }
                });

        user2Messages.stream()
                .map(SseTestHandler.Message::getData)
                .filter(data -> data != null && !data.isEmpty())
                .map(data -> {
                    try {
                        return JsonFactory.readFrom(data);
                    } catch (Exception e) {
                        LOGGER.warn("Failed to parse JSON message, skipping: {}", data);
                        return null;
                    }
                })
                .filter(jsonValue -> jsonValue != null)
                .map(JsonValue::asObject)
                .forEach(payload -> {
                    assertThat(payload.getValue(JsonPointer.of("/attributes/" + ATTR_TYPE))).isEmpty();
                    assertThat(payload.getValue(JsonPointer.of("/attributes/" + ATTR_HIDDEN))).isEmpty();
                    if (payload.getValue(JsonPointer.of("/attributes/" + ATTR_COMPLEX)).isPresent()) {
                        final JsonValue complexValue = payload.getValue(JsonPointer.of("/attributes/" + ATTR_COMPLEX)).get();
                        if (complexValue.isObject()) {
                            assertThat(complexValue.asObject().getValue(JsonPointer.of(COMPLEX_FIELD_SECRET))).isEmpty();
                        }
                    }
                    assertThat(payload.getValue(JsonPointer.of("/features/" + FEATURE_SOME))).isEmpty();
                    if (payload.getValue(JsonPointer.of("/features/" + FEATURE_OTHER + "/" + PROP_PROPERTIES)).isPresent()) {
                        final JsonValue propsValue = payload.getValue(JsonPointer.of("/features/" + FEATURE_OTHER + "/" + PROP_PROPERTIES)).get();
                        if (propsValue.isObject()) {
                            assertThat(propsValue.asObject().getValue(JsonPointer.of(PROP_BAR))).isEmpty();
                        }
                    }
                });

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

}
