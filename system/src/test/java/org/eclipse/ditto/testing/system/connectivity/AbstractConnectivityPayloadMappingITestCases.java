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
package org.eclipse.ditto.testing.system.connectivity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION1;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION_WITH_CONNECTION_ANNOUNCEMENTS;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION_WITH_MULTIPLE_PAYLOAD_MAPPINGS;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

import org.assertj.core.api.SoftAssertionError;
import org.assertj.core.util.Throwables;
import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.json.Jsonifiable;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.DittoClients;
import org.eclipse.ditto.client.configuration.AccessTokenAuthenticationConfiguration;
import org.eclipse.ditto.client.configuration.MessagingConfiguration;
import org.eclipse.ditto.client.configuration.ProxyConfiguration;
import org.eclipse.ditto.client.configuration.WebSocketMessagingConfiguration;
import org.eclipse.ditto.client.messaging.AuthenticationProvider;
import org.eclipse.ditto.client.messaging.AuthenticationProviders;
import org.eclipse.ditto.client.messaging.MessagingProviders;
import org.eclipse.ditto.connectivity.model.signals.announcements.ConnectionClosedAnnouncement;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.json.assertions.DittoJsonAssertions;
import org.eclipse.ditto.jwt.model.ImmutableJsonWebToken;
import org.eclipse.ditto.messages.model.Message;
import org.eclipse.ditto.messages.model.MessageBuilder;
import org.eclipse.ditto.messages.model.MessageDirection;
import org.eclipse.ditto.messages.model.signals.commands.SendThingMessage;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectId;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.TopicPath;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.categories.RequireProtocolHeaders;
import org.eclipse.ditto.testing.common.categories.RequireSource;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.assertions.DittoThingsAssertions;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttribute;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttributeResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyFeature;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neovisionaries.ws.client.WebSocket;

import io.restassured.response.Response;

public abstract class AbstractConnectivityPayloadMappingITestCases<C, M>
        extends AbstractConnectivityHeaderMappingITestCases<C, M> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnectivityPayloadMappingITestCases.class);
    protected static final String CONNECTION_STATUS_FEATURE = "ConnectionStatus";
    protected static final String READY_SINCE_PROPERTY = "readySince";
    protected static final String READY_UNTIL_PROPERTY = "readyUntil";
    protected static final String STATUS_PROPERTY = "status";
    private static final Random RANDOM = new Random();

    protected AbstractConnectivityPayloadMappingITestCases(final ConnectivityFactory cf) {
        super(cf);
    }

    @Test
    @Category(RequireSource.class)
    @Connections(ConnectionCategory.CONNECTION_WITH_PAYLOAD_MAPPING)
    public void sendWebsocketLiveCommandAnswersWithLiveResponseAndEnsurePayloadMappingWorks() throws Exception {
        // Given
        final int multiplier = 2;

        final ThingId thingId = generateThingId();

        final SubjectId connectivitySubjectId = SubjectId.newInstance(SubjectIssuer.INTEGRATION,
                SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution().getUsername() + ":" + cf.connectionNameWithPayloadMapping);
        final Subject connectivitySubject = Subject.newInstance(connectivitySubjectId);

        final Policy policy = Policy.newBuilder(PolicyId.of(thingId))
                .forLabel("OWNER")
                .setSubject(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getDefaultSubject())
                .setSubject(connectivitySubject)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"),
                        READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"),
                        READ, WRITE)
                .forLabel("DEVICE")
                .build();

        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();

        putThingWithPolicy(2, thing, policy, JsonSchemaVersion.V_2)
                .withJWT(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final CountDownLatch websocketLatch = new CountDownLatch(1);
        final CountDownLatch connectivityLatch = new CountDownLatch(multiplier);

        // check preservation of custom headers
        final String customHeaderName = "send-websocket-live-command-answers-with-live-response-header-name";
        final String customHeaderValue1 = "send-websocket-live-command-answers-with-live-response-header-value1";
        final String customHeaderValue2 = "send-websocket-live-command-answers-with-live-response-header-value2";

        // WebSocket client consumes live commands:
        final ConnectivityTestWebsocketClient clientUser1 =
                ConnectivityTestWebsocketClient.newInstance(thingsWsUrl(TestConstants.API_V_2),
                        SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getAccessToken());
        clientUser1.connect("sendWebsocketLiveCommandAnswersWithLiveResponse-" + UUID.randomUUID());
        clientUser1.startConsumingLiveCommands(liveCommand -> {
            LOGGER.info("clientUser1 got liveCommand <{}>", liveCommand);

            assertThat(liveCommand).isInstanceOf(ModifyAttribute.class);
            assertThat(liveCommand.getDittoHeaders().get(customHeaderName)).isEqualTo(customHeaderValue1);
            final ModifyAttribute receivedModifyAttribute = (ModifyAttribute) liveCommand;
            final ModifyAttributeResponse modifyAttributeResponse =
                    ModifyAttributeResponse.created(((ModifyAttribute) liveCommand).getEntityId(),
                            receivedModifyAttribute.getAttributePointer(),
                            receivedModifyAttribute.getAttributeValue(),
                            receivedModifyAttribute.getDittoHeaders()
                                    .toBuilder()
                                    .putHeader(customHeaderName, customHeaderValue2)
                                    .putHeader("multiplier", String.valueOf(multiplier))
                                    .build());
            // WebSocket client responds with a response to the live command
            clientUser1.send(modifyAttributeResponse);

            LOGGER.info("clientUser1 sent liveCommandResponse <{}>", modifyAttributeResponse);
            websocketLatch.countDown();
        }).get(15, TimeUnit.SECONDS);

        // When
        final String liveCommandCorrelationId = UUID.randomUUID().toString();
        final C responseConsumer = initResponseConsumer(cf.connectionNameWithPayloadMapping, liveCommandCorrelationId);
        final ModifyAttribute liveCommand =
                ModifyAttribute.of(thingId, JsonPointer.of("live-foo"), JsonValue.of("bar-is-all-we-need"),
                        DittoHeaders.newBuilder()
                                .correlationId(liveCommandCorrelationId)
                                .schemaVersion(JsonSchemaVersion.V_2)
                                .channel(TopicPath.Channel.LIVE.getName())
                                .putHeader(customHeaderName, customHeaderValue1)
                                .putHeader("keep-as-is", "true")
                                .build());
        // send live command via connectivity:
        sendSignal(cf.connectionNameWithPayloadMapping, liveCommand);

        // Then
        // expect response to the live command:
        final JsonObject expectedResponse = JsonObject.of("{\"namespace\":\"" + RANDOM_NAMESPACE + "\"," +
                "\"deviceid\":\"" + thingId.getName() +
                "\",\"action\":\"modify\",\"path\":\"/attributes/live-foo\",\"value\":\"bar-is-all-we-need\"}");
        for (int i = 0; i < multiplier; i++) {
            consumeResponseInFuture(liveCommandCorrelationId, responseConsumer)
                    .thenAccept(response -> {
                        LOGGER.info("Connectivity got response <{}>", response);
                        if (response == null) {
                            final String detailMsg = "Response was not received in expected time!";
                            LOGGER.error(detailMsg);
                            throw new AssertionError(detailMsg);
                        }
                        try {
                            final JsonObject receivedJsonObject = JsonObject.of(textFrom(response));
                            assertThat(receivedJsonObject).isEqualTo(expectedResponse);
                            connectivityLatch.countDown();
                        } catch (final Throwable e) {
                            LOGGER.error("Got unexpected Throwable {}: {}", e.getClass().getName(), e.getMessage(), e);
                        }
                    });
        }

        assertThat(websocketLatch.await(5, TimeUnit.SECONDS)).describedAs("websocketLatch").isTrue();
        assertThat(connectivityLatch.await(5, TimeUnit.SECONDS)).describedAs("connectivityLatch").isTrue();
    }

    @Test
    @Category(RequireSource.class)
    @Connections(ConnectionCategory.CONNECTION_WITH_PAYLOAD_MAPPING)
    public void dittoClientAnswersToLiveMessageCommandAndEnsurePayloadMappingWorks() throws Exception {

        // Given
        final int multiplier = 2;

        final ThingId thingId = generateThingId();

        final SubjectId connectivitySubjectId = SubjectId.newInstance(SubjectIssuer.INTEGRATION,
                SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution().getUsername() + ":" + cf.connectionNameWithPayloadMapping);
        final Subject connectivitySubject = Subject.newInstance(connectivitySubjectId);

        final Policy policy = Policy.newBuilder(PolicyId.of(thingId))
                .forLabel("OWNER")
                .setSubject(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getDefaultSubject())
                .setSubject(connectivitySubject)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"),
                        READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"),
                        READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"),
                        READ, WRITE)
                .forLabel("DEVICE")
                .build();

        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();

        putThingWithPolicy(2, thing, policy, JsonSchemaVersion.V_2)
                .withJWT(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final CountDownLatch websocketLatch = new CountDownLatch(1);
        final CountDownLatch connectivityLatch = new CountDownLatch(multiplier);

        // check preservation of custom headers
        final String customHeaderName = "send-websocket-live-command-answers-with-live-response-header-name";
        final String customHeaderValue1 = "send-websocket-live-command-answers-with-live-response-header-value1";
        final String customHeaderValue2 = "send-websocket-live-command-answers-with-live-response-header-value2";

        // WebSocket client consumes live commands:

        final DittoClient client =
                getDittoClient(thingsWsUrl(TestConstants.API_V_2),
                        SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getAccessToken());
        final String messageSubject = UUID.randomUUID().toString();
        final String registrationId = "registration" + UUID.randomUUID();
        client.live().startConsumption().toCompletableFuture().get(15, TimeUnit.SECONDS);
        client.live().registerForMessage(registrationId, messageSubject, message -> {
            message.reply()
                    .headers(message.getHeaders().toBuilder()
                            .putHeader(customHeaderName, customHeaderValue2)
                            .putHeader("multiplier", String.valueOf(multiplier))
                            .build())
                    .httpStatus(HttpStatus.IM_A_TEAPOT)
                    .payload("Hello Teapot!")
                    .send();
            LOGGER.info("clientUser1 sent liveMessageResponse.");
            websocketLatch.countDown();
        });

        // When
        final String liveCommandCorrelationId = UUID.randomUUID().toString();
        final C responseConsumer = initResponseConsumer(cf.connectionNameWithPayloadMapping, liveCommandCorrelationId);
        final Message<Object> message = Message.newBuilder(
                MessageBuilder.newHeadersBuilder(MessageDirection.TO, thingId, messageSubject)
                        .schemaVersion(JsonSchemaVersion.V_2)
                        .channel(TopicPath.Channel.LIVE.getName())
                        .correlationId(liveCommandCorrelationId)
                        .contentType("text/plain")
                        .putHeader(customHeaderName, customHeaderValue1)
                        .putHeader("keep-as-is", "true")
                        .build())
                .payload("Hello you!")
                .build();

        final DittoHeaders headers = DittoHeaders.newBuilder()
                .correlationId(liveCommandCorrelationId)
                .build();

        final SendThingMessage<Object> sendThingMessage = SendThingMessage.of(thingId, message, headers);
        // send live command via connectivity:
        sendSignal(cf.connectionNameWithPayloadMapping, sendThingMessage);

        // Then
        // expect response to the live command:
        final JsonObject expectedResponse = JsonObject.newBuilder()
                .set("namespace", RANDOM_NAMESPACE)
                .set("deviceid", thingId.getName())
                .set("action", messageSubject)
                .set("path", "/inbox/messages/" + messageSubject)
                .set("value", "Hello Teapot!")
                .build();

        for (int i = 0; i < multiplier; i++) {
            consumeResponseInFuture(liveCommandCorrelationId, responseConsumer)
                    .thenAccept(response -> {
                        LOGGER.info("Connectivity got response <{}>", response);
                        if (response == null) {
                            final String detailMsg = "Response was not received in expected time!";
                            LOGGER.error(detailMsg);
                            throw new AssertionError(detailMsg);
                        }
                        try {
                            final JsonObject receivedJsonObject = JsonObject.of(textFrom(response));
                            assertThat(receivedJsonObject).isEqualTo(expectedResponse);
                            connectivityLatch.countDown();
                        } catch (final Throwable e) {
                            LOGGER.error("Got unexpected Throwable {}: {}", e.getClass().getName(), e.getMessage(), e);
                        }
                    });
        }

        assertThat(websocketLatch.await(5, TimeUnit.SECONDS)).describedAs("websocketLatch").isTrue();
        assertThat(connectivityLatch.await(5, TimeUnit.SECONDS)).describedAs("connectivityLatch").isTrue();
    }

    private static DittoClient getDittoClient(final String url, final String bearerToken) {
        final MessagingConfiguration.Builder builder = WebSocketMessagingConfiguration.newBuilder()
                .endpoint(url)
                .jsonSchemaVersion(JsonSchemaVersion.V_2)
                .reconnectEnabled(false);

        getHttpProxyUrl().ifPresent(httpProxyUrl -> {
            final String proxyHost = httpProxyUrl.getHost();
            final int proxyPort = httpProxyUrl.getPort();

            final ProxyConfiguration proxyConfiguration = ProxyConfiguration.newBuilder()
                    .proxyHost(proxyHost)
                    .proxyPort(proxyPort)
                    .build();
            builder.proxyConfiguration(proxyConfiguration);
        });

        final AuthenticationProvider<WebSocket> authenticationProvider =
                AuthenticationProviders.accessToken(AccessTokenAuthenticationConfiguration.newBuilder()
                        .identifier("someId")
                        .accessTokenSupplier(() -> ImmutableJsonWebToken.fromToken(bearerToken))
                        .build());

        return DittoClients.newInstance(MessagingProviders.webSocket(builder.build(), authenticationProvider))
                .connect()
                .toCompletableFuture()
                .join();
    }

    private static Optional<URL> getHttpProxyUrl() {
        final String httpProxy = System.getenv("HTTP_PROXY");
        if (httpProxy != null && !httpProxy.isEmpty()) {
            try {
                return Optional.of(new URL(httpProxy));
            } catch (MalformedURLException e) {
                throw new IllegalStateException("Malformed HTTP_PROXY url: " + httpProxy);
            }
        }
        return Optional.empty();
    }

    @Test
    @Category(RequireSource.class)
    @Connections(ConnectionCategory.CONNECTION_WITH_PAYLOAD_MAPPING)
    public void sendCustomJsonToBeMappedByJavaScriptMapping() {
        sendCustomJsonToBeMappedByJavaScriptMapping(1, cf.connectionNameWithPayloadMapping, "");
    }

    @Test
    @Category(RequireSource.class)
    @Connections(CONNECTION_WITH_MULTIPLE_PAYLOAD_MAPPINGS)
    public void sendCustomJsonToBeMappedMultipleTimesByJavaScriptMapping() {
        sendCustomJsonToBeMappedByJavaScriptMapping(3, cf.connectionNameWithMultiplePayloadMappings,
                ConnectionModelFactory.JS_SOURCE_SUFFIX);
    }

    @Test
    @Connections(CONNECTION_WITH_MULTIPLE_PAYLOAD_MAPPINGS)
    public void modifyThingViaHttpAndExpectMultipleMappedThingEvents() {
        final int multiplierValue = 3;
        final String cid = UUID.randomUUID().toString();
        final ThingId thingId = generateThingId();
        final Policy policy =
                createNewPolicy(thingId, name.getMethodName(), cf.connectionNameWithMultiplePayloadMappings);
        final Thing thing = Thing.newBuilder().setId(thingId).setPolicyId(PolicyId.of(thingId)).build();


        final C consumer = initTargetsConsumer(
                cf.connectionNameWithMultiplePayloadMappings + ConnectionModelFactory.JS_SOURCE_SUFFIX);

        putPolicy(thingId.toString(), policy)
                .withJWT(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .withJWT(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getAccessToken())
                .withHeader("multiplier", multiplierValue)
                .withHeader(HttpHeader.X_CORRELATION_ID, cid)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        for (int i = 0; i < multiplierValue; i++) {
            LOGGER.info("Expecting message #" + i);
            final M message =
                    consumeFromTarget(
                            cf.connectionNameWithMultiplePayloadMappings + ConnectionModelFactory.JS_SOURCE_SUFFIX,
                            consumer);

            assertThat(message).describedAs("Message #" + i).isNotNull();

            final JsonObject expectedJson = getJsonProducedByJSMapping(thingId.getName(), thingId.getNamespace(), thing,
                    "created");
            assertThat(textFrom(message)).isEqualTo(expectedJson.toString());

            if (protocolSupportsHeaders()) {
                final Map<String, String> headers = checkHeaders(message);
                assertThat(headers).containsEntry("index", String.valueOf(i));
                assertThat(headers).containsEntry(DittoHeaderDefinition.CORRELATION_ID.getKey(), cid);
            }
        }
    }

    @Test
    @Category(RequireSource.class)
    @Connections(CONNECTION_WITH_MULTIPLE_PAYLOAD_MAPPINGS)
    public void javascriptMappingProducesMergeUpdate() {
        // prepare a thing that should be merge updated
        final ThingId thingId = generateThingId();
        final Policy policy =
                createNewPolicy(thingId, name.getMethodName(), cf.connectionNameWithMultiplePayloadMappings);
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(PolicyId.of(thingId))
                .setFeatureProperty("feature1", JsonPointer.of("pi"), JsonValue.of(Math.PI))
                .setFeatureProperty("feature4", JsonPointer.of("e"), JsonValue.of(Math.E))
                .build();

        putPolicy(thingId.toString(), policy)
                .withJWT(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .withJWT(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getAccessToken())
                .withHeader(HttpHeader.X_CORRELATION_ID, UUID.randomUUID().toString())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        // build and send a custom json message with values for multiple properties of different features
        final String mergeCorrelationId = UUID.randomUUID().toString();
        final C consumer = initResponseConsumer(cf.connectionNameWithMultiplePayloadMappings, mergeCorrelationId);

        // the _merge source uses a javascript payload mapping, that maps the given values to property values of
        // different features
        final String connectionName =
                cf.connectionNameWithMultiplePayloadMappings + ConnectionModelFactory.MERGE_SOURCE_SUFFIX;
        final int value1 = RANDOM.nextInt(10000);
        final int value2 = RANDOM.nextInt(10000);
        final int value3 = RANDOM.nextInt(10000);
        final JsonObject payload = JsonObject.newBuilder()
                .set("correlation-id", mergeCorrelationId)
                .set("reply-to", mergeCorrelationId)
                .set("namespace", thingId.getNamespace())
                .set("name", thingId.getName())
                .set("v1", value1) // --> maps to features/feature1/properties/value1
                .set("v2", value2) // --> maps to features/feature2/properties/value2
                .set("v3", value3) // --> maps to features/feature3/properties/value3
                .build();
        final DittoHeaders headers = DittoHeaders.newBuilder().correlationId(mergeCorrelationId).build();
        sendAsJsonString(connectionName, mergeCorrelationId, payload.toString(), headers);

        // expect 204 - no content response
        final M response = consumeResponse(mergeCorrelationId, consumer);
        assertThat(response).isNotNull();
        final JsonValue responseJson = JsonFactory.readFrom(textFrom(response));
        LOGGER.debug("Response received: {}", responseJson);
        assertThat(Optional.of(responseJson)
                .map(JsonValue::asObject)
                .flatMap(o -> o.getValue("status"))).contains(JsonValue.of(HttpStatus.NO_CONTENT.getCode()));

        // verify that all values were merged successfully
        Awaitility.await().untilAsserted(() -> {
            final Response retrieve = getThing(2, thingId)
                    .withJWT(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getAccessToken())
                    .expectingHttpStatus(HttpStatus.OK)
                    .fire();
            final Thing mergedThing = ThingsModelFactory.newThing(retrieve.asString());
            LOGGER.debug("Thing after merge: {}", mergedThing.toJsonString());
            assertPropertyValue(mergedThing, "feature1", "value1", JsonValue.of(value1));
            assertPropertyValue(mergedThing, "feature1", "pi", JsonValue.of(Math.PI));
            assertPropertyValue(mergedThing, "feature2", "value2", JsonValue.of(value2));
            assertPropertyValue(mergedThing, "feature3", "value3", JsonValue.of(value3));
            assertPropertyValue(mergedThing, "feature4", "e", JsonValue.of(Math.E));
        });
    }

    private static void assertPropertyValue(final Thing thing, final String featureId, final String property,
            final JsonValue value) {
        assertThat(thing.getFeatures()
                .flatMap(features -> features.getFeature(featureId).flatMap(f -> f.getProperty(property))))
                .contains(value);
    }

    private void sendCustomJsonToBeMappedByJavaScriptMapping(final int multiplier, final String connectionName,
            final String sourceSuffix) {
        sendCustomJsonToBeMappedByJavaScriptMapping(multiplier, connectionName, sourceSuffix, null);
    }

    private void sendCustomJsonToBeMappedByJavaScriptMapping(final int multiplier,
            final String connectionName,
            final String sourceSuffix,
            @Nullable final Consumer<JsonifiableAdaptable> responseVerifier) {

        // Given
        final String deviceId = name.getMethodName() + ":" + UUID.randomUUID();
        final String namespace = RANDOM_NAMESPACE;
        final ThingId thingId = ThingId.of(namespace, deviceId);

        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setAttribute(JsonPointer.of("hello"), JsonValue.of("World!"))
                .build();

        final JsonObject customJson = getJsonProducedByJSMapping(deviceId, namespace, thing, "create");

        final String correlationId = UUID.randomUUID().toString();
        final Map<String, String> headers = new HashMap<>();
        headers.put("reply-to", correlationId);
        headers.put("content-type", "application/json");
        final String customHeaderKey = "bumlux-header";
        final String customHeaderValue = "I am the rul0r of the world!";
        headers.put(customHeaderKey, customHeaderValue);
        headers.put("multiplier", String.valueOf(multiplier));

        // When
        final C consumer = initResponseConsumer(connectionName, correlationId);
        sendAsJsonString(connectionName + sourceSuffix, correlationId, customJson.toString(), headers);

        if (responseVerifier != null) {
            final M message = consumeResponse(correlationId, consumer);
            assertThat(message).isNotNull();
            final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(message);
            responseVerifier.accept(jsonifiableAdaptable);
        } else {
            // Then
            final List<M> receivedMessages = new ArrayList<>();
            for (int i = 0; i < multiplier; i++) {
                final M message = consumeResponse(correlationId, consumer);
                assertThat(message).isNotNull();
                receivedMessages.add(message);
            }

            assertThat(receivedMessages).hasSize(multiplier);

            final List<String> suffixes;
            if (multiplier > 1) {
                suffixes = IntStream.range(0, multiplier).mapToObj(i -> "-" + i).collect(Collectors.toList());
            } else {
                suffixes = Collections.singletonList("");
            }

            for (final M receivedMessage : receivedMessages) {
                final List<Throwable> messageErrors = new ArrayList<>();
                for (final String suffix : suffixes) {
                    try {
                        verifyReceivedMessageFromJavascriptMapping(receivedMessage, namespace, deviceId, thing,
                                customHeaderKey, customHeaderValue, correlationId, suffix);
                        messageErrors.clear();
                        break;
                    } catch (final AssertionError e) {
                        messageErrors.add(e);
                    }
                }
                if (!messageErrors.isEmpty()) {
                    throw new SoftAssertionError(Throwables.describeErrors(messageErrors));
                }
            }
        }
    }

    private void verifyReceivedMessageFromJavascriptMapping(final M message, final String namespace,
            final String deviceId,
            final Thing thing, final String customHeaderKey, final String customHeaderValue,
            final String correlationId, final String idSuffix) {

        final ThingId thingIdWithDiscriminator = ThingId.of(namespace, deviceId + idSuffix);
        final Thing thingWithModifiedId = thing.toBuilder()
                .setId(thingIdWithDiscriminator)
                .setPolicyId(PolicyId.of(thingIdWithDiscriminator))
                .build();
        final JsonObject expectedMappedResponse =
                getJsonProducedByJSMapping(deviceId + idSuffix, namespace, thingWithModifiedId, "create");

        final JsonValue responseMessage = JsonFactory.readFrom(textFrom(message));

        DittoJsonAssertions.assertThat(responseMessage).isInstanceOf(JsonObject.class);
        assertThat(responseMessage.toString()).isEqualTo(expectedMappedResponse.toString());

        // only check headers if protocol supports headers.
        // otherwise headers from javascript mapper cannot be recovered.
        if (protocolSupportsHeaders()) {
            final Map<String, String> responseHeaders = checkHeaders(message);

            // when not specifically retained in header mapping, these should be "lost"
            assertThat(responseHeaders).doesNotContainKey(customHeaderKey);

            // retained due to payload mapping
            assertThat(responseHeaders).containsEntry("new-header", deviceId + idSuffix);
            assertThat(responseHeaders).containsEntry("correlation-id", correlationId);
        }
    }

    @Test
    @Category({RequireSource.class, RequireProtocolHeaders.class})
    @Connections({CONNECTION_WITH_MULTIPLE_PAYLOAD_MAPPINGS, CONNECTION1})
    public void sendFeatureUpdateWithHonoTtdHeaderExpectConnectionStatusFeatureUpdated() {
        // Given
        final ThingId thingId = generateThingId();
        final String featureId = name.getMethodName();

        // create a new thing with correct policy
        createNewThingWithPolicy(thingId, featureId, cf.connectionName1, cf.connectionNameWithMultiplePayloadMappings);

        // modify feature via source that has ditto + status mapper
        sendMessageWithTtdHeaderAndVerifyFeature(thingId, featureId, Matchers.is(revValue(3)), 30,
                cf.connectionNameWithMultiplePayloadMappings + ConnectionModelFactory.DITTO_STATUS_SOURCE_SUFFIX);

        // modify feature via source that has only status mapper, expect only one update (rev=4)
        sendMessageWithTtdHeaderAndVerifyFeature(thingId, featureId, Matchers.is(revValue(4)), 60,
                cf.connectionNameWithMultiplePayloadMappings + ConnectionModelFactory.STATUS_SOURCE_SUFFIX);
    }

    private void sendMessageWithTtdHeaderAndVerifyFeature(final ThingId thingId, final String featureId,
            final Matcher<String> etagMatcher, final long ttd, final String destination) {
        final DittoHeaders moreHeaders = DittoHeaders.newBuilder()
                .correlationId(UUID.randomUUID().toString())
                // the mapping is configured to read thingId from device_id header
                .putHeader("device_id", thingId)
                // add ttd header as hub would do
                .putHeader("ttd", String.valueOf(ttd))
                .putHeader("creation-time", String.valueOf(System.currentTimeMillis()))
                .build();

        final Signal<ModifyFeature> anotherModifyFeature = ModifyFeature.of(thingId,
                ThingsModelFactory.newFeature(featureId), moreHeaders);

        final String correlationId = anotherModifyFeature.getDittoHeaders().getCorrelationId().get();
        final Adaptable adaptable = PROTOCOL_ADAPTER.toAdaptable(anotherModifyFeature);
        final String stringMessage = ProtocolFactory.wrapAsJsonifiableAdaptable(adaptable).toJsonString();
        final DittoHeaders headersMap = adaptable.getDittoHeaders().toBuilder().putHeaders(moreHeaders).build();

        sendAsJsonString(destination, correlationId, stringMessage, headersMap);

        final Response response1 = getThing(2, thingId)
                .withJWT(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(Matchers.containsString(CONNECTION_STATUS_FEATURE))
                .expectingHeader("etag", etagMatcher)
                .useAwaitility(Awaitility.await().atMost(Duration.ofSeconds(5)))
                .withLogging(LOGGER, "thing")
                .fire();

        final Thing thing1 = ThingsModelFactory.newThing(response1.getBody().asString());

        verifyConnectionStatusFeature(thing1, ttd);
    }

    @Test
    @Category(RequireProtocolHeaders.class)
    @Connections(CONNECTION_WITH_CONNECTION_ANNOUNCEMENTS)
    public void appliesPayloadMappingOnConnectionAnnouncements() {
        final String connectionName = cf.connectionWithConnectionAnnouncements;
        final String connectionId = cf.getConnectionId(connectionName).toString();
        final C consumer = initTargetsConsumer(connectionName);

        // some connection types will still receive the opened announcement after initializing the targets consumer,
        // some targets will already have received it before initializing the target in this test. Therefore they
        // either receive an opened announcement or nothing
        consumeFromTarget(connectionName, consumer);

        try {
            connectionsClient().closeConnection(connectionId)
                    .withDevopsAuth()
                    .expectingHttpStatus(HttpStatus.OK)
                    .fire();

            final M mappedMessage = consumeFromTarget(connectionName, consumer);

            final Map<String, String> headers = checkHeaders(mappedMessage);
            assertThat(headers).containsEntry("new-header-from-payload-mapping", connectionId);

            final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(mappedMessage);
            final Adaptable eventAdaptable = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
            final Jsonifiable<JsonObject> event = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable);
            assertThat(event).isInstanceOf(ConnectionClosedAnnouncement.class);
        } finally {
            connectionsClient().openConnection(cf.getConnectionId(connectionName).toString())
                    .withDevopsAuth()
                    .expectingHttpStatus(HttpStatus.OK)
                    .fire();
        }
    }

    private void verifyConnectionStatusFeature(final Thing thing, final long ttd) {

        DittoThingsAssertions.assertThat(thing).hasFeatureWithId(CONNECTION_STATUS_FEATURE);

        final Feature connectionStatusFeature = thing.getFeatures()
                .flatMap(f -> f.getFeature(CONNECTION_STATUS_FEATURE))
                .orElseThrow(notFoundException("feature " + CONNECTION_STATUS_FEATURE));

        final Instant readySince = connectionStatusFeature
                .getProperty(STATUS_PROPERTY)
                .filter(JsonValue::isObject)
                .map(JsonValue::asObject)
                .flatMap(jo -> jo.getValue(READY_SINCE_PROPERTY))
                .map(JsonValue::asString)
                .map(Instant::parse)
                .orElseThrow(notFoundException("property " + READY_SINCE_PROPERTY));
        final Instant readyUntil = connectionStatusFeature
                .getProperty(STATUS_PROPERTY)
                .filter(JsonValue::isObject)
                .map(JsonValue::asObject)
                .flatMap(jo -> jo.getValue(READY_UNTIL_PROPERTY))
                .map(JsonValue::asString)
                .map(Instant::parse)
                .orElseThrow(notFoundException("property " + READY_UNTIL_PROPERTY));

        assertThat(readySince).isBefore(readyUntil);

        assertThat(readyUntil.toEpochMilli() - readySince.toEpochMilli()).isEqualTo(ttd * 1000);
    }

    private Supplier<IllegalStateException> notFoundException(final String text) {
        return () -> new IllegalStateException(text + " not found");
    }

    private String revValue(final int rev) {
        return "\"rev:" + rev + "\"";
    }

    public static JsonObject getJsonProducedByJSMapping(final String deviceId, final String namespace,
            final Thing thing,
            final String action) {

        return JsonObject.newBuilder()
                .set("namespace", namespace)
                .set("deviceid", deviceId)
                .set("action", action)
                .set("path", "/")
                .set("value", thing.toJson())
                .build();
    }
}
