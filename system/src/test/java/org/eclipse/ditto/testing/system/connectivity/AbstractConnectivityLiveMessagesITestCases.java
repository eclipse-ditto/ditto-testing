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
import static org.eclipse.ditto.messages.model.MessageDirection.TO;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION1;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION_WITH_RAW_MESSAGE_MAPPER_1;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION_WITH_RAW_MESSAGE_MAPPER_2;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.commands.CommandResponse;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.FilteredTopic;
import org.eclipse.ditto.connectivity.model.ReplyTarget;
import org.eclipse.ditto.connectivity.model.Source;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.connectivity.model.Topic;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.messages.model.Message;
import org.eclipse.ditto.messages.model.MessageDirection;
import org.eclipse.ditto.messages.model.MessageHeaders;
import org.eclipse.ditto.messages.model.MessagesModelFactory;
import org.eclipse.ditto.messages.model.signals.commands.SendThingMessage;
import org.eclipse.ditto.messages.model.signals.commands.SendThingMessageResponse;
import org.eclipse.ditto.policies.api.Permission;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectId;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.TopicPath;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.categories.RequireProtocolHeaders;
import org.eclipse.ditto.testing.common.categories.RequireSource;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingFieldSelector;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttribute;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttributeResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThingResponse;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.http.ContentType;
import io.restassured.response.Response;

public abstract class AbstractConnectivityLiveMessagesITestCases<C, M>
        extends AbstractConnectivityCommandOrderITestCases<C, M> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnectivityLiveMessagesITestCases.class);

    private static final String FILTER_MESSAGES_BY_RQL = "FILTER_MESSAGE_BY_RQL";
    private static final String WITH_REPLY_TARGET = "WITH_REPLY_TARGET";

    static {
        // adds filtering for live messages with enrichment of "attributes" as extra fields +
        // an RQL limiting the received messages to subject "allowedSubject" and having attribute awesome=true
        addMod(FILTER_MESSAGES_BY_RQL, connection -> {
                    final List<Target> adjustedTargets =
                            connection.getTargets().stream()
                                    .map(target -> {
                                        final Set<FilteredTopic> topics = new HashSet<>();
                                        // only subscribe for live messages
                                        topics.add(ConnectivityModelFactory
                                                .newFilteredTopicBuilder(Topic.LIVE_MESSAGES)
                                                .withExtraFields(ThingFieldSelector.fromString("attributes"))
                                                .withFilter("and(" +
                                                        "eq(topic:subject,'allowed-subject')," +
                                                        "eq(attributes/awesome,true)" +
                                                        ")")
                                                .build()
                                        );
                                        return ConnectivityModelFactory.newTargetBuilder(target)
                                                .topics(topics)
                                                .build();
                                    })
                                    .collect(Collectors.toList());
                    return connection.toBuilder()
                            .setTargets(adjustedTargets)
                            .build();
                }
        );

        // adds a "replyTarget" for the connection's source
        addMod(WITH_REPLY_TARGET, connection -> {
                    final List<Source> adjustedSource =
                            connection.getSources().stream()
                                    .map(source -> ConnectivityModelFactory.newSourceBuilder(source)
                                            .replyTarget(ReplyTarget.newBuilder()
                                                    .address("{{ header:correlation-id }}")
                                                    .headerMapping(
                                                            ConnectionModelFactory.getHeaderMappingForReplyTarget())
                                                    .build())
                                            .build())
                                    .collect(Collectors.toList());
                    return connection.toBuilder()
                            .setSources(adjustedSource)
                            .build();
                }
        );
    }

    private ConnectivityTestWebsocketClient webSocketClient;

    protected AbstractConnectivityLiveMessagesITestCases(final ConnectivityFactory cf) {
        super(cf);
    }

    @Before
    public void setupWsClient() {
        webSocketClient = initiateWebSocketClient(serviceEnv.getDefaultTestingContext().getOAuthClient());
    }

    @After
    public void cleanUpWsClient() {
        webSocketClient.disconnect();
    }

    @Test
    @Category(RequireSource.class)
    @Connections(ConnectionCategory.CONNECTION1)
    public void sendWebsocketLiveCommandAnswersWithLiveResponse() throws Exception {
        // Given
        final String connectionName = cf.connectionName1;

        final ThingId thingId =
                createNewThingWithAccessForUserAndConnection(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient(),
                        connectionName);

        final CountDownLatch websocketLatch = new CountDownLatch(1);
        final CountDownLatch connectivityLatch = new CountDownLatch(1);

        // check preservation of custom headers
        final String customHeaderName = "send-websocket-live-command-answers-with-live-response-header-name";
        final String customHeaderValue1 = "send-websocket-live-command-answers-with-live-response-header-value1";
        final String customHeaderValue2 = "send-websocket-live-command-answers-with-live-response-header-value2";

        // WebSocket client consumes live commands:
        final ConnectivityTestWebsocketClient clientUser1 =
                ConnectivityTestWebsocketClient.newInstance(dittoWsUrl(TestConstants.API_V_2),
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
                                    .build());
            // WebSocket client responds with a response to the live command
            clientUser1.send(modifyAttributeResponse);

            LOGGER.info("clientUser1 sent liveCommandResponse <{}>", modifyAttributeResponse);
            websocketLatch.countDown();
        }).get(15, TimeUnit.SECONDS);

        // When
        final String liveCommandCorrelationId = UUID.randomUUID().toString();
        final C responseConsumer = initResponseConsumer(connectionName, liveCommandCorrelationId);
        final ModifyAttribute liveCommand =
                ModifyAttribute.of(thingId, JsonPointer.of("live-foo"), JsonValue.of("bar-is-all-we-need"),
                        DittoHeaders.newBuilder()
                                .correlationId(liveCommandCorrelationId)
                                .schemaVersion(JsonSchemaVersion.V_2)
                                .channel(TopicPath.Channel.LIVE.getName())
                                .putHeader(customHeaderName, customHeaderValue1)
                                .build());
        // send live command via connectivity:
        sendSignal(connectionName, liveCommand);

        // Then
        // expect response to the live command:
        consumeResponseInFuture(liveCommandCorrelationId, responseConsumer)
                .thenAccept(response -> {
                    LOGGER.info("Connectivity got response <{}>", response);
                    if (response == null) {
                        final String detailMsg = "Response was not received in expected time!";
                        LOGGER.error(detailMsg);
                        throw new AssertionError(detailMsg);
                    }
                    try {
                        final Signal<?> signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(response));
                        assertThat(signal).isInstanceOf(CommandResponse.class);
                        assertThat(signal).isInstanceOf(ModifyAttributeResponse.class);
                        final ModifyAttributeResponse modifyAttributeResponse = (ModifyAttributeResponse) signal;
                        assertThat(modifyAttributeResponse.getHttpStatus()).isEqualTo(HttpStatus.CREATED);

                        connectivityLatch.countDown();
                    } catch (final Throwable e) {
                        LOGGER.error("Got unexpected Throwable {}: {}", e.getClass().getName(), e.getMessage(), e);
                    }
                });

        assertThat(websocketLatch.await(5, TimeUnit.SECONDS)).describedAs("websocketLatch").isTrue();
        assertThat(connectivityLatch.await(5, TimeUnit.SECONDS)).describedAs("connectivityLatch").isTrue();
    }

    @Test
    @Category({RequireSource.class, RequireProtocolHeaders.class})
    @UseConnection(category = ConnectionCategory.CONNECTION1, mod = WITH_REPLY_TARGET)
    public void sendWebsocketLiveChannelConditionCommandAnswersWithLiveResponse() throws Exception {
        // Given
        final var connectionName = cf.connectionName1;

        final var thingId =
                createNewThingWithAccessForUserAndConnection(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient(),
                        connectionName);

        final var websocketLatch = new CountDownLatch(1);
        final var connectivityLatch = new CountDownLatch(1);

        // WebSocket client consumes live commands:
        final var clientUser1 =
                ConnectivityTestWebsocketClient.newInstance(dittoWsUrl(TestConstants.API_V_2),
                        SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getAccessToken());
        clientUser1.connect("sendWebsocketLiveChannelConditionCommandAnswersWithLiveResponse-" + UUID.randomUUID());
        clientUser1.startConsumingLiveCommands(liveCommand -> {
            LOGGER.info("clientUser1 got liveCommand <{}>", liveCommand);

            assertThat(liveCommand).isInstanceOf(RetrieveThing.class);
            final var receivedRetrieveThing = (RetrieveThing) liveCommand;
            final var retrieveThingResponse =
                    RetrieveThingResponse.of(receivedRetrieveThing.getEntityId(),
                            Thing.newBuilder()
                                    .setAttribute(JsonPointer.of("life"), JsonValue.of("is live"))
                                    .setId(receivedRetrieveThing.getEntityId())
                                    .build(),
                            null,
                            null,
                            receivedRetrieveThing.getDittoHeaders()
                                    .toBuilder()
                                    .build());
            // WebSocket client responds with a response to the live command
            clientUser1.send(retrieveThingResponse);

            LOGGER.info("clientUser1 sent liveCommandResponse <{}>", retrieveThingResponse);
            websocketLatch.countDown();
        }).get(15, TimeUnit.SECONDS);

        // When
        final var liveCommandCorrelationId = UUID.randomUUID().toString();
        final var responseConsumer = initResponseConsumer(connectionName, liveCommandCorrelationId);
        final var commandWithChannelCondition =
                RetrieveThing.of(thingId, DittoHeaders.newBuilder()
                        .correlationId(liveCommandCorrelationId)
                        .schemaVersion(JsonSchemaVersion.V_2)
                        .liveChannelCondition(String.format("eq(thingId,'%s')", thingId))
                        .build());
        // send command with live-channel-condition via connectivity:
        sendSignal(connectionName, commandWithChannelCondition);

        // Then
        // expect response command to be live and that the condition matched
        consumeResponseInFuture(liveCommandCorrelationId, responseConsumer)
                .thenAccept(response -> {
                    LOGGER.info("Connectivity got response <{}>", response);
                    if (response == null) {
                        final var detailMsg = "Response was not received in expected time!";
                        LOGGER.error(detailMsg);
                        throw new AssertionError(detailMsg);
                    }
                    try {
                        final var signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(response));

                        assertThat(signal).isInstanceOf(CommandResponse.class);
                        assertThat(signal).isInstanceOf(RetrieveThingResponse.class);

                        final var retrieveThingResponse = (RetrieveThingResponse) signal;
                        final var dittoHeaders = retrieveThingResponse.getDittoHeaders();

                        assertThat(retrieveThingResponse.getHttpStatus()).isEqualTo(HttpStatus.OK);
                        assertThat(dittoHeaders.getChannel()).hasValue("live");
                        assertThat(dittoHeaders.didLiveChannelConditionMatch()).isTrue();

                        connectivityLatch.countDown();
                    } catch (final Throwable e) {
                        LOGGER.error("Got unexpected Throwable {}: {}", e.getClass().getName(), e.getMessage(), e);
                    }
                });

        assertThat(websocketLatch.await(5, TimeUnit.SECONDS)).describedAs("websocketLatch").isTrue();
        assertThat(connectivityLatch.await(11, TimeUnit.SECONDS)).describedAs("connectivityLatch").isTrue();
    }

    @Test
    @Category(RequireSource.class)
    @Connections(ConnectionCategory.CONNECTION1)
    public void canReceiveResponseWithoutRequestingAcknowledgement() throws Exception {
        // Given
        final String connectionName = cf.connectionName1;
        final ThingId thingId =
                createNewThingWithAccessForUserAndConnection(serviceEnv.getDefaultTestingContext().getOAuthClient(),
                        connectionName);

        final CountDownLatch websocketLatch = new CountDownLatch(1);
        final CountDownLatch connectivityLatch = new CountDownLatch(1);
        final String messageSubject = UUID.randomUUID().toString();

        // WebSocket client consumes live commands:
        webSocketClient.connect("canReceiveResponseWithoutRequestingAcknowledgement-" + UUID.randomUUID());
        webSocketClient.startConsumingMessages(liveMessage -> {
            LOGGER.info("webSocketClient got liveMessage <{}>", liveMessage);

            assertThat(liveMessage).isInstanceOf(SendThingMessage.class);
            final SendThingMessage<?> liveMessageObj = (SendThingMessage<?>) liveMessage;

            final MessageHeaders messageHeaders = MessageHeaders
                    .newBuilder(MessageDirection.FROM, thingId, messageSubject)
                    .build();
            final Message<String> messageResponse =
                    MessagesModelFactory.<String>newMessageBuilder(messageHeaders)
                            .payload("Pong")
                            .build();
            final SendThingMessageResponse<String> liveResponse = SendThingMessageResponse.of(
                    liveMessageObj.getEntityId(),
                    messageResponse,
                    HttpStatus.OK,
                    liveMessageObj.getDittoHeaders()
            );
            // WebSocket client responds with a response to the live command
            webSocketClient.send(liveResponse);

            LOGGER.info("webSocketClient sent liveResponse <{}>", liveResponse);
            websocketLatch.countDown();
        }).get(15, TimeUnit.SECONDS);

        // When
        final String liveCommandCorrelationId = UUID.randomUUID().toString();
        final C responseConsumer = initResponseConsumer(connectionName, liveCommandCorrelationId);
        final DittoHeaders headers = DittoHeaders.newBuilder()
                .correlationId(liveCommandCorrelationId)
                .schemaVersion(JsonSchemaVersion.V_2)
                .channel(TopicPath.Channel.LIVE.getName())
                .responseRequired(true)
                .acknowledgementRequests(Collections.emptyList())
                .build();
        final MessageHeaders messageHeaders =
                MessageHeaders.newBuilder(MessageDirection.TO, thingId, messageSubject).build();
        final Message<String> message = MessagesModelFactory.<String>newMessageBuilder(messageHeaders)
                .payload("Ping")
                .build();
        final SendThingMessage<String> liveMessage = SendThingMessage.of(thingId, message, headers);
        // send live command via connectivity:
        sendSignal(connectionName, liveMessage);

        // Then
        // expect response to the live command:
        consumeResponseInFuture(liveCommandCorrelationId, responseConsumer)
                .thenAccept(response -> {
                    LOGGER.info("Connectivity got response <{}>", response);
                    if (response == null) {
                        final String detailMsg = "Response was not received in expected time!";
                        LOGGER.error(detailMsg);
                        throw new AssertionError(detailMsg);
                    }
                    try {
                        final Signal<?> signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(response));
                        assertThat(signal).isInstanceOf(SendThingMessageResponse.class);
                        final SendThingMessageResponse<?> liveResponse = (SendThingMessageResponse<?>) signal;
                        assertThat(liveResponse.getHttpStatus()).isEqualTo(HttpStatus.OK);

                        connectivityLatch.countDown();
                    } catch (final Throwable e) {
                        LOGGER.error("Got unexpected Throwable {}: {}", e.getClass().getName(), e.getMessage(), e);
                    }
                });

        assertThat(websocketLatch.await(5, TimeUnit.SECONDS)).describedAs("websocketLatch").isTrue();
        assertThat(connectivityLatch.await(15, TimeUnit.SECONDS)).describedAs("connectivityLatch").isTrue();
    }

    /**
     * This test should verify, that if response-required is set to false in the headers of the response, the response
     * gets still delivered.
     */
    @Test
    @Category(RequireSource.class)
    @Connections(ConnectionCategory.CONNECTION1)
    public void responseRequiredHeaderInResponseHasNoEffect() throws Exception {
        // Given
        final String connectionName = cf.connectionName1;
        final ThingId thingId =
                createNewThingWithAccessForUserAndConnection(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient(),
                        connectionName);

        final CountDownLatch websocketLatch = new CountDownLatch(1);
        final CountDownLatch connectivityLatch = new CountDownLatch(1);
        final String messageSubject = UUID.randomUUID().toString();

        // WebSocket client consumes live commands:
        final ConnectivityTestWebsocketClient webSocketClient =
                initiateWebSocketClient(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient());
        webSocketClient.connect("responseRequiredHeaderInResponseHasNoEffect-" + UUID.randomUUID());
        webSocketClient.startConsumingMessages(liveMessage -> {
            LOGGER.info("webSocketClient got liveMessage <{}>", liveMessage);

            assertThat(liveMessage).isInstanceOf(SendThingMessage.class);
            final SendThingMessage<?> liveMessageObj = (SendThingMessage<?>) liveMessage;
            final DittoHeaders headersWithResponseRequiredFalse = liveMessageObj.getDittoHeaders()
                    .toBuilder()
                    .responseRequired(false)
                    .build();
            final MessageHeaders messageHeaders = MessageHeaders
                    .newBuilder(MessageDirection.FROM, thingId, messageSubject)
                    .responseRequired(false)
                    .build();
            final Message<String> messageResponse =
                    MessagesModelFactory.<String>newMessageBuilder(messageHeaders)
                            .payload("Pong")
                            .build();
            final SendThingMessageResponse<String> liveResponse = SendThingMessageResponse.of(
                    liveMessageObj.getEntityId(),
                    messageResponse,
                    HttpStatus.OK,
                    headersWithResponseRequiredFalse
            );
            // WebSocket client responds with a response to the live command
            webSocketClient.send(liveResponse);

            LOGGER.info("webSocketClient sent liveResponse <{}>", liveResponse);
            websocketLatch.countDown();
        }).get(15, TimeUnit.SECONDS);

        // When
        final String liveCommandCorrelationId = UUID.randomUUID().toString();
        final C responseConsumer = initResponseConsumer(connectionName, liveCommandCorrelationId);
        final DittoHeaders headers = DittoHeaders.newBuilder()
                .correlationId(liveCommandCorrelationId)
                .schemaVersion(JsonSchemaVersion.V_2)
                .channel(TopicPath.Channel.LIVE.getName())
                .build();
        final MessageHeaders messageHeaders =
                MessageHeaders.newBuilder(MessageDirection.TO, thingId, messageSubject).build();
        final Message<String> message = MessagesModelFactory.<String>newMessageBuilder(messageHeaders)
                .payload("Ping")
                .build();
        final SendThingMessage<String> liveMessage = SendThingMessage.of(thingId, message, headers);
        // send live command via connectivity:
        sendSignal(connectionName, liveMessage);

        // Then
        // expect response to the live command:
        consumeResponseInFuture(liveCommandCorrelationId, responseConsumer)
                .thenAccept(response -> {
                    LOGGER.info("Connectivity got response <{}>", response);
                    if (response == null) {
                        final String detailMsg = "Response was not received in expected time!";
                        LOGGER.error(detailMsg);
                        throw new AssertionError(detailMsg);
                    }
                    try {
                        final Signal<?> signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(response));
                        assertThat(signal).isInstanceOf(SendThingMessageResponse.class);
                        final SendThingMessageResponse<?> liveResponse = (SendThingMessageResponse<?>) signal;
                        assertThat(liveResponse.getHttpStatus()).isEqualTo(HttpStatus.OK);

                        connectivityLatch.countDown();
                    } catch (final Throwable e) {
                        LOGGER.error("Got unexpected Throwable {}: {}", e.getClass().getName(), e.getMessage(), e);
                    }
                });

        assertThat(websocketLatch.await(5, TimeUnit.SECONDS)).describedAs("websocketLatch").isTrue();
        assertThat(connectivityLatch.await(5, TimeUnit.SECONDS)).describedAs("connectivityLatch").isTrue();
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1, CONNECTION_WITH_RAW_MESSAGE_MAPPER_1, CONNECTION_WITH_RAW_MESSAGE_MAPPER_2})
    public void canSendRawPayloadViaConnectionWithEnabledRawMessageMapper() {
        // Given
        final String sendingConnectionName = cf.connectionWithRawMessageMapper1;
        final String receivingConnectionName = cf.connectionWithRawMessageMapper2;

        final ThingId thingId =
                createNewThingWithAccessForUserAndConnection(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient(),
                        cf.connectionName1, sendingConnectionName, receivingConnectionName);

        final String messageSubject = UUID.randomUUID().toString();
        final String liveMessageCorrelationId = UUID.randomUUID().toString();

        final C targetsConsumer = initTargetsConsumer(receivingConnectionName);
        final C responseConsumer = initResponseConsumer(sendingConnectionName, liveMessageCorrelationId);

        // When sending live message with raw string
        final MessageHeaders messageHeaders =
                MessageHeaders.newBuilder(MessageDirection.TO, thingId, messageSubject)
                        .contentType("text/plain")
                        .correlationId(liveMessageCorrelationId)
                        .build();
        final String message = "{\"headers\":{},\"message\":\"Ping\"}";
        sendMessageAsJsonString(sendingConnectionName, message, messageHeaders);

        // Then receiving live message as raw string
        final M liveMessage = consumeFromTarget(receivingConnectionName, targetsConsumer);
        assertThat(liveMessage).as("No message received").isNotNull();
        assertThat(trimHeadersFromMessage(textFrom(liveMessage))).isEqualTo(message);

        // When responding with live response with raw string
        final MessageHeaders responseHeaders =
                MessageHeaders.newBuilder(MessageDirection.FROM, thingId, messageSubject)
                        .contentType("text/plain")
                        .correlationId(liveMessageCorrelationId)
                        .httpStatus(HttpStatus.OK)
                        .build();
        final String response = "{\"headers\":{},\"message\":\"Pong\"}";
        sendMessageAsJsonString(receivingConnectionName, response, responseHeaders);

        // Then receiving live response as raw string
        final M liveResponse = consumeResponse(liveMessageCorrelationId, responseConsumer);
        assertThat(liveResponse).as("No response received").isNotNull();
        assertThat(trimHeadersFromMessage(textFrom(liveResponse))).isEqualTo(response);
    }

    private String trimHeadersFromMessage(final String response) {
        if (connectionTypeHasHeadersInAdaptable()) {
            return JsonObject.of(response).set(JsonifiableAdaptable.JsonFields.HEADERS, JsonObject.empty()).toString();
        } else {
            return response;
        }
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1, CONNECTION_WITH_RAW_MESSAGE_MAPPER_1, CONNECTION_WITH_RAW_MESSAGE_MAPPER_2})
    public void canSendRawBytePayloadViaConnectionWithEnabledRawMessageMapper() throws IOException {
        // Given
        final String sendingConnectionName = cf.connectionWithRawMessageMapper1;
        final String receivingConnectionName = cf.connectionWithRawMessageMapper2;

        final ThingId thingId =
                createNewThingWithAccessForUserAndConnection(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient(),
                        cf.connectionName1, sendingConnectionName, receivingConnectionName);

        final String messageSubject = UUID.randomUUID().toString();
        final String liveMessageCorrelationId = UUID.randomUUID().toString();

        final C targetsConsumer = initTargetsConsumer(receivingConnectionName);
        final C responseConsumer = initResponseConsumer(sendingConnectionName, liveMessageCorrelationId);

        // When sending live message with raw string
        final MessageHeaders messageHeaders =
                MessageHeaders.newBuilder(MessageDirection.TO, thingId, messageSubject)
                        .contentType("application/octet-stream")
                        .correlationId(liveMessageCorrelationId)
                        .build();
        final InputStream binary = this.getClass().getClassLoader().getResourceAsStream("BinaryMessagePayload.jpg");
        final byte[] message = binary.readAllBytes();
        sendMessageAsBytePayload(sendingConnectionName, message, messageHeaders);

        // Then receiving live message as raw string
        final M liveMessage = consumeFromTarget(receivingConnectionName, targetsConsumer);
        assertThat(liveMessage).as("No message received").isNotNull();
        assertThat(bytesFrom(liveMessage)).isEqualTo(message);

        // When responding with live response with raw string
        final MessageHeaders responseHeaders =
                MessageHeaders.newBuilder(MessageDirection.FROM, thingId, messageSubject)
                        .contentType("application/octet-stream")
                        .correlationId(liveMessageCorrelationId)
                        .httpStatus(HttpStatus.OK)
                        .build();
        final byte[] response = new byte[]{4, 7, 1, 1};
        sendMessageAsBytePayload(receivingConnectionName, response, responseHeaders);

        // Then receiving live response as raw string
        final M liveResponse = consumeResponse(liveMessageCorrelationId, responseConsumer);
        assertThat(liveResponse).as("No response received").isNotNull();
        assertThat(bytesFrom(liveResponse)).isEqualTo(response);
    }

    @Test
    @Category(RequireSource.class)
    @Connections(ConnectionCategory.CONNECTION1)
    public void sendConnectivityMessageOverHttpWithContentTypeOctetStream() {

        final String respondingConnectionName = cf.connectionName1;
        final C messageConsumer = initTargetsConsumer(respondingConnectionName);

        final ThingId thingId = createNewThingWithAccessForUserAndConnection(serviceEnv.getDefaultTestingContext()
                .getOAuthClient(), respondingConnectionName);

        final String correlationId = UUID.randomUUID().toString();
        final String messageSubject = "please-respond";
        final String message = "\"This is a test.\"";
        final String responsePayload = "This is my response!";

        // 1. Send message asynchronously
        final CompletableFuture<Response> messageResponseFuture = CompletableFuture.supplyAsync(
                () -> postMessage(2, thingId, TO, messageSubject, ContentType.JSON, message, "20")
                        .withCorrelationId(correlationId)
                        // 4. Then assert response is json payload but octet stream content type
                        .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                        .expectingBody(Matchers.comparesEqualTo(responsePayload))
                        .expectingHeader(HttpHeader.CONTENT_TYPE.getName(), ContentType.BINARY.toString())
                        .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken(), true)
                        .fire());

        // 2. Then receiving live message
        final M liveMessage = consumeFromTarget(respondingConnectionName, messageConsumer);
        assertThat(liveMessage).as("No message received").isNotNull();
        assertThat(textFrom(liveMessage)).contains(message);

        // 3. Then respond with non binary content but octet stream content type
        final MessageHeaders responseHeaders = MessageHeaders.newBuilder(MessageDirection.FROM, thingId, messageSubject)
                .contentType(TestConstants.CONTENT_TYPE_APPLICATION_OCTET_STREAM)
                .correlationId(correlationId)
                .httpStatus(HttpStatus.BAD_REQUEST)
                .build();
        final Message<String> messageResponse = MessagesModelFactory.<String>newMessageBuilder(responseHeaders)
                .payload(responsePayload)
                .build();
        final SendThingMessageResponse<String> thingMessageResponse =
                SendThingMessageResponse.of(thingId, messageResponse, HttpStatus.BAD_REQUEST, responseHeaders);
        sendSignal(respondingConnectionName, thingMessageResponse);

        // see step 4
        messageResponseFuture.join();
    }

    @Test
    @Category(RequireSource.class)
    @UseConnection(category = ConnectionCategory.CONNECTION1, mod = FILTER_MESSAGES_BY_RQL)
    public void filterConnectivityMessageViaEnrichedRqlFilter() {

        final String respondingConnectionName = cf.connectionName1;
        final C messageConsumer = initTargetsConsumer(respondingConnectionName);

        final Thing thing = thingWithRandomId().toBuilder()
                .setAttribute(JsonPointer.of("awesome"), JsonValue.of(false))
                .build();
        final ThingId thingId = thing.getEntityId().orElseThrow();
        final Policy policy = policyWhichAllowsAccessForUserAndConnection(PolicyId.of(thingId),
                serviceEnv.getDefaultTestingContext().getOAuthClient(),
                respondingConnectionName);
        createNewThing(thing, policy, respondingConnectionName);

        final String blockedMessageResponseFutureWrongSubjectCorrelationId = UUID.randomUUID().toString();
        final String blockedMessageResponseFutureWrongAttributeCorrelationId = UUID.randomUUID().toString();
        final String allowedMessageResponseFutureCorrelationId = UUID.randomUUID().toString();

        final String notAllowedMessageSubject = "not-allowed-subject";
        final String allowedMessageSubject = "allowed-subject";
        final String message = "\"PING?\"";
        final String responsePayload = "PONG!";

        // 1. Send message asynchronously
        final CompletableFuture<Response> blockedMessageResponseFutureWrongAttribute = CompletableFuture.supplyAsync(
                () -> postMessage(2, thingId, TO, allowedMessageSubject, ContentType.JSON, message, "10")
                        .withCorrelationId(blockedMessageResponseFutureWrongAttributeCorrelationId)
                        // 3. Then assert response times out because message is not even received
                        .expectingHttpStatus(HttpStatus.REQUEST_TIMEOUT)
                        .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken(), true)
                        .fire());

        // 2. Then assert that message is _NOT_ received
        final M blockedMessageWrongAttribute = consumeFromTarget(respondingConnectionName, messageConsumer);
        assertThat(blockedMessageWrongAttribute)
                .as("Message with received which should have been filtered because of wrong attribute value")
                .isNull();

        blockedMessageResponseFutureWrongAttribute.join();

        // 4. change attribute "awesome=true" which should cause that the message with "allowed-subject" will be received
        putAttribute(TestConstants.API_V_2, thingId, "awesome", "true")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // 5. send a message with allowed subject which should now pass the configured filter
        final CompletableFuture<Response> allowedMessageResponseFuture = CompletableFuture.supplyAsync(
                () -> postMessage(2, thingId, TO, allowedMessageSubject, ContentType.JSON, message, "10")
                        .withCorrelationId(allowedMessageResponseFutureCorrelationId)
                        // 7. Then assert response is received because
                        .expectingHttpStatus(HttpStatus.IM_USED)
                        .expectingBody(Matchers.comparesEqualTo(responsePayload))
                        .expectingHeader(HttpHeader.CONTENT_TYPE.getName(), ContentType.TEXT.toString())
                        .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken(), true)
                        .fire());

        // 6. Then receiving live message
        final M allowedMessage = consumeFromTarget(respondingConnectionName, messageConsumer);
        assertThat(allowedMessage).as("No message received")
                .isNotNull();
        assertThat(textFrom(allowedMessage)).doesNotContain(notAllowedMessageSubject);
        assertThat(textFrom(allowedMessage)).contains(allowedMessageSubject);
        assertThat(textFrom(allowedMessage)).contains(message);

        // 8. Then respond with message response
        final MessageHeaders responseHeaders =
                MessageHeaders.newBuilder(MessageDirection.FROM, thingId, allowedMessageSubject)
                        .contentType(TestConstants.CONTENT_TYPE_TEXT_PLAIN)
                        .correlationId(allowedMessageResponseFutureCorrelationId)
                        .httpStatus(HttpStatus.IM_USED)
                        .build();
        final Message<String> messageResponse = MessagesModelFactory.<String>newMessageBuilder(responseHeaders)
                .payload(responsePayload)
                .build();
        final SendThingMessageResponse<String> thingMessageResponse =
                SendThingMessageResponse.of(thingId, messageResponse, HttpStatus.IM_USED, responseHeaders);
        sendSignal(respondingConnectionName, thingMessageResponse);

        allowedMessageResponseFuture.join();

        // 9. send a message with not allowed subject which will be filtered
        final CompletableFuture<Response> blockedMessageResponseFutureWrongSubject = CompletableFuture.supplyAsync(
                () -> postMessage(2, thingId, TO, notAllowedMessageSubject, ContentType.JSON, message, "10")
                        .withCorrelationId(blockedMessageResponseFutureWrongSubjectCorrelationId)
                        // 11. Then assert response times out because message is not even received
                        .expectingHttpStatus(HttpStatus.REQUEST_TIMEOUT)
                        .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken(), true)
                        .fire());

        // 10. Then assert that message is _NOT_ received
        final M blockedMessageWrongSubject = consumeFromTarget(respondingConnectionName, messageConsumer);
        assertThat(blockedMessageWrongSubject)
                .as("Message with subject received which should have been filtered")
                .isNull();

        blockedMessageResponseFutureWrongSubject.join();
    }

    private ThingId createNewThingWithAccessForUserAndConnection(final AuthClient client,
            final String connectionName,
            final String... connectionNames) {
        final Thing thing = thingWithRandomId();
        final ThingId thingId = thing.getEntityId().orElseThrow();
        final Policy policy = policyWhichAllowsAccessForUserAndConnection(PolicyId.of(thingId), client, connectionName,
                connectionNames);
        createNewThing(thing, policy, connectionName);
        return thingId;
    }

    private Thing thingWithRandomId() {
        final ThingId thingId = generateThingId();
        return Thing.newBuilder()
                .setId(thingId)
                .build();
    }

    private static Policy policyWhichAllowsAccessForUserAndConnection(final PolicyId policyId,
            final AuthClient client, final String connectionName, final String... connectionNames) {

        final SubjectId connectivitySubjectId = SubjectId.newInstance(SubjectIssuer.INTEGRATION,
                SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution().getUsername() + ":" + connectionName);
        final Subject connectivitySubject = Subject.newInstance(connectivitySubjectId);
        final List<Subject> subjects = Arrays.stream(connectionNames)
                .map(name -> SubjectId.newInstance(SubjectIssuer.INTEGRATION,
                        SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution().getUsername() + ":" + name))
                .map(Subject::newInstance)
                .collect(Collectors.toList());

        subjects.add(client.getSubject());
        subjects.add(connectivitySubject);

        return Policy.newBuilder(policyId)
                .forLabel("OWNER")
                .setSubjects(Subjects.newInstance(subjects))
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"),
                        Permission.READ, Permission.WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"),
                        Permission.READ, Permission.WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"),
                        Permission.READ, Permission.WRITE)
                .forLabel("DEVICE")
                .build();
    }

    private static ConnectivityTestWebsocketClient initiateWebSocketClient(final AuthClient client) {
        return ConnectivityTestWebsocketClient.newInstance(dittoWsUrl(TestConstants.API_V_2), client.getAccessToken());
    }

}
