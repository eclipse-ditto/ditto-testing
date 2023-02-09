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
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION_WITH_CONNECTION_ANNOUNCEMENTS;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.headers.DittoHeadersBuilder;
import org.eclipse.ditto.base.model.json.Jsonifiable;
import org.eclipse.ditto.connectivity.model.signals.announcements.ConnectionClosedAnnouncement;
import org.eclipse.ditto.connectivity.model.signals.announcements.ConnectionOpenedAnnouncement;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.messages.model.Message;
import org.eclipse.ditto.messages.model.MessageBuilder;
import org.eclipse.ditto.messages.model.MessageDirection;
import org.eclipse.ditto.messages.model.signals.commands.MessageCommand;
import org.eclipse.ditto.messages.model.signals.commands.SendThingMessage;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.testing.common.categories.RequireProtocolHeaders;
import org.eclipse.ditto.testing.common.categories.RequireSource;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.eclipse.ditto.things.model.signals.events.ThingCreated;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public abstract class AbstractConnectivityHeaderMappingITestCases<C, M>
        extends AbstractConnectivityMessageSizeLimitITestCases<C, M> {

    protected AbstractConnectivityHeaderMappingITestCases(final ConnectivityFactory cf) {
        super(cf);
    }

    @Test
    @Category({RequireSource.class, RequireProtocolHeaders.class})
    @Connections({ConnectionCategory.CONNECTION_WITH_HEADER_MAPPING})
    public void commandResponseContainsMappedHeaders() {
        final ThingBuilder.FromScratch thingBuilder = Thing.newBuilder();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(cf.connectionNameWithHeaderMapping))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();

        final String correlationId = createNewCorrelationId();
        final ThingId thingId = generateThingId();
        final String overwriteCorrelationId = "bumlux-4711";
        final DittoHeaders sentHeaders = createDittoHeaders(correlationId).toBuilder()
                .putHeader("overwrite-correlation-id", overwriteCorrelationId)
                .build();

        final CreateThing createThing =
                CreateThing.of(thingBuilder.setId(thingId).build(), policy.toJson(), sentHeaders);

        final C responseConsumer = initResponseConsumer(cf.connectionNameWithHeaderMapping, correlationId);

        waitMillis(1000);
        sendSignal(cf.connectionNameWithHeaderMapping, createThing);

        final M response = consumeResponse(correlationId, responseConsumer);

        assertThat(response).withFailMessage("Did not receive a response.").isNotNull();

        final Map<String, String> headers = checkHeaders(response);

        assertThat(headers).containsEntry("my-response-correlation-id", overwriteCorrelationId);
    }

    @Test
    @Category(RequireProtocolHeaders.class)
    @Connections({ConnectionCategory.CONNECTION1, ConnectionCategory.CONNECTION_WITH_HEADER_MAPPING})
    public void ensureConsumedMessagesWithHeaderMappingAppliedContainExpectedHeaders() {
        // verifies target header mapping from connectionName1 to connectionNameWithHeaderMapping
        // Given
        final ThingBuilder.FromScratch thingBuilder = Thing.newBuilder();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(cf.connectionName1))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .forLabel("RESTRICTED")
                .setSubject(connectionSubject(cf.connectionNameWithHeaderMapping))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ)
                .build();

        final String correlationId = createNewCorrelationId();
        final ThingId thingId = generateThingId();
        final String overwriteCorrelationId = "bumlux-4711";
        final String expectedUppercaseThingName = thingId.getName().toUpperCase();
        final DittoHeadersBuilder sentHeadersBuilder = createDittoHeaders(correlationId).toBuilder();
        sentHeadersBuilder.putHeader("overwrite-correlation-id", overwriteCorrelationId);
        final DittoHeaders sentHeaders = sentHeadersBuilder.build();

        final CreateThing createThing =
                CreateThing.of(thingBuilder.setId(thingId).build(), policy.toJson(), sentHeaders);

        final String messageContentType = "text/plain";
        final String messageSubject = "the-test-is-on-fire";
        final String messagePayload = "who you gonna call?";
        final Message<?> message = Message.newBuilder(
                MessageBuilder.newHeadersBuilder(MessageDirection.TO, thingId, messageSubject)
                        .contentType(messageContentType)
                        .build())
                .payload(messagePayload)
                .build();
        final SendThingMessage<?> sendThingMessage = SendThingMessage.of(thingId, message, sentHeaders);

        // When
        final C eventConsumer = initTargetsConsumer(cf.connectionNameWithHeaderMapping);

        waitMillis(1000);
        sendSignal(cf.connectionName1, createThing);
        waitMillis(1000);
        sendSignal(cf.connectionName1, sendThingMessage);
        waitShort();


        // Then
        final M eventMessage = consumeFromTarget(cf.connectionNameWithHeaderMapping, eventConsumer);
        assertThat(eventMessage).withFailMessage("Did not receive an event.").isNotNull();
        final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(eventMessage);
        final Adaptable eventAdaptable = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
        final Jsonifiable<JsonObject> event = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable);

        final ThingCreated tc = thingEventForJson(event, ThingCreated.class, correlationId, thingId);
        assertThat(tc.getRevision()).isEqualTo(1L);

        // expect headers from getHeaderMappingForConnectionWithHeaderMapping():
        final Map<String, String> expectedHeadersThingCreated = new HashMap<>();
        expectedHeadersThingCreated.put("correlation-id", correlationId);
        expectedHeadersThingCreated.put("my-correlation-id", overwriteCorrelationId);
        expectedHeadersThingCreated.put("original-content-type", "application/json");
        expectedHeadersThingCreated.put("content-type", "application/vnd.eclipse.ditto+json");
        expectedHeadersThingCreated.put("uppercase-thing-name", expectedUppercaseThingName);

        final Map<String, String> receivedProtocolHeaders = checkHeaders(eventMessage);
        assertThat(receivedProtocolHeaders).containsAllEntriesOf(expectedHeadersThingCreated);


        final M sendThingMessageMessage = consumeFromTarget(cf.connectionNameWithHeaderMapping, eventConsumer);
        assertThat(sendThingMessageMessage).withFailMessage("Did not receive an sendThingMessage.").isNotNull();
        final JsonifiableAdaptable jsonifiableAdaptable1 = jsonifiableAdaptableFrom(sendThingMessageMessage);
        final Adaptable eventAdaptable1 = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable1).build();
        final Jsonifiable<JsonObject> event1 = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable1);

        assertThat(event1).isInstanceOf(MessageCommand.class);
        assertThat(event1).isInstanceOf(SendThingMessage.class);
        final SendThingMessage<?> stm = (SendThingMessage) event1;
        assertThat((CharSequence) stm.getEntityId()).isEqualTo(thingId);
        final Message<String> receiveMessage = (Message<String>) stm.getMessage();
        assertThat(receiveMessage.getSubject()).isEqualTo(messageSubject);
        assertThat(receiveMessage.getPayload()).contains(messagePayload);

        // expect headers from getHeaderMappingForConnectionWithHeaderMapping():
        final Map<String, String> expectedHeadersThingMessage = new HashMap<>();
        expectedHeadersThingMessage.put("correlation-id", correlationId);
        expectedHeadersThingMessage.put("my-correlation-id", overwriteCorrelationId);
        expectedHeadersThingMessage.put("subject", messageSubject);
        expectedHeadersThingMessage.put("original-content-type", messageContentType);
        expectedHeadersThingMessage.put("content-type", "application/vnd.eclipse.ditto+json");
        expectedHeadersThingMessage.put("uppercase-thing-name", expectedUppercaseThingName);

        final Map<String, String> receivedProtocolHeaders1 = checkHeaders(sendThingMessageMessage);
        assertThat(receivedProtocolHeaders1).containsAllEntriesOf(expectedHeadersThingMessage);
    }

    @Test
    @Category({RequireSource.class, RequireProtocolHeaders.class})
    @Connections({ConnectionCategory.CONNECTION1, ConnectionCategory.CONNECTION_WITH_HEADER_MAPPING})
    public void ensureProducedMessagesWithHeaderMappingAppliedContainExpectedHeaders() {

        // verifies source header mapping from connectionNameWithHeaderMapping to connectionName1
        // Given
        final ThingBuilder.FromScratch thingBuilder = Thing.newBuilder();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(cf.connectionNameWithHeaderMapping))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .forLabel("RESTRICTED")
                .setSubject(connectionSubject(cf.connectionName1))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ)
                .build();

        final String correlationId = createNewCorrelationId();
        final ThingId thingId = generateThingId();
        final String overwriteCorrelationId = "bumlux-4711";
        final String expectedUppercaseThingName = thingId.getName().toUpperCase();
        final DittoHeadersBuilder sentHeadersBuilder = createDittoHeaders(correlationId).toBuilder();
        sentHeadersBuilder.putHeader("overwrite-correlation-id", overwriteCorrelationId);
        final DittoHeaders sentHeaders = sentHeadersBuilder.build();

        final CreateThing createThing =
                CreateThing.of(thingBuilder.setId(thingId).build(), policy.toJson(), sentHeaders);

        final String messageContentType = "text/plain";
        final String messageSubject = "the-test-is-on-fire";
        final String messagePayload = "who you gonna call?";
        final Message<?> message = Message.newBuilder(
                MessageBuilder.newHeadersBuilder(MessageDirection.TO, thingId, messageSubject)
                        .contentType(messageContentType)
                        .build()
        )
                .payload(messagePayload)
                .build();
        final SendThingMessage<?> sendThingMessage = SendThingMessage.of(thingId, message, sentHeaders);

        // When
        final C eventConsumer = initTargetsConsumer(cf.connectionName1);

        sendSignal(cf.connectionNameWithHeaderMapping, createThing);
        waitMillis(1000);
        sendSignal(cf.connectionNameWithHeaderMapping, sendThingMessage);
        waitShort();

        // Then
        final M eventMessage = consumeFromTarget(cf.connectionName1, eventConsumer);
        assertThat(eventMessage).withFailMessage("Did not receive an event.").isNotNull();
        final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(eventMessage);
        final Adaptable eventAdaptable = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
        final Jsonifiable<JsonObject> event = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable);

        final ThingCreated tc = thingEventForJson(event, ThingCreated.class, correlationId, thingId);
        assertThat(tc.getRevision()).isEqualTo(1L);

        // expect headers from getHeaderMappingForConnectionWithHeaderMapping():
        final Map<String, String> expectedHeadersThingCreated = new HashMap<>();
        expectedHeadersThingCreated.put("correlation-id", correlationId);
        expectedHeadersThingCreated.put("my-correlation-id", overwriteCorrelationId);
        expectedHeadersThingCreated.put("original-content-type", "application/json");
        expectedHeadersThingCreated.put("content-type", "application/json");
        expectedHeadersThingCreated.put("uppercase-thing-name", expectedUppercaseThingName);

        assertThat(tc.getDittoHeaders()).containsAllEntriesOf(expectedHeadersThingCreated);

        final M sendThingMessageMessage = consumeFromTarget(cf.connectionName1, eventConsumer);
        assertThat(sendThingMessageMessage).withFailMessage("Did not receive an sendThingMessage.").isNotNull();
        final JsonifiableAdaptable jsonifiableAdaptable1 = jsonifiableAdaptableFrom(sendThingMessageMessage);
        final Adaptable eventAdaptable1 = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable1).build();
        final Jsonifiable<JsonObject> event1 = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable1);

        assertThat(event1).isInstanceOf(MessageCommand.class);
        assertThat(event1).isInstanceOf(SendThingMessage.class);
        final SendThingMessage<?> stm = (SendThingMessage) event1;
        assertThat((CharSequence) stm.getEntityId()).isEqualTo(thingId);
        final Message<String> receiveMessage = (Message<String>) stm.getMessage();
        assertThat(receiveMessage.getSubject()).isEqualTo(messageSubject);
        assertThat(receiveMessage.getPayload()).contains(messagePayload);

        // expect headers from getHeaderMappingForConnectionWithHeaderMapping():
        final Map<String, String> expectedHeadersThingMessage = new HashMap<>();
        expectedHeadersThingMessage.put("correlation-id", correlationId);
        expectedHeadersThingMessage.put("my-correlation-id", overwriteCorrelationId);
        expectedHeadersThingMessage.put("subject", messageSubject);
        expectedHeadersThingMessage.put("original-content-type", messageContentType);
        expectedHeadersThingMessage.put("content-type", messageContentType);
        expectedHeadersThingMessage.put("uppercase-thing-name", expectedUppercaseThingName);

        assertThat(receiveMessage.getHeaders()).containsAllEntriesOf(expectedHeadersThingMessage);
    }

    @Test
    @Category(RequireProtocolHeaders.class)
    @Connections({CONNECTION_WITH_CONNECTION_ANNOUNCEMENTS})
    public void appliesHeaderMappingOnConnectionAnnouncements() {
        final String connectionName = cf.connectionWithConnectionAnnouncements;
        final String connectionId = cf.getConnectionId(connectionName).toString();

        final C consumer = initTargetsConsumer(connectionName);

        // some connection types will still receive the opened announcement after initializing the targets consumer,
        // some targets will already have received it before initializing the target in this test. Therefore they
        // either receive an opened announcement or nothing
        expectMsgClassOrNothing(ConnectionOpenedAnnouncement.class, connectionName, consumer);

        try {
            connectionsClient().closeConnection(connectionId)
                    .withDevopsAuth()
                    .expectingHttpStatus(HttpStatus.OK)
                    .fire();

            final M connectionClosedAnnouncementMessage = consumeFromTarget(connectionName, consumer);
            assertThat(connectionClosedAnnouncementMessage)
                    .withFailMessage("Did not receive a connection closed announcement.")
                    .isNotNull();

            final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(connectionClosedAnnouncementMessage);
            final Adaptable eventAdaptable = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
            final Jsonifiable<JsonObject> event = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable);
            assertThat(event).isInstanceOf(ConnectionClosedAnnouncement.class);

            final Map<String, String> receivedProtocolHeaders = checkHeaders(connectionClosedAnnouncementMessage);
            assertThat(receivedProtocolHeaders).containsEntry("subject", "closed");
        } finally {
            connectionsClient().openConnection(connectionId)
                    .withDevopsAuth()
                    .expectingHttpStatus(HttpStatus.OK)
                    .fire();
        }
    }

}
