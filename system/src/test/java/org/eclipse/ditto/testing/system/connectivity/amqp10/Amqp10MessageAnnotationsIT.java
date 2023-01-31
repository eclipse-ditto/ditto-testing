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
package org.eclipse.ditto.testing.system.connectivity.amqp10;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Date;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import javax.jms.TextMessage;

import org.eclipse.ditto.base.model.auth.AuthorizationContext;
import org.eclipse.ditto.base.model.auth.AuthorizationSubject;
import org.eclipse.ditto.base.model.auth.DittoAuthorizationContextType;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.ConnectivityStatus;
import org.eclipse.ditto.connectivity.model.Topic;
import org.eclipse.ditto.testing.common.client.ditto_protocol.options.Option;
import org.eclipse.ditto.testing.common.composite_resources.HttpToAmqpResource;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.connectivity.ConnectionResource;
import org.eclipse.ditto.testing.common.connectivity.amqp.AmqpClient;
import org.eclipse.ditto.testing.common.connectivity.amqp.AmqpClientResource;
import org.eclipse.ditto.testing.common.connectivity.amqp.options.AmqpClientOptionDefinitions;
import org.eclipse.ditto.testing.common.correlationid.TestNameCorrelationId;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.events.ThingCreated;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * This test ensures that the back-end supports AMQP message annotations ({@code amqp.message.annotation}) in both
 * directions.
 * The test requires two connections with different IDs because events are dropped for the originating connection ID.
 */
@RunIf(DockerEnvironment.class)
@AmqpClientResource.Config(connectionName = Amqp10MessageAnnotationsIT.CONNECTION_NAME)
public final class Amqp10MessageAnnotationsIT {

    static final String CONNECTION_NAME = "Amqp10MessageAnnotationsIT";

    private static final TestConfig TEST_CONFIG = TestConfig.getInstance();

    private static final String INCOMING_JS_PAYLOAD_MAPPING_SCRIPT = """
            function mapToDittoProtocolMsg(
                headers,
                textPayload,
                bytePayload,
                contentType
            ) {
                const deviceId = headers["amqp.message.annotation:iothub-connection-device-id"];
                const deviceIdParts = deviceId.split(":");
                
                // Assuming, that content-type is always 'application/json'.
                return Ditto.buildDittoProtocolMsg(
                    deviceIdParts[0],
                    deviceIdParts[1],
                    "things",
                    "twin",
                    "commands",
                    "create",
                    "/",
                    headers,
                    {
                        "thingId": deviceId,
                        "attributes": JSON.parse(textPayload)
                    }
                );
            }""";

    @ClassRule(order = 0)
    public static final HttpToAmqpResource HTTP_TO_AMQP_RESOURCE = HttpToAmqpResource.newInstance(
            TEST_CONFIG,
            (amqpClientResource, testSolutionResource) -> {
                final var authorizationContext = AuthorizationContext.newInstance(
                        DittoAuthorizationContextType.PRE_AUTHENTICATED_CONNECTION,
                        AuthorizationSubject.newInstance(
                                MessageFormat.format("integration:{0}:{1}",
                                        testSolutionResource.getTestUsername(),
                                        amqpClientResource.getConnectionName())
                        )
                );
                return ConnectivityModelFactory.newConnectionBuilder(
                                ConnectionId.generateRandom(),
                                ConnectionType.AMQP_10,
                                ConnectivityStatus.OPEN,
                                amqpClientResource.getEndpointUri().toString()
                        )
                        .name(amqpClientResource.getConnectionName())
                        .specificConfig(Map.of("jms.closeTimeout", "0"))
                        .sources(List.of(
                                ConnectivityModelFactory.newSourceBuilder()
                                        .authorizationContext(authorizationContext)
                                        .address(amqpClientResource.getSourceAddress())
                                        .payloadMapping(ConnectivityModelFactory.newPayloadMapping("myMapping"))
                                        .replyTargetEnabled(true)
                                        .build()
                        ))
                        .payloadMappingDefinition(ConnectivityModelFactory.newPayloadMappingDefinition(Map.of(
                                "myMapping",
                                ConnectivityModelFactory.newMappingContext(
                                        "JavaScript",
                                        Map.of("incomingScript", INCOMING_JS_PAYLOAD_MAPPING_SCRIPT)
                                )
                        )))
                        .build();
            }
    );

    @ClassRule(order = 1)
    public static final ConnectionResource LISTENER_APP_CONNECTION_RESOURCE =
            ConnectionResource.newInstance(
                    TEST_CONFIG.getTestEnvironment(),
                    HTTP_TO_AMQP_RESOURCE.getConnectionsHttpClientResource(),
                    () -> {
                        final var amqpClientResource = HTTP_TO_AMQP_RESOURCE.getAmqpClientResource();
                        final var authorizationContext = AuthorizationContext.newInstance(
                                DittoAuthorizationContextType.PRE_AUTHENTICATED_CONNECTION,
                                AuthorizationSubject.newInstance(
                                        MessageFormat.format("integration:{1}",
                                                amqpClientResource.getConnectionName())
                                )
                        );
                        return ConnectivityModelFactory.newConnectionBuilder(
                                        ConnectionId.of("temporary"),
                                        ConnectionType.AMQP_10,
                                        ConnectivityStatus.OPEN,
                                        amqpClientResource.getEndpointUri().toString()
                                )
                                .name(amqpClientResource.getConnectionName())
                                .specificConfig(Map.of("jms.closeTimeout", "0"))
                                .targets(List.of(
                                        ConnectivityModelFactory.newTargetBuilder()
                                                .authorizationContext(authorizationContext)
                                                .address(amqpClientResource.getTargetAddress())
                                                .topics(Topic.TWIN_EVENTS)
                                                .headerMapping(
                                                        ConnectivityModelFactory.newHeaderMapping(Map.of(

                                                                // This prefix determines that the back-end sets the
                                                                // AMQP message annotation to the outgoing AMQP message.
                                                                // The prefix itself is not part of the annotation key.
                                                                "amqp.message.annotation:thingId", "{{ thing:id }}"
                                                        ))
                                                )
                                                .qos(1)
                                                .build()
                                ))
                                .build();
                    }
            );

    @Rule
    public final TestNameCorrelationId testNameCorrelationId = TestNameCorrelationId.newInstance();

    @Test
    public void sendRawAmqpMessageWithMessageAnnotationHeaderGetsMappedToCreateThing() {
        final var thingId = getThingId();
        final var thingAttributes = getThingAttributes();
        final var amqpClientResource = HTTP_TO_AMQP_RESOURCE.getAmqpClientResource();
        final var amqpClient = amqpClientResource.getAmqpClient();

        final var textMessageFuture = new CompletableFuture<TextMessage>();
        final var thingCreatedFuture = new CompletableFuture<ThingCreated>();

        amqpClient.onTextMessage(textMessageFuture::complete);
        amqpClient.onSignal(signal -> {
            if (signal instanceof ThingCreated thingCreated) {
                thingCreatedFuture.complete(thingCreated);
            }
        });

        amqpClient.sendRaw(
                thingAttributes.toJsonString(),
                null,
                testNameCorrelationId.getCorrelationId(),
                Option.newInstance(
                        AmqpClientOptionDefinitions.Send.MESSAGE_ANNOTATION_HEADER,
                        new AmqpClientOptionDefinitions.MessageAnnotationHeader(
                                "iothub-connection-device-id",
                                thingId.toString()
                        )
                )
        );

        assertThat(textMessageFuture)
                .succeedsWithin(Duration.ofSeconds(3L))
                .satisfies(textMessage -> {
                    final var amqpJmsMessageFacade = AmqpClient.getAmqpJmsMessageFacade(textMessage).orElseThrow();
                    assertThat(amqpJmsMessageFacade.getTracingAnnotation("thingId"))
                            .as("AMQP message annotation")
                            .isEqualTo(thingId.toString());
                });

        assertThat(thingCreatedFuture)
                .succeedsWithin(Duration.ofSeconds(3L))
                .satisfies(thingCreated -> {
                    final var createdThing = thingCreated.getThing();
                    assertThat(createdThing.getEntityId()).hasValue(thingId);
                    assertThat(createdThing.getAttributes()).hasValue(thingAttributes);
                });
    }

    private static ThingId getThingId() {
        return ThingId.generateRandom();
    }

    private static Attributes getThingAttributes() {
        final var simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return ThingsModelFactory.newAttributesBuilder()
                .set("manufacturer", "ACME Inc.")
                .set("manufacturingDate", simpleDateFormat.format(Date.from(Instant.now())))
                .build();
    }

}
