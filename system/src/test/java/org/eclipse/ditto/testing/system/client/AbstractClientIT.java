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
package org.eclipse.ditto.testing.system.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.JUnitSoftAssertions;
import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.changes.Change;
import org.eclipse.ditto.client.configuration.BasicAuthenticationConfiguration;
import org.eclipse.ditto.client.configuration.ClientCredentialsAuthenticationConfiguration;
import org.eclipse.ditto.client.configuration.ProxyConfiguration;
import org.eclipse.ditto.client.live.messages.MessageRegistration;
import org.eclipse.ditto.client.live.messages.RepliableMessage;
import org.eclipse.ditto.client.management.CommonManagement;
import org.eclipse.ditto.client.messaging.AuthenticationProvider;
import org.eclipse.ditto.client.messaging.AuthenticationProviders;
import org.eclipse.ditto.client.twin.Twin;
import org.eclipse.ditto.client.twin.TwinThingHandle;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.messages.model.MessageDirection;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.Solution;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.system.client.util.ClientFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingTooLargeException;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neovisionaries.ws.client.WebSocket;

import io.restassured.response.Response;

/**
 * Abstract base class for integration tests using the Ditto Client.
 */
public abstract class AbstractClientIT extends IntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractClientIT.class);

    protected static final int DEFAULT_MAX_THING_SIZE = 1024 * 100;
    protected static final int DEFAULT_MAX_MESSAGE_SIZE = 1024 * 250;

    public static final int TIMEOUT_SECONDS = 30;
    public static final int LATCH_TIMEOUT = TIMEOUT_SECONDS;
    public static final int NEGATIVE_LATCH_TIMEOUT = TIMEOUT_SECONDS / 5;

    protected static final String BINARY_PAYLOAD_RESOURCE = "BinaryMessagePayload.jpg";

    @Rule
    public TestName name = new TestName();

    @Rule
    public JUnitSoftAssertions softly = new JUnitSoftAssertions();

    protected static DittoClient newDittoClient(final AuthClient authClient) {
        return newDittoClient(authClient, Collections.emptyList(), Collections.emptyList());
    }

    protected static DittoClient newDittoClientV2(final AuthClient authClient) {
        return newDittoClientV2(authClient, Collections.emptyList(), Collections.emptyList());
    }

    protected static DittoClient newDittoClientV2(final AuthClient authClient,
            final Collection<AcknowledgementLabel> declaredTwinAcknowledgements,
            final Collection<AcknowledgementLabel> declaredLiveAcknowledgements) {

        final AuthenticationProvider<WebSocket> authenticationProvider = authProvider(authClient);
        return newDittoClientV2(authenticationProvider, declaredTwinAcknowledgements, declaredLiveAcknowledgements,
                null);
    }

    protected static DittoClient newDittoClientV2(final AuthClient authClient,
            final Collection<AcknowledgementLabel> declaredTwinAcknowledgements,
            final Collection<AcknowledgementLabel> declaredLiveAcknowledgements,
            final Consumer<Throwable> errorHandler) {

        final AuthenticationProvider<WebSocket> authenticationProvider = authProvider(authClient);
        return newDittoClientV2(authenticationProvider, declaredTwinAcknowledgements, declaredLiveAcknowledgements,
                errorHandler);
    }

    protected static DittoClient newDittoClient(final AuthClient authClient,
            final Collection<AcknowledgementLabel> declaredTwinAcknowledgements,
            final Collection<AcknowledgementLabel> declaredLiveAcknowledgements) {

        final AuthenticationProvider<WebSocket> authenticationProvider = authProvider(authClient);
        return newDittoClient(authenticationProvider, declaredTwinAcknowledgements, declaredLiveAcknowledgements);
    }

    private static AuthenticationProvider<WebSocket> authProvider(final AuthClient authClient) {
        return AuthenticationProviders.clientCredentials(ClientCredentialsAuthenticationConfiguration.newBuilder()
                .clientId(authClient.getClientId())
                .clientSecret(authClient.getClientSecret())
                .tokenEndpoint(authClient.getTokenEndpoint())
                .scopes(scopeStringToList(authClient.getScope().orElseThrow()))
                .proxyConfiguration(proxyConfiguration())
                .build());
    }

    protected static DittoClient newDittoClient(final Solution solution, final JsonSchemaVersion schemaVersion) {
        final AuthenticationProvider<WebSocket> authenticationProvider =
                AuthenticationProviders.basic(BasicAuthenticationConfiguration.newBuilder()
                        .username(solution.getUsername())
                        .password(solution.getSecret())
                        .build());
        return newDittoClient(authenticationProvider, schemaVersion, Collections.emptyList(), Collections.emptyList());
    }

    protected static DittoClient newDittoClient(final AuthenticationProvider<WebSocket> authenticationProvider,
            final Collection<AcknowledgementLabel> declaredTwinAcknowledgements,
            final Collection<AcknowledgementLabel> declaredLiveAcknowledgements) {
        return newDittoClient(authenticationProvider, JsonSchemaVersion.V_2, declaredTwinAcknowledgements,
                declaredLiveAcknowledgements);
    }


    protected static DittoClient newDittoClient(final AuthenticationProvider<WebSocket> authenticationProvider,
            final JsonSchemaVersion schemaVersion,
            final Collection<AcknowledgementLabel> declaredTwinAcknowledgements,
            final Collection<AcknowledgementLabel> declaredLiveAcknowledgements) {
        return ClientFactory.newClient(dittoWsUrl(schemaVersion.toInt()), schemaVersion, authenticationProvider,
                proxyConfiguration(), declaredTwinAcknowledgements, declaredLiveAcknowledgements, null);
    }

    protected static DittoClient newDittoClientV2(final AuthenticationProvider<WebSocket> authenticationProvider,
            final Collection<AcknowledgementLabel> declaredTwinAcknowledgements,
            final Collection<AcknowledgementLabel> declaredLiveAcknowledgements,
            final Consumer<Throwable> errorHandler) {
        return ClientFactory.newClient(dittoWsUrl(2), JsonSchemaVersion.V_2, authenticationProvider,
                proxyConfiguration(), declaredTwinAcknowledgements, declaredLiveAcknowledgements, errorHandler);
    }

    protected static ThingId postEmptyThing() {
        final Response response = postThing(JsonSchemaVersion.V_2.toInt(), JsonFactory.newObject())
                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken())
                .fire();
        return ThingId.of(parseIdFromLocation(response.getHeader("Location")));
    }

    protected static void sendMessagesOverRest(final ThingId thingId,
            final String featureId,
            final String subject,
            final String contentType,
            final int messageCount) {

        final Supplier<String> payloadSupplier =
                () -> new com.eclipsesource.json.JsonObject()
                        .add("createdAt", System.currentTimeMillis()).toString();

        sendMessagesOverRest(thingId, featureId, subject, contentType, messageCount, payloadSupplier);
    }

    protected static void sendMessagesOverRest(final ThingId thingId,
            final String featureId,
            final String subject,
            final String contentType,
            final int messageCount,
            final Supplier<String> payloadSupplier) {

        LOGGER.info("Going to start sending {} messages with contentType {}...", messageCount, contentType);
        IntStream.range(0, messageCount).parallel().forEach((i) ->
        {
            final String payload = payloadSupplier.get();
            try {
                final Response response;
                if (featureId == null) {
                    response =
                            postMessage(2, thingId, MessageDirection.FROM, subject, contentType, payload,
                                    "0").fire();
                } else {
                    response = postMessage(2, thingId, featureId, MessageDirection.FROM, subject, contentType,
                            payload, "0").fire();
                }
                LOGGER.info("Response: {}", response.getStatusLine());
                if (LOGGER.isDebugEnabled()) {
                    response.prettyPrint();
                }
            } catch (final Exception e) {
                LOGGER.error("Exception occurred when sending message", e);
            }
        });
    }

    public static void shutdownClient(final DittoClient client) {
        if (client != null) {
            client.destroy();
        }
    }

    public static boolean changePathEndsWithPointer(final Change change, final JsonPointer jsonPointer) {
        final int levelCount = jsonPointer.getLevelCount();
        final JsonPointer path = change.getPath();

        return path.getSubPointer(path.getLevelCount() - levelCount)
                .map(pointer -> pointer.equals(jsonPointer))
                .orElse(false);
    }

    public void awaitLatchTrue(final CountDownLatch latch) throws InterruptedException {
        Assertions.assertThat(latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS))
                .withFailMessage("Latch did not reach zero before timeout. Count=" + latch.getCount())
                .isTrue();
    }

    protected void awaitLatchFalse(final CountDownLatch latch) throws InterruptedException {
        Assertions.assertThat(latch.await(NEGATIVE_LATCH_TIMEOUT, TimeUnit.SECONDS))
                .withFailMessage("Latch should not reach zero before timeout.")
                .isFalse();
    }

    protected void startConsumptionAndWait(final CommonManagement<?, ?>... clients) {
        try {
            final CompletableFuture<?> future = CompletableFuture.allOf(Arrays.stream(clients)
                    .map(CommonManagement::startConsumption)
                    .map(CompletionStage::toCompletableFuture)
                    .toArray(CompletableFuture[]::new));
            future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    protected static ProxyConfiguration proxyConfiguration() {
        ProxyConfiguration proxyConfiguration = null;
        if (TEST_CONFIG.isHttpProxyEnabled()) {
            proxyConfiguration = ProxyConfiguration.newBuilder()
                    .proxyHost(TEST_CONFIG.getHttpProxyHost())
                    .proxyPort(TEST_CONFIG.getHttpProxyPort())
                    .build();
        }
        return proxyConfiguration;
    }

    protected static void registerForMessages(final MessageRegistration registration, final String subject,
            final CountDownLatch latch) {
        registerForMessages(registration, subject, latch, message -> {});
    }

    protected static void registerForMessages(final MessageRegistration registration, final String subject,
            final CountDownLatch latch, final Consumer<RepliableMessage<ByteBuffer, Object>> messageCheck) {
        registration.registerForMessage(UUID.randomUUID().toString(), subject, ByteBuffer.class, message -> {
            try {
                LOGGER.info("Message received: {}", message);
                messageCheck.accept(message);
                latch.countDown();
            } catch (final Throwable e) {
                LOGGER.error("Error in message handler:", e);
            }
        });
    }

    protected static List<String> scopeStringToList(final String scopes) {
        final String[] splittedScopes = scopes.split(",");
        final List<String> scopesList;
        scopesList = Arrays.asList(splittedScopes);
        return scopesList;
    }

    protected void expectThingTooLargeExceptionIfThingGetsTooLarge(final DittoClient dittoClient, final Thing thing,
            final Function<TwinThingHandle, CompletionStage<Void>> modification) {
        final ThingId thingId = thing.getEntityId().orElseThrow();
        final Twin twin = dittoClient.twin();
        final CompletableFuture<Void> resultFuture = twin.create(thing)
                .thenCompose(created -> {
                    LOGGER.info("Created Thing: '{}'", created);
                    return modification.apply(twin.forId(thingId));
                })
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        LOGGER.info("Received exception response: '{}'", throwable.getMessage());
                    } else {
                        LOGGER.info("Request was successful, that was not expected.");
                    }
                }).toCompletableFuture();

        Awaitility.await().until(resultFuture::isDone);

        assertThat(resultFuture)
                .isCompletedExceptionally()
                .hasFailedWithThrowableThat()
                .isInstanceOf(ThingTooLargeException.class);
    }

}
