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
package org.eclipse.ditto.testing.system.connectivity.httppush;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.pekko.http.javadsl.model.HttpMethods;
import org.apache.pekko.http.javadsl.model.HttpRequest;
import org.apache.pekko.http.javadsl.model.headers.Authorization;
import org.apache.pekko.http.javadsl.model.headers.HttpCredentials;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.eclipse.ditto.base.model.acks.AcknowledgementRequest;
import org.eclipse.ditto.base.model.acks.DittoAcknowledgementLabel;
import org.eclipse.ditto.base.model.common.DittoConstants;
import org.eclipse.ditto.base.model.common.DittoDuration;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.Enforcement;
import org.eclipse.ditto.connectivity.model.HmacCredentials;
import org.eclipse.ditto.connectivity.model.MappingContext;
import org.eclipse.ditto.connectivity.model.OAuthClientCredentials;
import org.eclipse.ditto.connectivity.model.TargetBuilder;
import org.eclipse.ditto.connectivity.model.Topic;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.assertions.DittoJsonAssertions;
import org.eclipse.ditto.jwt.model.ImmutableJsonWebToken;
import org.eclipse.ditto.jwt.model.JsonWebToken;
import org.eclipse.ditto.messages.model.Message;
import org.eclipse.ditto.messages.model.MessageDirection;
import org.eclipse.ditto.messages.model.MessageHeaders;
import org.eclipse.ditto.messages.model.MessageHeadersBuilder;
import org.eclipse.ditto.messages.model.signals.commands.SendThingMessage;
import org.eclipse.ditto.messages.model.signals.commands.SendThingMessageResponse;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.Solution;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.client.http.AsyncHttpClientFactory;
import org.eclipse.ditto.testing.common.http.HttpTestServer;
import org.eclipse.ditto.testing.system.connectivity.AbstractTargetOnlyTestCases;
import org.eclipse.ditto.testing.system.connectivity.ConnectionCategory;
import org.eclipse.ditto.testing.system.connectivity.ConnectionModelFactory;
import org.eclipse.ditto.testing.system.connectivity.Connections;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityFactory;
import org.eclipse.ditto.things.model.Features;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttribute;
import org.eclipse.ditto.things.model.signals.commands.modify.MergeThing;
import org.eclipse.ditto.things.model.signals.commands.modify.DeleteAttribute;
import org.eclipse.ditto.things.model.signals.commands.modify.DeleteFeature;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyFeatureProperty;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * System tests for HTTP-push connections.
 * Failsafe won't run this directly because it does not end in *IT.java.
 */
public final class HttpPushConnectivitySuite extends AbstractTargetOnlyTestCases<String, HttpRequest> {

    private static final Enforcement ENFORCEMENT =
            ConnectivityModelFactory.newEnforcement("{{ header:device_id }}", "{{ thing:id }}");

    private static final String FEATURES_JSON = "{\n" +
            "  \"lorem\": { \"properties\": { \"ipsum\": \"dolor\" } },\n" +
            "  \"sit\": { \"properties\": { \"amet\": \"consetetur\" } }\n" +
            "}";

    private static HttpTestServer server;
    private HttpPushConnectivityWorker worker;

    public HttpPushConnectivitySuite() {
        super(ConnectivityFactory.of(
                "HttpPush",
                connectionModelFactory,
                ConnectionType.HTTP_PUSH,
                HttpPushConnectivitySuite::getServerUri,
                Collections::emptyMap,
                HttpPushConnectivitySuite::defaultTargetAddress,
                suffix -> null,
                id -> ENFORCEMENT,
                () -> SSH_TUNNEL_CONFIG)
                .withDefaultHeaderMapping(HttpPushConnectivityWorker.CORRELATION_ID_MAP));
    }

    private static String getServerUri(final boolean tunnel, final boolean basicAuth) {
        return server.getUri(tunnel, basicAuth, CONFIG.getHttpTunnel(), CONFIG.getHttpHostName());
    }

    @BeforeClass
    public static void startServer() {
        server = HttpTestServer.newInstance();
    }

    @AfterClass
    public static void stopServer() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @Before
    @Override
    public void setupConnectivity() throws Exception {
        super.setupConnectivity();
        server.resetServerWithKeyPaths(cf.allConnectionNames());
        worker = HttpPushConnectivityWorker.of(server);
        cf.setUpConnections(connectionsWatcher.getConnections());
    }

    @After
    public void cleanUp() {
        cleanupConnections(testingContextWithRandomNs.getSolution().getUsername());
    }

    @Test
    @Connections(ConnectionCategory.NONE)
    public void testHttpMethodAndBasicAuth() {
        final ConnectivityFactory factory =
                cf.withTargetAddress(name -> "PATCH:/" + name);
        final String connectionName = cf.disambiguateConnectionName(
                testingContextWithRandomNs.getSolution().getUsername(),"HttpPushMethodInTargetAddress");
        factory.setupSingleConnection(connectionName).join();
        sendSignal(null, newCreateThing(connectionName, connectionName));
        final HttpRequest request = consumeResponse(null, connectionName);
        assertThat(request.method()).isEqualTo(HttpMethods.PATCH);
        assertThat(request.getHeader(Authorization.class))
                .contains(Authorization.basic(HttpTestServer.USERNAME, HttpTestServer.PASSWORD));
    }

    @Test
    @Connections(ConnectionCategory.NONE)
    public void testOAuth2() {
        // GIVEN: A connection is created with OAuth2 credentials
        final Solution solution = testingContextWithRandomNs.getSolution();
        final String connectionName = cf.disambiguateConnectionName(
                testingContextWithRandomNs.getSolution().getUsername(),"HttpPushWithOAuth2");

        final var authClient = testingContextWithRandomNs.getOAuthClient();
        final var credentials = OAuthClientCredentials.newBuilder()
                .tokenEndpoint(authClient.getTokenEndpoint())
                .clientId(authClient.getClientId())
                .clientSecret(authClient.getClientSecret())
                .scope(authClient.getScope().orElse(""))
                .build();

        final Connection connection = cf.getSingleConnection(solution.getUsername(), connectionName)
                .toBuilder()
                .uri(server.getUri(false, false, CONFIG.getHttpTunnel(), CONFIG.getHttpHostName()))
                .credentials(credentials)
                .build();
        cf.asyncCreateConnection(solution, connection).join();

        // WHEN: The connection publishes a signal
        sendSignal(null, newCreateThing(connectionName, connectionName));
        final HttpRequest request = consumeResponse(null, connectionName);

        // THEN: The authorization header of the HTTP request contains a bearer token from the token endpoint.
        final String authorization =
                request.getHeader("authorization").map(org.apache.pekko.http.javadsl.model.HttpHeader::value).orElseThrow();
        final String bearer = "Bearer ";
        assertThat(authorization).startsWith(bearer);
        final String token = authorization.substring(bearer.length());
        final JsonWebToken expectedJwt =
                ImmutableJsonWebToken.fromToken(testingContextWithRandomNs.getOAuthClient().getAccessToken());
        final JsonWebToken jwt = ImmutableJsonWebToken.fromToken(token);
        assertThat(jwt.getIssuer()).isEqualTo(expectedJwt.getIssuer());
        assertThat(jwt.getSubjects()).isEqualTo(expectedJwt.getSubjects());
        assertThat(jwt.getScopes()).isEqualTo(expectedJwt.getScopes());
    }

    @Test
    @Connections(ConnectionCategory.NONE)
    public void testAzureHmacAuth() {
        final Solution solution = testingContextWithRandomNs.getSolution();
        final String connectionName = cf.disambiguateConnectionName(
                testingContextWithRandomNs.getSolution().getUsername(),"HttpPushWithAzHmacAuth");

        final String algorithm = "az-monitor-2016-04-01";
        final String workspaceId = "xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx";
        final String sharedKey = "SGFsbG8gV2VsdCEgSXN0IGRhcyBhbG";

        final Connection singleConnectionWithAwsHmacAuth =
                getSingleConnectionWithAzHmacAuth(cf.getSingleConnection(solution.getUsername(), connectionName), algorithm,
                        workspaceId,
                        sharedKey, false);
        cf.asyncCreateConnection(solution, singleConnectionWithAwsHmacAuth).join();

        sendSignal(null, newCreateThing(connectionName, connectionName));
        final HttpRequest request = consumeResponse(null, connectionName);

        final AzRequestSigning azRequestSigning = AzRequestSigning.newInstance(workspaceId, sharedKey);
        final HttpCredentials httpCredentials = azRequestSigning.generateSignedAuthorizationHeader(request);

        final String authorization =
                request.getHeader("authorization").map(org.apache.pekko.http.javadsl.model.HttpHeader::value).orElseThrow();
        assertThat(authorization).isEqualTo(httpCredentials.toString());
    }

    @Test
    @Connections(ConnectionCategory.NONE)
    public void testAzureSaslAuth() {
        // GIVEN: A connection is created with az-sasl HMAC credentials
        final Solution solution = testingContextWithRandomNs.getSolution();
        final String connectionName = cf.disambiguateConnectionName(
                testingContextWithRandomNs.getSolution().getUsername(),"HttpPushWithAzSaslAuth");
        final String skn = "RootSharedAccessKeyPolicy";
        final String endpoint = "localhost";

        final String sharedKey = "SGFsbG8gV2VsdCEgSXN0IGRhcyBhbG";
        final HmacCredentials credentials = HmacCredentials.of("az-sasl", JsonObject.newBuilder()
                .set("sharedKeyName", skn)
                .set("sharedKey", sharedKey)
                .set("endpoint", endpoint)
                .build());

        final Connection singleConnectionWithAzureSaslAuth = cf.getSingleConnection(solution.getUsername(), connectionName)
                .toBuilder()
                .uri(server.getUri(false, false, CONFIG.getHttpTunnel(), CONFIG.getHttpHostName()))
                .credentials(credentials)
                .build();
        cf.asyncCreateConnection(solution, singleConnectionWithAzureSaslAuth).join();

        // WHEN: The connection publishes a signal
        sendSignal(null, newCreateThing(connectionName, connectionName));
        final HttpRequest request = consumeResponse(null, connectionName);

        final AzSaslRequestSigning saslRequestSigning = AzSaslRequestSigning.newInstance(endpoint, skn, sharedKey);
        final HttpCredentials expectedCredentials = saslRequestSigning.generateSignedAuthorizationHeader(request);

        // THEN: The authorization header of the HTTP request conforms to Azure-SAS pattern.
        final String authorization =
                request.getHeader("authorization").map(org.apache.pekko.http.javadsl.model.HttpHeader::value).orElseThrow();
        assertThat(authorization).isEqualTo(expectedCredentials.toString());
    }

    @Test
    @Connections(ConnectionCategory.NONE)
    public void testTunneledAzureHmacAuth() {
        final Solution solution = testingContextWithRandomNs.getSolution();
        final String connectionName = cf.disambiguateConnectionName(
                testingContextWithRandomNs.getSolution().getUsername(),"HttpPushWithTunneledAzHmacAuth");

        final String algorithm = "az-monitor-2016-04-01";
        final String workspaceId = "xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx";
        final String sharedKey = "SGFsbG8gV2VsdCEgSXN0IGRhcyBhbG";

        final Connection singleConnectionWithAzHmacAuth =
                getSingleConnectionWithAzHmacAuth(cf.getSingleConnectionWithTunnel(connectionName),
                        algorithm, workspaceId, sharedKey, true);
        cf.asyncCreateConnection(solution, singleConnectionWithAzHmacAuth).join();

        sendSignal(null, newCreateThing(connectionName, connectionName));
        final HttpRequest request = consumeResponse(null, connectionName);

        final AzRequestSigning azRequestSigning = AzRequestSigning.newInstance(workspaceId, sharedKey);
        final HttpCredentials httpCredentials = azRequestSigning.generateSignedAuthorizationHeader(request);

        final String authorization =
                request.getHeader("authorization").map(org.apache.pekko.http.javadsl.model.HttpHeader::value).orElseThrow();
        assertThat(authorization).isEqualTo(httpCredentials.toString());
    }

    @Test
    @Connections(ConnectionCategory.NONE)
    public void testAwsHmacAuth() {
        final String region = "us-east-1";
        final String service = "iam";
        final String secretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        final String accessKey = "MyAwesomeAccessKey";
        final String algorithm = "aws4-hmac-sha256";
        final boolean doubleEncodeAndNormalize = false;
        final List<String> canonicalHeaderNames = List.of("x-amz-date",
                "host");

        final Solution solution = testingContextWithRandomNs.getSolution();
        final String connectionName = cf.disambiguateConnectionName(
                testingContextWithRandomNs.getSolution().getUsername(),"HttpPushWithAwsHmacAuth");
        // this is not actually consuming a response but the forwarded event via http
        final Connection singleConnectionWithAwsHmacAuth =
                getSingleConnectionWithAwsHmacAuth(cf.getSingleConnection(solution.getUsername(), connectionName),
                        region, service, secretKey, accessKey, algorithm, doubleEncodeAndNormalize,
                        canonicalHeaderNames, false);

        cf.asyncCreateConnection(solution, singleConnectionWithAwsHmacAuth).join();

        sendSignal(null, newCreateThing(connectionName, connectionName));
        final HttpRequest request = consumeResponse(null, connectionName);
        final AwsRequestSigning awsRequestSigning =
                AwsRequestSigning.newInstance(region, service, secretKey, accessKey, algorithm,
                        doubleEncodeAndNormalize, canonicalHeaderNames, AwsRequestSigning.XAmzContentSha256.EXCLUDED);
        final HttpCredentials httpCredentials = awsRequestSigning.generateSignedAuthorizationHeader(request);

        final String authorization =
                request.getHeader("authorization").map(org.apache.pekko.http.javadsl.model.HttpHeader::value).orElseThrow();
        assertThat(authorization).isEqualTo(httpCredentials.toString());
    }

    @Test
    @Connections(ConnectionCategory.NONE)
    public void testTunneledAwsHmacAuth() {
        final String region = "us-east-1";
        final String service = "iam";
        final String secretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        final String accessKey = "MyAwesomeAccessKey";
        final String algorithm = "aws4-hmac-sha256";
        final boolean doubleEncodeAndNormalize = false;
        final List<String> canonicalHeaderNames = List.of("x-amz-date",
                "host");

        final Solution solution = testingContextWithRandomNs.getSolution();
        final String connectionName = cf.disambiguateConnectionName(
                testingContextWithRandomNs.getSolution().getUsername(),"HttpPushWithTunneledAwsHmacAuth");
        // this is not actually consuming a response but the forwarded event via http
        final Connection singleConnectionWithAwsHmacAuth =
                getSingleConnectionWithAwsHmacAuth(cf.getSingleConnectionWithTunnel(connectionName),
                        region, service, secretKey, accessKey, algorithm, doubleEncodeAndNormalize,
                        canonicalHeaderNames, true);

        cf.asyncCreateConnection(solution, singleConnectionWithAwsHmacAuth).join();

        sendSignal(null, newCreateThing(connectionName, connectionName));
        final HttpRequest request = consumeResponse(null, connectionName);
        final AwsRequestSigning awsRequestSigning =
                AwsRequestSigning.newInstance(region, service, secretKey, accessKey, algorithm,
                        doubleEncodeAndNormalize, canonicalHeaderNames, AwsRequestSigning.XAmzContentSha256.EXCLUDED);
        final HttpCredentials httpCredentials = awsRequestSigning.generateSignedAuthorizationHeader(request);

        final String authorization =
                request.getHeader("authorization").map(org.apache.pekko.http.javadsl.model.HttpHeader::value).orElseThrow();
        assertThat(authorization).isEqualTo(httpCredentials.toString());
    }

    @Test
    @Connections(ConnectionCategory.CONNECTION1)
    public void testNormalizedPayloadMapping() {
        final Solution solution = testingContextWithRandomNs.getSolution();
        final String connectionName = cf.disambiguateConnectionName(
                testingContextWithRandomNs.getSolution().getUsername(),"HttpPushNormalizedPayloadMapping");
        // include connectionName1 as a receiving connection to get thingCreated event.
        final CreateThing createThing =
                attachFeatures(newTargetOnlyCreateThing(connectionName, cf.connectionName1),
                        ThingsModelFactory.newFeatures(FEATURES_JSON));
        sendSignal(null, createThing);
        // this is not actually consuming a response but the forwarded event via http
        consumeResponse(null, cf.connectionName1);
        final Connection singleConnection = cf.getSingleConnection(solution.getUsername(), connectionName);
        final MappingContext mappingContext = ConnectivityModelFactory.newMappingContext("Normalized",
                Collections.singletonMap("fields", "features,attributes,_revision"));
        final Connection connectionWithNormalizedPayload =
                singleConnection.toBuilder()
                        .setTargets(singleConnection.getTargets().stream()
                                .map(ConnectivityModelFactory::newTargetBuilder)
                                .map(sb -> sb.payloadMapping(ConnectivityModelFactory.newPayloadMapping("normalized")))
                                .map(TargetBuilder::build)
                                .collect(Collectors.toList()))
                        .payloadMappingDefinition(
                                ConnectivityModelFactory.newPayloadMappingDefinition("normalized", mappingContext))
                        .build();

        cf.asyncCreateConnection(solution, connectionWithNormalizedPayload).join();

        final ThingId thingId = createThing.getThing().getEntityId().orElseThrow(NoSuchElementException::new);
        final ModifyFeatureProperty modifyFeatureProperty =
                ModifyFeatureProperty.of(
                        thingId,
                        "sit",
                        JsonPointer.of("amet"),
                        JsonFactory.newObject("{\"consetetur\":[\"sadipscing\", \"elitr\"]}"),
                        createThing.getDittoHeaders()
                );
        sendSignal(null, modifyFeatureProperty);
        final HttpRequest request = consumeResponse(null, connectionName);
        final JsonObject normalizedJson =
                JsonFactory.newObject(request.entity()
                        .toStrict(5000L, server.getMat())
                        .toCompletableFuture()
                        .join()
                        .getData()
                        .utf8String()
                );

        DittoJsonAssertions.assertThat(normalizedJson)
                .containsExactlyInAnyOrderElementsOf(JsonFactory.newObject("{\n" +
                        "  \"features\": {\n" +
                        "    \"sit\": {\"properties\":{ \"amet\":{ \"consetetur\":[\"sadipscing\",\"elitr\"]}}}\n" +
                        "  },\n" +
                        "  \"_revision\": 2\n" +
                        "}")
                );
    }

    @Test
    @Connections(ConnectionCategory.CONNECTION1)
    public void testNormalizedPayloadMappingForMergeThing() {
        final Solution solution = testingContextWithRandomNs.getSolution();
        final String connectionName = cf.disambiguateConnectionName(
                testingContextWithRandomNs.getSolution().getUsername(),"HttpPushNormalizedPayloadMappingForMergeThing");
        // include connectionName1 as a receiving connection to get thingCreated event.
        final CreateThing createThing =
                attachFeatures(newTargetOnlyCreateThing(connectionName, cf.connectionName1),
                        ThingsModelFactory.newFeatures(FEATURES_JSON));
        sendSignal(null, createThing);
        // this is not actually consuming a response but the forwarded event via http
        consumeResponse(null, cf.connectionName1);
        final Connection singleConnection = cf.getSingleConnection(solution.getUsername(), connectionName);
        final MappingContext mappingContext = ConnectivityModelFactory.newMappingContext("Normalized",
                Collections.singletonMap("fields", "features,attributes,_revision"));
        final Connection connectionWithNormalizedPayload =
                singleConnection.toBuilder()
                        .setTargets(singleConnection.getTargets().stream()
                                .map(ConnectivityModelFactory::newTargetBuilder)
                                .map(sb -> sb.payloadMapping(ConnectivityModelFactory.newPayloadMapping("normalized")))
                                .map(TargetBuilder::build)
                                .collect(Collectors.toList()))
                        .payloadMappingDefinition(
                                ConnectivityModelFactory.newPayloadMappingDefinition("normalized", mappingContext))
                        .build();

        cf.asyncCreateConnection(solution, connectionWithNormalizedPayload).join();

        final ThingId thingId = createThing.getThing().getEntityId().orElseThrow(NoSuchElementException::new);
        final MergeThing mergeThing = MergeThing.withFeatures(thingId,
                ThingsModelFactory.newFeatures(JsonFactory.newObject("{\n" +
                        "  \"lorem\": { \"properties\": { \"ipsum\" : null }},\n" +
                        "  \"sit\": { \"properties\": { \"amet\": { \"consetetur\":[\"sadipscing\",\"elitr\"]}}}\n" +
                        "}")),
                createThing.getDittoHeaders());

        sendSignal(null, mergeThing);
        final HttpRequest request = consumeResponse(null, connectionName);
        final JsonObject normalizedJson =
                JsonFactory.newObject(request.entity()
                        .toStrict(5000L, server.getMat())
                        .toCompletableFuture()
                        .join()
                        .getData()
                        .utf8String()
                );

        DittoJsonAssertions.assertThat(normalizedJson)
                .containsExactlyInAnyOrderElementsOf(JsonFactory.newObject("{\n" +
                        "  \"features\": {\n" +
                        "    \"sit\": {\"properties\":{ \"amet\":{ \"consetetur\":[\"sadipscing\",\"elitr\"]}}}\n" +
                        "  },\n" +
                        "  \"_revision\": 2\n" +
                        "}")
                );
    }

    @Test
    @Connections(ConnectionCategory.CONNECTION1)
    public void testNormalizedPayloadMappingWithDeletedFields() {
        final Solution solution = testingContextWithRandomNs.getSolution();
        final String connectionName = cf.disambiguateConnectionName(
                testingContextWithRandomNs.getSolution().getUsername(),"HttpPushNormalizedPayloadMappingWithDeletedFields");
        final CreateThing createThing =
                attachFeatures(newTargetOnlyCreateThing(connectionName, cf.connectionName1),
                        ThingsModelFactory.newFeatures(FEATURES_JSON));
        sendSignal(null, createThing);
        consumeResponse(null, cf.connectionName1);
        final Connection singleConnection = cf.getSingleConnection(solution.getUsername(), connectionName);
        final Map<String, String> mappingOptions = Map.of(
                "fields", "features,attributes,_revision,_deletedFields",
                "includeDeletedFields", "true"
        );
        final MappingContext mappingContext = ConnectivityModelFactory.newMappingContext("Normalized", mappingOptions);
        final Connection connectionWithNormalizedPayload =
                singleConnection.toBuilder()
                        .setTargets(singleConnection.getTargets().stream()
                                .map(ConnectivityModelFactory::newTargetBuilder)
                                .map(sb -> sb.payloadMapping(ConnectivityModelFactory.newPayloadMapping("normalized")))
                                .map(TargetBuilder::build)
                                .collect(Collectors.toList()))
                        .payloadMappingDefinition(
                                ConnectivityModelFactory.newPayloadMappingDefinition("normalized", mappingContext))
                        .build();

        cf.asyncCreateConnection(solution, connectionWithNormalizedPayload).join();

        final ThingId thingId = createThing.getThing().getEntityId().orElseThrow(NoSuchElementException::new);
        final MergeThing mergeThing = MergeThing.withFeatures(thingId,
                ThingsModelFactory.newFeatures(JsonFactory.newObject("{\n" +
                        "  \"lorem\": { \"properties\": { \"ipsum\" : null }},\n" +
                        "  \"sit\": { \"properties\": { \"amet\": { \"consetetur\":[\"sadipscing\",\"elitr\"]}}}\n" +
                        "}")),
                createThing.getDittoHeaders());

        sendSignal(null, mergeThing);
        final HttpRequest request = consumeResponse(null, connectionName);
        final JsonObject normalizedJson =
                JsonFactory.newObject(request.entity()
                        .toStrict(5000L, server.getMat())
                        .toCompletableFuture()
                        .join()
                        .getData()
                        .utf8String()
                );

        assertThat(normalizedJson.getValue(JsonPointer.of("/_deletedFields/features/lorem/properties/ipsum")))
                .isPresent()
                .get()
                .satisfies(value -> assertThat(value.isString()).isTrue());
    }

    @Test
    @Connections(ConnectionCategory.CONNECTION1)
    public void testNormalizedPayloadMappingWithDeleteAttribute() {
        final Solution solution = testingContextWithRandomNs.getSolution();
        final String connectionName = cf.disambiguateConnectionName(
                testingContextWithRandomNs.getSolution().getUsername(),"HttpPushNormalizedPayloadMappingWithDeleteAttribute");
        final CreateThing createThing =
                attachFeatures(newTargetOnlyCreateThing(connectionName, cf.connectionName1),
                        ThingsModelFactory.newFeatures(FEATURES_JSON));
        sendSignal(null, createThing);
        consumeResponse(null, cf.connectionName1);
        final Connection singleConnection = cf.getSingleConnection(solution.getUsername(), connectionName);
        final Map<String, String> mappingOptions = Map.of(
                "fields", "features,attributes,_revision,_deletedFields",
                "includeDeletedFields", "true"
        );
        final MappingContext mappingContext = ConnectivityModelFactory.newMappingContext("Normalized", mappingOptions);
        final Connection connectionWithNormalizedPayload =
                singleConnection.toBuilder()
                        .setTargets(singleConnection.getTargets().stream()
                                .map(ConnectivityModelFactory::newTargetBuilder)
                                .map(sb -> sb.payloadMapping(ConnectivityModelFactory.newPayloadMapping("normalized")))
                                .map(TargetBuilder::build)
                                .collect(Collectors.toList()))
                        .payloadMappingDefinition(
                                ConnectivityModelFactory.newPayloadMappingDefinition("normalized", mappingContext))
                        .build();

        cf.asyncCreateConnection(solution, connectionWithNormalizedPayload).join();

        final ThingId thingId = createThing.getThing().getEntityId().orElseThrow(NoSuchElementException::new);
        final ModifyAttribute modifyAttribute = ModifyAttribute.of(
                thingId,
                JsonPointer.of("/type"),
                JsonFactory.newValue("foo"),
                createThing.getDittoHeaders()
        );
        sendSignal(null, modifyAttribute);
        consumeResponse(null, connectionName);

        final DeleteAttribute deleteAttribute = DeleteAttribute.of(thingId, JsonPointer.of("/type"),
                createThing.getDittoHeaders());
        sendSignal(null, deleteAttribute);
        final HttpRequest request = consumeResponse(null, connectionName);
        final JsonObject normalizedJson =
                JsonFactory.newObject(request.entity()
                        .toStrict(5000L, server.getMat())
                        .toCompletableFuture()
                        .join()
                        .getData()
                        .utf8String()
                );

        assertThat(normalizedJson.getValue(JsonPointer.of("/_deletedFields/attributes/type")))
                .isPresent()
                .get()
                .satisfies(value -> assertThat(value.isString()).isTrue());
    }

    @Test
    @Connections(ConnectionCategory.CONNECTION1)
    public void testNormalizedPayloadMappingWithDeleteFeature() {
        final Solution solution = testingContextWithRandomNs.getSolution();
        final String connectionName = cf.disambiguateConnectionName(
                testingContextWithRandomNs.getSolution().getUsername(),"HttpPushNormalizedPayloadMappingWithDeleteFeature");
        final CreateThing createThing =
                attachFeatures(newTargetOnlyCreateThing(connectionName, cf.connectionName1),
                        ThingsModelFactory.newFeatures(FEATURES_JSON));
        sendSignal(null, createThing);
        consumeResponse(null, cf.connectionName1);
        final Connection singleConnection = cf.getSingleConnection(solution.getUsername(), connectionName);
        final Map<String, String> mappingOptions = Map.of(
                "fields", "features,attributes,_revision,_deletedFields",
                "includeDeletedFields", "true"
        );
        final MappingContext mappingContext = ConnectivityModelFactory.newMappingContext("Normalized", mappingOptions);
        final Connection connectionWithNormalizedPayload =
                singleConnection.toBuilder()
                        .setTargets(singleConnection.getTargets().stream()
                                .map(ConnectivityModelFactory::newTargetBuilder)
                                .map(sb -> sb.payloadMapping(ConnectivityModelFactory.newPayloadMapping("normalized")))
                                .map(TargetBuilder::build)
                                .collect(Collectors.toList()))
                        .payloadMappingDefinition(
                                ConnectivityModelFactory.newPayloadMappingDefinition("normalized", mappingContext))
                        .build();

        cf.asyncCreateConnection(solution, connectionWithNormalizedPayload).join();

        final ThingId thingId = createThing.getThing().getEntityId().orElseThrow(NoSuchElementException::new);
        final DeleteFeature deleteFeature = DeleteFeature.of(thingId, "lorem", createThing.getDittoHeaders());
        sendSignal(null, deleteFeature);
        final HttpRequest request = consumeResponse(null, connectionName);
        final JsonObject normalizedJson =
                JsonFactory.newObject(request.entity()
                        .toStrict(5000L, server.getMat())
                        .toCompletableFuture()
                        .join()
                        .getData()
                        .utf8String()
                );

        assertThat(normalizedJson.getValue(JsonPointer.of("/_deletedFields/features/lorem")))
                .isPresent()
                .get()
                .satisfies(value -> assertThat(value.isString()).isTrue());
    }

    @Test
    @Connections(ConnectionCategory.CONNECTION1)
    public void testSendLiveMessageViaHttpPushRespondViaWebsocketMessage()
            throws ExecutionException, InterruptedException {

        final String eventConsumer = initTargetsConsumer(cf.connectionName1, null);
        final ConnectivityFactory factory = cf.withTargetAddress(name -> "POST:/" + name);
        final String connectionName = cf.disambiguateConnectionName(
                testingContextWithRandomNs.getSolution().getUsername(),"SendLiveMessageViaHttpPushRespondViaWebsocketMessage");
        factory.setupSingleConnection(connectionName).join();

        final CreateThing createThing = newTargetOnlyCreateThing(connectionName, cf.connectionName1);
        sendSignal(null, createThing);
        final ThingId thingId = createThing.getEntityId();

        final AsyncHttpClient client = AsyncHttpClientFactory.newInstance(TEST_CONFIG);

        final String accessToken = testingContextWithRandomNs.getOAuthClient().getAccessToken();
        final String authorization = "Bearer " + accessToken;

        final String correlationId = UUID.randomUUID().toString();
        final String messageSubject = "test";

        final String url = dittoUrl(TestConstants.API_V_2,
                "/things/" + thingId + "/inbox/messages/" + messageSubject);

        final ListenableFuture<Response> postMessageResponse = client.preparePost(url)
                .setHeader(HttpHeader.X_CORRELATION_ID.getName(), correlationId)
                .addHeader("Authorization", authorization)
                .addHeader("Cache-Control", "no-cache")
                .execute();
        consumeAndAssert(connectionName, thingId, correlationId, eventConsumer,
                SendThingMessage.class, stm -> assertThat(stm.getMessage().getSubject()).isEqualTo(messageSubject));

        final HttpStatus responseHttpStatus = HttpStatus.MULTI_STATUS;
        final String responseContentType = "text/plain";
        final String responsePayload = "This is my response";

        final Message<Object> responseMessage =
                Message.newBuilder(MessageHeaders.newBuilder(MessageDirection.TO, thingId, messageSubject)
                        .httpStatus(responseHttpStatus)
                        .contentType(responseContentType)
                        .build())
                        .payload(responsePayload)
                        .build();
        final SendThingMessageResponse<Object> thingMessageResponse =
                SendThingMessageResponse.of(thingId, responseMessage, responseHttpStatus,
                        DittoHeaders.newBuilder()
                                .correlationId(correlationId)
                                .build());
        sendSignal(null, thingMessageResponse);

        final Response httpMessageResponse = postMessageResponse.toCompletableFuture().get();
        assertThat(httpMessageResponse.getStatusCode()).isEqualTo(responseHttpStatus.getCode());
        assertThat(httpMessageResponse.getContentType()).isEqualTo(responseContentType);
        assertThat(httpMessageResponse.getResponseBody()).isEqualTo(responsePayload);
    }

    /**
     * This test is required because we want to keep the workaround to explicitly declare the "live-response" label
     * as issued ack and make our service handle this as live message response.
     * Therefor this test creates a special connection which declares "live-response" as issued ack.
     */
    @Test
    @Connections(ConnectionCategory.CONNECTION1)
    public void testSendLiveMessageViaHttpPushRespondViaHttpResponseViaLiveResponseIssuedAck()
            throws ExecutionException, InterruptedException {

        final Solution solution = testingContextWithRandomNs.getSolution();
        final String respondingConnectionName =
                cf.disambiguateConnectionName(
                        testingContextWithRandomNs.getSolution().getUsername(),"slmvhprvhrvlria");
        final Connection singleConnection = cf.getSingleConnection(solution.getUsername(), respondingConnectionName);
        final Connection connectionWithLiveResponseIssuedAck =
                singleConnection.toBuilder()
                        .setTargets(singleConnection.getTargets().stream()
                                .map(ConnectivityModelFactory::newTargetBuilder)
                                .map(tb -> tb.topics(Topic.LIVE_MESSAGES)
                                        .issuedAcknowledgementLabel(DittoAcknowledgementLabel.LIVE_RESPONSE))
                                .map(TargetBuilder::build)
                                .collect(Collectors.toList()))
                        .build();

        cf.asyncCreateConnection(solution, connectionWithLiveResponseIssuedAck).join();
        final String responsePayload = "This is my response";
        final String eventConsumer = initTargetsConsumer(cf.connectionName1, null);

        final CreateThing createThing = newTargetOnlyCreateThing(respondingConnectionName, cf.connectionName1);
        sendSignal(null, createThing);
        final ThingId thingId = createThing.getEntityId();

        final AsyncHttpClient client = AsyncHttpClientFactory.newInstance(TEST_CONFIG);

        final String accessToken = testingContextWithRandomNs.getOAuthClient().getAccessToken();
        final String authorization = "Bearer " + accessToken;

        final String correlationId = UUID.randomUUID().toString();
        final String messageSubject = "please-respond";

        final String url = dittoUrl(TestConstants.API_V_2,
                "/things/" + thingId + "/inbox/messages/" + messageSubject);

        final HttpStatus responseHttpStatus = HttpStatus.MULTI_STATUS;
        final String responseContentType = "text/plain";
        final Map<String, String> headersMap = new HashMap<>();
        headersMap.put("status", String.valueOf(responseHttpStatus.getCode()));
        headersMap.put("content-type", responseContentType);
        sendAsJsonString(respondingConnectionName, correlationId, responsePayload, headersMap);

        final ListenableFuture<Response> postMessageResponse = client.preparePost(url)
                .setHeader(HttpHeader.X_CORRELATION_ID.getName(), correlationId)
                .addHeader("Authorization", authorization)
                .addHeader("Cache-Control", "no-cache")
                .setBody("This is my request")
                .execute();
        consumeAndAssert(respondingConnectionName, thingId, correlationId, eventConsumer,
                SendThingMessage.class, stm -> assertThat(stm.getMessage().getSubject()).isEqualTo(messageSubject));

        final Response httpMessageResponse = postMessageResponse.toCompletableFuture().get();
        assertThat(httpMessageResponse.getStatusCode()).isEqualTo(responseHttpStatus.getCode());
        assertThat(httpMessageResponse.getContentType()).isEqualTo(responseContentType);
        assertThat(httpMessageResponse.getResponseBody()).isEqualTo(responsePayload);
    }

    @Test
    @Connections(ConnectionCategory.CONNECTION1)
    public void testSendLiveMessageViaHttpPushRespondViaHttpResponse() throws ExecutionException, InterruptedException {
        final String eventConsumer = initTargetsConsumer(cf.connectionName1, null);

        final String respondingConnectionName = cf.connectionName1;
        final CreateThing createThing = newTargetOnlyCreateThing(cf.connectionName1, cf.connectionName1);
        sendSignal(null, createThing);
        final ThingId thingId = createThing.getEntityId();

        final AsyncHttpClient client = AsyncHttpClientFactory.newInstance(TEST_CONFIG);

        final String accessToken = testingContextWithRandomNs.getOAuthClient().getAccessToken();
        final String authorization = "Bearer " + accessToken;

        final String correlationId = UUID.randomUUID().toString();
        final String messageSubject = "please-respond";

        final String url = dittoUrl(TestConstants.API_V_2,
                "/things/" + thingId + "/inbox/messages/" + messageSubject);

        final HttpStatus responseHttpStatus = HttpStatus.MULTI_STATUS;
        final String messageContentType = "text/plain";
        final String responseContentType = DittoConstants.DITTO_PROTOCOL_CONTENT_TYPE;
        final MessageHeaders responseMessageHeaders =
                MessageHeadersBuilder.newInstance(MessageDirection.TO, thingId, messageSubject)
                        .contentType(messageContentType)
                        .correlationId(correlationId)
                        .build();
        final String responsePayload = "This is my response!";

        final SendThingMessageResponse<String> liveResponse = SendThingMessageResponse.of(thingId,
                Message.<String>newBuilder(responseMessageHeaders).payload(responsePayload).build(),
                responseHttpStatus, responseMessageHeaders);
        final Adaptable liveResponseAdaptable = DittoProtocolAdapter.newInstance().toAdaptable(liveResponse);
        final String messageResponseString =
                ProtocolFactory.wrapAsJsonifiableAdaptable(liveResponseAdaptable).toJson().toString();

        sendAsJsonString(respondingConnectionName, correlationId, messageResponseString,
                Map.of("content-type", responseContentType));

        final ListenableFuture<Response> postMessageResponse = client.preparePost(url)
                .setHeader(HttpHeader.X_CORRELATION_ID.getName(), correlationId)
                .addHeader("Authorization", authorization)
                .addHeader("Cache-Control", "no-cache")
                .setBody("This is my request")
                .execute();
        consumeAndAssert(respondingConnectionName, thingId, correlationId, eventConsumer,
                SendThingMessage.class, stm -> assertThat(stm.getMessage().getSubject()).isEqualTo(messageSubject));

        final Response httpMessageResponse = postMessageResponse.toCompletableFuture().get();
        assertThat(httpMessageResponse.getStatusCode()).isEqualTo(responseHttpStatus.getCode());
        assertThat(httpMessageResponse.getContentType()).isEqualTo(messageContentType);
        assertThat(httpMessageResponse.getResponseBody()).isEqualTo(responsePayload);
    }

    @Test
    @Connections(ConnectionCategory.CONNECTION1)
    public void testSendPolicyAnnouncementWithTargetIssuedAck() throws InterruptedException {
        // start consumer
        final var consumer = initTargetsConsumer(cf.connectionName1);

        final ConnectionId connectionId = cf.getConnectionId(cf.connectionName1);
        final String targetAddress = defaultTargetAddress(cf.connectionName1);
        // GIVEN: a policy is created with a expiring subject expiring soon
        final var autoIssuedAckLabel = ConnectionModelFactory.toAcknowledgementLabel(connectionId, targetAddress);
        final var ackTimeout = DittoDuration.parseDuration("60s");
        final var policyId =
                createExpiringPolicy(AcknowledgementRequest.of(autoIssuedAckLabel), ackTimeout, cf.connectionName1,
                        Duration.ofSeconds(3), DittoDuration.parseDuration("5s"));
        final var connectionSubjectId = connectionSubject(cf.connectionName1).getId();

        // WHEN: subject expired
        TimeUnit.SECONDS.sleep(3);

        // THEN: announcement was sent and HTTP server responded with success
        final var message1 = consumeFromTarget(cf.connectionName1, consumer);
        assertThat(message1).withFailMessage("Did not receive policy announcement via HTTP").isNotNull();
        final var announcement = assertSubjectDeletionAnnouncement(message1, policyId, connectionSubjectId);

        final Instant deleteAt = announcement.getDeleteAt();
        final Instant now = Instant.now();
        if (deleteAt.isAfter(now)) {
            TimeUnit.MILLISECONDS.sleep(Duration.between(now, deleteAt).plusMillis(100).toMillis());
        }

        // THEN: expired subject is deleted
        final var willGetDeleted = "will-get-deleted";
        getPolicyEntrySubject(policyId, willGetDeleted, connectionSubjectId.toString())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .useAwaitility(AWAITILITY_DEFAULT_CONFIG)
                .fire();
    }

    private Connection getSingleConnectionWithAzHmacAuth(final Connection connection, final String algorithm,
            final String workspaceId,
            final String sharedKey, final boolean withTunnel) {
        final HmacCredentials hmacCredentials = HmacCredentials.of(algorithm, JsonObject.newBuilder()
                .set("workspaceId", workspaceId)
                .set("sharedKey", sharedKey)
                .build());

        return connection.toBuilder()
                .uri(server.getUri(withTunnel, false, CONFIG.getHttpTunnel(), CONFIG.getHttpHostName()))
                .credentials(hmacCredentials)
                .build();
    }

    private Connection getSingleConnectionWithAwsHmacAuth(final Connection connection, final String region,
            final String service, final String secretKey,
            final String accessKey, final String algorithm, final boolean doubleDecode,
            final List<String> canonicalHeaderNames, final boolean withTunnel) {
        final HmacCredentials hmacAwsCredentials = HmacCredentials.of(algorithm, JsonObject.newBuilder()
                .set("region", region)
                .set("service", service)
                .set("accessKey", accessKey)
                .set("secretKey", secretKey)
                .set("doubleEncode", doubleDecode)
                .set("canonicalHeaders", JsonArray.of(canonicalHeaderNames))
                .build());

        return connection.toBuilder()
                .uri(server.getUri(withTunnel, false, CONFIG.getHttpTunnel(), CONFIG.getHttpHostName()))
                .credentials(hmacAwsCredentials)
                .build();
    }

    @Override
    protected String targetAddressForTargetPlaceHolderSubstitution() {
        return defaultTargetAddress(cf.connectionNameWithAuthPlaceholderOnHEADER_ID);
    }

    @Override
    protected HttpPushConnectivityWorker getConnectivityWorker() {
        return worker;
    }

    private static String defaultTargetAddress(final String connectionName) {
        return "POST:/" + connectionName;
    }

    private static CreateThing attachFeatures(final CreateThing command, final Features features) {
        return CreateThing.of(command.getThing().toBuilder().setFeatures(features).build(),
                command.getInitialPolicy().orElse(null),
                command.getPolicyIdOrPlaceholder().orElse(null),
                command.getDittoHeaders());

    }

}
