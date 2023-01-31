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
package org.eclipse.ditto.testing.system.conditional;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.text.MessageFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.eclipse.ditto.base.model.auth.AuthorizationContext;
import org.eclipse.ditto.base.model.auth.AuthorizationSubject;
import org.eclipse.ditto.base.model.auth.DittoAuthorizationContextType;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.common.ResponseType;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.headers.entitytag.EntityTagMatchers;
import org.eclipse.ditto.base.model.headers.metadata.MetadataHeaderKey;
import org.eclipse.ditto.base.model.signals.commands.CommandResponse;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.ConnectivityStatus;
import org.eclipse.ditto.connectivity.model.ReplyTarget;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.messages.model.Message;
import org.eclipse.ditto.messages.model.MessageHeaders;
import org.eclipse.ditto.messages.model.signals.commands.SendThingMessage;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.ConnectionsHttpClientResource;
import org.eclipse.ditto.testing.common.TestSolutionResource;
import org.eclipse.ditto.testing.common.ThingJsonProducer;
import org.eclipse.ditto.testing.common.client.ditto_protocol.DittoProtocolClient;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.connectivity.amqp.AmqpClientResource;
import org.eclipse.ditto.testing.common.connectivity.amqp.AmqpConfig;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.correlationid.TestNameCorrelationId;
import org.eclipse.ditto.testing.common.policies.PoliciesHttpClientResource;
import org.eclipse.ditto.testing.common.policies.PolicyWithConnectionSubjectResource;
import org.eclipse.ditto.testing.common.things.ThingsHttpClient;
import org.eclipse.ditto.testing.common.things.ThingsHttpClientResource;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.modify.DeleteFeatureProperty;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttribute;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyFeature;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveAttribute;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveFeature;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveFeatures;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThing;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * Connectivity system-tests for conditional requests (retrieve/update/delete).
 * At the moment only 'amqp' is tested. Other connectivity implementations need a different setup and additional
 * {@link DittoProtocolClient} implementations. The testcases themself don't care about the connectivity channel.
 * <p>
 * There is also a 'legacy connectivity' Tests, to have one tests for all connectivity channels
 * {@link org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityITestCases#updateAttributeBasedOnCondition}.
 */
@RunIf(DockerEnvironment.class)
@AmqpClientResource.Config(connectionName = ConditionAmqpIT.CONNECTION_NAME)
public final class ConditionAmqpIT {

    static final String CONNECTION_NAME = "ConditionConnectivityIT";

    private static final TestConfig TEST_CONFIG = TestConfig.getInstance();

    @ClassRule(order = 0)
    public static final TestSolutionResource TEST_SOLUTION_RESOURCE = TestSolutionResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 0)
    public static final AmqpClientResource AMQP_CLIENT_RESOURCE =
            AmqpClientResource.newInstance(AmqpConfig.of(TEST_CONFIG));

    @ClassRule(order = 1)
    public static final ThingsHttpClientResource THINGS_HTTP_CLIENT_RESOURCE =
            ThingsHttpClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @ClassRule(order = 1)
    public static final PoliciesHttpClientResource POLICIES_HTTP_CLIENT_RESOURCE =
            PoliciesHttpClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @ClassRule(order = 1)
    public static final ConnectionsHttpClientResource CONNECTIONS_CLIENT_RESOURCE =
            ConnectionsHttpClientResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 2)
    public static final PolicyWithConnectionSubjectResource POLICY_WITH_CONNECTION_SUBJECT_RESOURCE =
            PolicyWithConnectionSubjectResource.newInstance(POLICIES_HTTP_CLIENT_RESOURCE,
                    CONNECTION_NAME);

    private static ThingsHttpClient thingsHttpClient;
    private static DittoProtocolClient dittoProtocolClient;

    @Rule
    public final TestNameCorrelationId testNameCorrelationId = TestNameCorrelationId.newInstance();

    private ThingId thingId;

    @BeforeClass
    public static void beforeClass() {
        thingsHttpClient = THINGS_HTTP_CLIENT_RESOURCE.getThingsClient();
        dittoProtocolClient = AMQP_CLIENT_RESOURCE.getAmqpClient();

        createAmqpConnectionInTestSolution();
    }

    private static void createAmqpConnectionInTestSolution() {
        final var amqpConnection = getAmqpConnection(AMQP_CLIENT_RESOURCE.getConnectionName(),
                AMQP_CLIENT_RESOURCE.getEndpointUri(),
                AMQP_CLIENT_RESOURCE.getSourceAddress(),
                AMQP_CLIENT_RESOURCE.getReplyAddress());
        final var solutionsClient = CONNECTIONS_CLIENT_RESOURCE.getConnectionsClient();
        solutionsClient.postConnection(amqpConnection,
                CorrelationId.random().withSuffix(".createAmqpConnection"));
    }

    @Before
    public void before() {
        final var thing = getThing()
                .toBuilder()
                .build();

        final var newThing = thingsHttpClient.postThingWithInitialPolicy(thing,
                POLICY_WITH_CONNECTION_SUBJECT_RESOURCE.getPolicy(),
                testNameCorrelationId.getCorrelationId(".postThing"));

        thingId = newThing.getEntityId().orElseThrow();
    }

    private static Thing getThing() {
        final var thingJsonProducer = new ThingJsonProducer();
        return thingJsonProducer.getThing();
    }

    @Test
    public void sendLiveMessageWithConditionHeader() {
        final var dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(testNameCorrelationId.getCorrelationId())
                .responseRequired(true)
                .condition("eq(attributes/manufacturer,\"ACMA\")")
                .build();
        final Map<String, String> mandatoryHeaders = new LinkedHashMap<>();
        mandatoryHeaders.put("ditto-message-direction", "TO");
        mandatoryHeaders.put("ditto-message-thing-id", thingId.toString());
        mandatoryHeaders.put("ditto-message-subject", "subject");
        final var sendLiveMessage = SendThingMessage.of(thingId,
                Message.newBuilder(MessageHeaders.of(mandatoryHeaders)).build(),
                dittoHeaders);

        final var response = dittoProtocolClient.send(sendLiveMessage).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.PRECONDITION_FAILED);
    }

    @Test
    public void putAttributeWithConditionHeader() {
        final var dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(testNameCorrelationId.getCorrelationId())
                .responseRequired(true)
                .condition("eq(attributes/manufacturer,\"ACME\")")
                .build();

        final var modifyAttribute = ModifyAttribute.of(thingId,
                JsonPointer.of("manufacturer"),
                JsonFactory.newValue("Bosch IO"),
                dittoHeaders);

        final var response = dittoProtocolClient.send(modifyAttribute).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithConditionHeader() {
        final var response =
                sendModifyFeatureWithCondition("gt(features/EnvironmentScanner/properties/temperature,\"20\")");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithConditionHeaderPreConditionFailed() {
        final var response =
                sendModifyFeatureWithCondition("lt(features/EnvironmentScanner/properties/temperature,\"20\")");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.PRECONDITION_FAILED);
    }

    @Test
    public void putFeaturePropertyWithAndCondition() {
        final var response =
                sendModifyFeatureWithCondition("and(gt(features/EnvironmentScanner/properties/temperature,\"20\")," +
                        "gt(features/EnvironmentScanner/properties/humidity,\"71\"))");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithOrCondition() {
        final var response =
                sendModifyFeatureWithCondition("or(gt(features/EnvironmentScanner/properties/temperature,\"20\")," +
                        "gt(features/EnvironmentScanner/properties/humidity,\"74\"))");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithNotCondition() {
        final var response =
                sendModifyFeatureWithCondition("not(eq(features/EnvironmentScanner/properties/temperature,\"20.1\"))");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void putFeatureWithLtCondition() {
        final var response = sendModifyFeatureWithCondition(
                "lt(features/EnvironmentScanner/properties/barometricPressure,\"980.0\")");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithLeCondition() {
        final var response = sendModifyFeatureWithCondition(
                "le(features/EnvironmentScanner/properties/barometricPressure,\"980.0\")");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void putFeatureWithGtCondition() {
        final var response = sendModifyFeatureWithCondition(
                "gt(features/EnvironmentScanner/properties/barometricPressure,\"960.0\")");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithGeCondition() {
        final var response = sendModifyFeatureWithCondition(
                "ge(features/EnvironmentScanner/properties/barometricPressure,\"970.7\")");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithNeCondition() {
        final var response = sendModifyFeatureWithCondition(
                "ne(features/EnvironmentScanner/properties/barometricPressure,\"970.6\")");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithExistsCondition() {
        final var response = sendModifyFeatureWithCondition("exists(features/EnvironmentScanner/properties/location)");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithLikeCondition() {
        final var response = sendModifyFeatureWithCondition("like(attributes/manufacturer,\"ACME\")");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithInCondition() {
        final var response =
                sendModifyFeatureWithCondition("in(attributes/manufacturer,\"ACME\",\"BUMLUX\",\"Bosch IO\")");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithTimestampCondition() {
        thingsHttpClient.putFeatureProperty(thingId,
                "EnvironmentScanner",
                "lastUpdated",
                JsonValue.of("2021-08-10T15:07:20.398Z"),
                testNameCorrelationId.getCorrelationId(".createFeatureProperty"));

        final var response = sendModifyFeatureWithCondition(
                "gt(features/EnvironmentScanner/properties/lastUpdated,\"2021-08-10T15:07:10.000Z\")");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void putAttributeWithConditionAndMissingReadPermission() {
        final var policyId = PolicyId.of(thingId);
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        final var policy = getPolicy(policyId, baseCorrelationId.withSuffix(".getPolicy"));
        final var adjustedPolicy = PoliciesModelFactory.newPolicyBuilder(policy.orElseThrow())
                .setRevokedPermissionsFor("CONNECTION", "thing", "/attributes", "READ")
                .build();
        putPolicy(policyId, adjustedPolicy, baseCorrelationId.withSuffix(".putAdjustedPolicy"));

        final var modifyAttribute = ModifyAttribute.of(thingId,
                JsonPointer.of("manufacturer"),
                JsonFactory.newValue("Bosch IO"),
                DittoHeaders.newBuilder()
                        .correlationId(baseCorrelationId)
                        .responseRequired(true)
                        .condition("eq(attributes/manufacturer,\"ACME\")")
                        .build());

        final var response = dittoProtocolClient.send(modifyAttribute).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.PRECONDITION_FAILED);
    }

    @Test
    public void putAttributeWithConditionAndPartialReadPermission() {
        final var policyId = PolicyId.of(thingId);
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        final var policy = getPolicy(policyId, baseCorrelationId.withSuffix(".getPolicy"));
        final var adjustedPolicy = PoliciesModelFactory.newPolicyBuilder(policy.orElseThrow())
                .setRevokedPermissionsFor("CONNECTION", "thing", "/features/Vehicle/properties/status", "READ")
                .build();
        putPolicy(policyId, adjustedPolicy, baseCorrelationId.withSuffix(".putAdjustedPolicy"));

        final var modifyAttribute = ModifyAttribute.of(thingId,
                JsonPointer.of("manufacturer"),
                JsonFactory.newValue("Bosch IO"),
                DittoHeaders.newBuilder()
                        .correlationId(baseCorrelationId)
                        .responseRequired(true)
                        .condition("and(eq(attributes/manufacturer,\"ACME\")," +
                                "eq(features/Vehicle/properties/status/running,\"true\"))")
                        .build());

        final var response = dittoProtocolClient.send(modifyAttribute).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.PRECONDITION_FAILED);
    }

    @Test
    public void putFeaturePropertyWithInvalidConditionSyntax() {
        final var response =
                sendModifyFeatureWithCondition("in(attributes/manufacturer,\"ACME\" \"BUMLUX\",\"Bosch IO\")");

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    public void putAttributeWithDoubleSlashInCondition() {
        final var modifyAttribute = ModifyAttribute.of(thingId,
                JsonPointer.of("manufacturer"),
                JsonFactory.newValue("Bosch IO"),
                DittoHeaders.newBuilder()
                        .correlationId(testNameCorrelationId.getCorrelationId())
                        .responseRequired(true)
                        .condition("eq(attributes//manufacturer,\"ACME\")")
                        .build());

        final var response = dittoProtocolClient.send(modifyAttribute).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    public void conditionallyDeleteFeatureProperty() {
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();
        final var featureId = "Vehicle";
        final var propertyName = "fault";

        final var deleteFeatureProperty = DeleteFeatureProperty.of(thingId,
                featureId,
                JsonPointer.of(propertyName),
                DittoHeaders.newBuilder()
                        .correlationId(baseCorrelationId)
                        .responseRequired(true)
                        .condition("eq(features/Vehicle/properties/status/running,true)")
                        .build());

        final var response = dittoProtocolClient.send(deleteFeatureProperty).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);

        final var featurePropertyOptional = thingsHttpClient.getFeatureProperty(thingId,
                featureId,
                propertyName,
                baseCorrelationId.withSuffix(".getProperty"));

        assertThat(featurePropertyOptional).as("feature property was deleted").isEmpty();
    }

    @Test
    public void getAttributeWhenConditionEvaluatesToTrue() {
        final var retrieveAttribute = RetrieveAttribute.of(thingId,
                JsonPointer.of("model"),
                DittoHeaders.newBuilder()
                        .correlationId(testNameCorrelationId.getCorrelationId())
                        .responseRequired(true)
                        .condition("eq(attributes/manufacturer,\"ACME\")")
                        .build());

        final var response = dittoProtocolClient.send(retrieveAttribute).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void getAttributeWhenConditionEvaluatesToFalse() {
        final var retrieveAttribute = RetrieveAttribute.of(thingId,
                JsonPointer.of("model"),
                DittoHeaders.newBuilder()
                        .correlationId(testNameCorrelationId.getCorrelationId())
                        .responseRequired(true)
                        .condition("eq(attributes/manufacturer,\"FOO\")")
                        .build());

        final var response = dittoProtocolClient.send(retrieveAttribute).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.PRECONDITION_FAILED);
    }

    @Test
    public void getFeaturesWithSortCondition() {
        final var dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(testNameCorrelationId.getCorrelationId())
                .responseRequired(true)
                .condition("sort(+featureId)")
                .build();
        final var retrieveFeatures = RetrieveFeatures.of(thingId, dittoHeaders);

        final var response = dittoProtocolClient.send(retrieveFeatures).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    public void putAttributeWithExistsConditionAndMissingReadPermission() {
        final var policyId = PolicyId.of(thingId);
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        final var policy = getPolicy(policyId, baseCorrelationId.withSuffix(".getPolicy"));
        final var adjustedPolicy = PoliciesModelFactory.newPolicyBuilder(policy.orElseThrow())
                .setRevokedPermissionsFor("CONNECTION", "thing", "/attributes/manufacturer", "READ")
                .build();
        putPolicy(policyId, adjustedPolicy, baseCorrelationId.withSuffix(".putAdjustedPolicy"));

        final var modifyAttribute = ModifyAttribute.of(thingId,
                JsonPointer.of("manufacturer"),
                JsonFactory.newValue("Bosch IO"),
                DittoHeaders.newBuilder()
                        .correlationId(testNameCorrelationId.getCorrelationId())
                        .responseRequired(true)
                        .condition("exists(attributes/manufacturer)")
                        .build());

        final var response = dittoProtocolClient.send(modifyAttribute).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.PRECONDITION_FAILED);
    }

    @Test
    public void putAttributeWithNotExistsConditionAndMissingReadPermission() {
        final var policyId = PolicyId.of(thingId);
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        final var policy = getPolicy(policyId, baseCorrelationId.withSuffix(".getPolicy"));
        final var adjustedPolicy = PoliciesModelFactory.newPolicyBuilder(policy.orElseThrow())
                .setRevokedPermissionsFor("CONNECTION", "thing", "/attributes/manufacturer", "READ")
                .build();
        putPolicy(policyId, adjustedPolicy, baseCorrelationId.withSuffix(".putAdjustedPolicy"));

        final var modifyAttribute = ModifyAttribute.of(thingId,
                JsonPointer.of("manufacturer"),
                JsonFactory.newValue("Bosch IO"),
                DittoHeaders.newBuilder()
                        .correlationId(baseCorrelationId)
                        .responseRequired(true)
                        .condition("not(exists(attributes/manufacturer))")
                        .build());

        final var response = dittoProtocolClient.send(modifyAttribute).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.PRECONDITION_FAILED);
    }

    @Test
    public void getThingWithConditionalRequestAndConditionHeaderBothEvaluatingToTrue() {
        final var dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(testNameCorrelationId.getCorrelationId())
                .responseRequired(true)
                .condition("eq(attributes/manufacturer,\"ACME\")")
                .ifMatch(EntityTagMatchers.fromStrings("\"rev:1\""))
                .build();
        final var retrieveFeatures = RetrieveThing.of(thingId, dittoHeaders);

        final var response = dittoProtocolClient.send(retrieveFeatures).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void getThingWithConditionalRequestEvaluatingToFalseAndConditionHeaderEvaluatingToTrue() {
        final var dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(testNameCorrelationId.getCorrelationId())
                .responseRequired(true)
                .condition("eq(attributes/manufacturer,\"ACME\")")
                .ifMatch(EntityTagMatchers.fromStrings("\"rev:23\""))
                .build();
        final var retrieveFeatures = RetrieveThing.of(thingId, dittoHeaders);

        final var response = dittoProtocolClient.send(retrieveFeatures).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.PRECONDITION_FAILED);
    }

    @Test
    public void getFeatureWithEqualConditionOnMetadata() {
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();
        putMetadata(thingId, baseCorrelationId.withSuffix(".putMetadata"));

        final var dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(baseCorrelationId)
                .responseRequired(true)
                .condition("eq(_metadata/features/EnvironmentScanner/properties/sensorType,\"self-pushing\")")
                .build();
        final var retrieveFeature = RetrieveFeature.of(thingId, "EnvironmentScanner", dittoHeaders);

        final var response = dittoProtocolClient.send(retrieveFeature).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void getFeatureWithExistsConditionOnMetadata() {
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();
        putMetadata(thingId, baseCorrelationId.withSuffix(".putMetadata"));

        final var dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(baseCorrelationId)
                .responseRequired(true)
                .condition("exists(_metadata/features/EnvironmentScanner/properties/sensorType)")
                .build();
        final var retrieveFeature = RetrieveFeature.of(thingId, "EnvironmentScanner", dittoHeaders);

        final var response = dittoProtocolClient.send(retrieveFeature).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.OK);
    }

    private CommandResponse<?> sendModifyFeatureWithCondition(final String condition) {
        final var modifyFeature = ModifyFeature.of(thingId,
                ThingsModelFactory.newFeatureBuilder()
                        .properties(ThingsModelFactory.newFeaturePropertiesBuilder()
                                .set("temperature", 10.2)
                                .build())
                        .withId("EnvironmentScanner")
                        .build(),
                DittoHeaders.newBuilder()
                        .correlationId(testNameCorrelationId.getCorrelationId())
                        .responseRequired(true)
                        .condition(condition)
                        .build());

        return dittoProtocolClient.send(modifyFeature).join();
    }

    private static Optional<Policy> getPolicy(final PolicyId policyId, final CorrelationId correlationId) {
        final var policiesClient = POLICIES_HTTP_CLIENT_RESOURCE.getPoliciesClient();
        return policiesClient.getPolicy(policyId, correlationId);
    }

    private static void putPolicy(final PolicyId policyId, final Policy policy, final CorrelationId correlationId) {
        final var policiesClient = POLICIES_HTTP_CLIENT_RESOURCE.getPoliciesClient();
        policiesClient.putPolicy(policyId, policy, correlationId);
    }

    private static Connection getAmqpConnection(final String connectionName,
            final URI uri,
            final String sourceAddress,
            final String replyAddress) {

        final var connectionId = ConnectionId.of("connection-" + UUID.randomUUID());

        final var connectionBuilder = ConnectivityModelFactory.newConnectionBuilder(connectionId,
                ConnectionType.AMQP_10,
                ConnectivityStatus.OPEN,
                uri.toString());

        return connectionBuilder.name(connectionName)
                .specificConfig(Map.of("jms.closeTimeout", "0"))
                .sources(List.of(ConnectivityModelFactory.newSourceBuilder()
                        .authorizationContext(getAuthorizationContext(connectionName))
                        .address(sourceAddress)
                        .headerMapping(ConnectivityModelFactory.newHeaderMapping(Map.of("reply-to",
                                "{{header:reply-to}}")))
                        .replyTarget(ReplyTarget.newBuilder()
                                .address(replyAddress)
                                .expectedResponseTypes(Set.of(ResponseType.RESPONSE, ResponseType.ERROR))
                                .build())
                        .build()))
                .build();
    }

    private static AuthorizationContext getAuthorizationContext(final String connectionName) {

        return AuthorizationContext.newInstance(DittoAuthorizationContextType.PRE_AUTHENTICATED_CONNECTION,
                getAuthorizationSubject(connectionName));
    }

    private static AuthorizationSubject getAuthorizationSubject(final String connectionName) {

        return AuthorizationSubject.newInstance(MessageFormat.format("integration:{0}",
                connectionName));
    }

    private static void putMetadata(final ThingId thingId, final CorrelationId correlationId) {

        final var dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(correlationId)
                .responseRequired(true)
                .putMetadata(
                        MetadataHeaderKey.parse("/features/EnvironmentScanner/properties/sensorType"),
                        JsonValue.of("self-pushing"))
                .build();

        final var modifyWithMetadata = ModifyThing.of(thingId, getThing(), null, dittoHeaders);
        final var response = dittoProtocolClient.send(modifyWithMetadata).join();

        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
    }

}
