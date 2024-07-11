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
import java.util.Optional;

import org.apache.http.HttpStatus;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.CommonTestConfig;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.TestSolutionResource;
import org.eclipse.ditto.testing.common.ThingJsonProducer;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.client.BasicAuth;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.correlationid.TestNameCorrelationId;
import org.eclipse.ditto.testing.common.gateway.GatewayConfig;
import org.eclipse.ditto.testing.common.policies.PoliciesHttpClientResource;
import org.eclipse.ditto.testing.common.things.ThingsHttpClient;
import org.eclipse.ditto.testing.common.things.ThingsHttpClientResource;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.restassured.RestAssured;
import io.restassured.authentication.AuthenticationScheme;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;

/**
 * Thing structure used in the test (see {@link ThingJsonProducer}):
 * <pre>
 * {
 *     "attributes": {
 *         "manufacturer": "ACME",
 *         "make": "Fancy Fab Car",
 *         "model": "Environmental FourWheeler 4711",
 *         "VIN": "0815666337"
 *     },
 *     "features": {
 *         "Vehicle": {
 *             "properties": {
 *                 "configuration": {
 *                     "transmission": {
 *                         "type": "manual",
 *                         "gears": 7
 *                     }
 *                 },
 *                 "status": {
 *                     "running": true,
 *                     "speed": 90,
 *                     "gear": 5
 *                 },
 *                 "fault": {
 *                     "flatTyre": false
 *                 }
 *             }
 *         },
 *         "EnvironmentScanner": {
 *             "properties": {
 *                 "temperature": 20.8,
 *                 "humidity": 73,
 *                 "barometricPressure": 970.7,
 *                 "location": {
 *                     "longitude": 47.68217,
 *                     "latitude": 9.386372
 *                 },
 *                 "altitude": 399
 *             }
 *         }
 *     }
 * }
 * </pre>
 */
public final class ConditionHttpIT {

    private static final TestConfig TEST_CONFIG = TestConfig.getInstance();

    private static final CommonTestConfig COMMON_TEST_CONFIG = CommonTestConfig.getInstance();

    private static final String FEATURE_ID = "EnvironmentScanner";

    @ClassRule(order = 0)
    public static final TestSolutionResource TEST_SOLUTION_RESOURCE = TestSolutionResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 1)
    public static final ThingsHttpClientResource THINGS_HTTP_CLIENT_RESOURCE =
            ThingsHttpClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @ClassRule(order = 1)
    public static final PoliciesHttpClientResource POLICIES_HTTP_CLIENT_RESOURCE =
            PoliciesHttpClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    private static URI thingsBaseUri;
    private static ThingsHttpClient thingsHttpClient;

    @Rule
    public final TestNameCorrelationId testNameCorrelationId = TestNameCorrelationId.newInstance();

    private ThingId thingId;

    @BeforeClass
    public static void beforeClass() {
        final var gatewayConfig = GatewayConfig.of(TEST_CONFIG);
        final var httpUriApi2 = gatewayConfig.getHttpUriApi2();
        thingsBaseUri = httpUriApi2.resolve("./things");
        thingsHttpClient = THINGS_HTTP_CLIENT_RESOURCE.getThingsClient();

        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    }

    @Before
    public void before() {
        final var newThing = postNewThing(getThing());
        thingId = newThing.getEntityId().orElseThrow();
    }

    @After
    public void after() {
        postDeleteThing(thingId);
        deletePolicy(PolicyId.of(thingId), testNameCorrelationId.getCorrelationId().withSuffix(".deleteAdjustedPolicy"));
    }

    private static Thing getThing() {
        final var thingJsonProducer = new ThingJsonProducer();
        return thingJsonProducer.getThing();
    }

    private Thing postNewThing(final Thing thing) {
        return thingsHttpClient.postThing(thing, testNameCorrelationId.getCorrelationId(".postThing"));
    }

    private void postDeleteThing(final ThingId thingId) {
        thingsHttpClient.deleteThing(thingId, testNameCorrelationId.getCorrelationId(".deleteThing"));
    }

    @Test
    public void senLiveMessageWithConditionHeader() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "eq(attributes/manufacturer,\"ACMA\")")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .post("/{thingId}/inbox/messages/{messageSubject}", thingId.toString(), "testMessage")

                .then()
                .statusCode(HttpStatus.SC_PRECONDITION_FAILED);
    }

    @Test
    public void putAttributeWithConditionHeader() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "eq(attributes/manufacturer,\"ACME\")")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putAttributeWithConditionInQuery() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .param(HttpHeader.CONDITION.getName(), "eq(attributes/manufacturer,\"ACME\")")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithConditionHeader() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "gt(features/EnvironmentScanner/properties/temperature,\"20\")")
                .body(JsonValue.of("10.2").toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "temperature")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithConditionHeaderPreConditionFailed() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "lt(features/EnvironmentScanner/properties/temperature,\"20\")")
                .body(JsonValue.of("10.2").toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "temperature")

                .then()
                .statusCode(HttpStatus.SC_PRECONDITION_FAILED);
    }

    @Test
    @Category(Acceptance.class)
    public void putFeaturePropertyWithAndCondition() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(),
                        "and(gt(features/EnvironmentScanner/properties/temperature,\"20\")," +
                                "gt(features/EnvironmentScanner/properties/humidity,\"71\"))")
                .body(JsonValue.of("10.2").toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "temperature")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithOrCondition() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(),
                        "or(gt(features/EnvironmentScanner/properties/temperature,\"20\")," +
                                "gt(features/EnvironmentScanner/properties/humidity,\"74\"))")
                .body(JsonValue.of("10.2").toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "temperature")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithNotCondition() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(),
                        "not(eq(features/EnvironmentScanner/properties/temperature,\"20.1\"))")
                .body(JsonValue.of("10.2").toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "temperature")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putFeatureWithLtCondition() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(),
                        "lt(features/EnvironmentScanner/properties/barometricPressure,\"980.0\")")
                .body(JsonValue.of("1010.2").toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "barometricPressure")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithLeCondition() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(),
                        "le(features/EnvironmentScanner/properties/barometricPressure,\"980.0\")")
                .body(JsonValue.of("1010.2").toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "barometricPressure")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putFeatureWithGtCondition() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(),
                        "gt(features/EnvironmentScanner/properties/barometricPressure,\"960.0\")")
                .body(JsonValue.of("1010.2").toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "barometricPressure")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithGeCondition() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(),
                        "ge(features/EnvironmentScanner/properties/barometricPressure,\"970.7\")")
                .body(JsonValue.of("1010.2").toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "barometricPressure")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithNeCondition() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(),
                        "ne(features/EnvironmentScanner/properties/barometricPressure,\"970.6\")")
                .body(JsonValue.of("1010.2").toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "barometricPressure")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithExistsCondition() {
        final var jsonObject = JsonObject.newBuilder()
                .set("longitude", 97.698541)
                .set("latitude", 12.985421)
                .build();

        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "exists(features/EnvironmentScanner/properties/location)")
                .body(jsonObject.toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "location")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithLikeCondition() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "like(attributes/manufacturer,\"ACME\")")
                .body(JsonValue.of("56").toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "humidity")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithInCondition() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "in(attributes/manufacturer,\"ACME\",\"BUMLUX\",\"Bosch IO\")")
                .body(JsonValue.of("56").toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "humidity")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putFeaturePropertyWithTimestampCondition() {
        thingsHttpClient.putFeatureProperty(thingId,
                FEATURE_ID,
                "lastUpdated",
                JsonValue.of("2021-08-10T15:07:20.398Z"),
                testNameCorrelationId.getCorrelationId(".createFeatureProperty"));

        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(),
                        "gt(features/EnvironmentScanner/properties/lastUpdated,\"2021-08-10T15:07:10.000Z\")")
                .body(JsonValue.of("35.9").toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "temperature")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void putAttributeWithConditionAndMissingReadPermission() {
        final var policyId = PolicyId.of(thingId);
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        final var policy = getPolicy(policyId, baseCorrelationId.withSuffix(".getPolicy"));
        final var adjustedPolicy = PoliciesModelFactory.newPolicyBuilder(policy.orElseThrow())
                .setRevokedPermissionsFor("DEFAULT", "thing", "/attributes", "READ")
                .build();
        putPolicy(policyId, adjustedPolicy, baseCorrelationId.withSuffix(".putAdjustedPolicy"));

        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "eq(attributes/manufacturer,\"ACME\")")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_PRECONDITION_FAILED);
    }

    @Test
    @Category(Acceptance.class)
    public void putAttributeWithConditionAndPartialReadPermission() {
        final var policyId = PolicyId.of(thingId);
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        final var policy = getPolicy(policyId, baseCorrelationId.withSuffix(".getPolicy"));
        final var adjustedPolicy = PoliciesModelFactory.newPolicyBuilder(policy.orElseThrow())
                .setRevokedPermissionsFor("DEFAULT", "thing", "/features/Vehicle/properties/status", "READ")
                .build();
        putPolicy(policyId, adjustedPolicy, baseCorrelationId.withSuffix(".putAdjustedPolicy"));

        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "and(eq(attributes/manufacturer,\"ACME\")," +
                        "eq(features/Vehicle/properties/status/running,\"true\"))")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_PRECONDITION_FAILED);
    }

    @Test
    public void putFeaturePropertyWithInvalidConditionSyntax() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "in(attributes/manufacturer,\"ACME\" \"BUMLUX\",\"Bosch IO\")")
                .body(JsonValue.of("56").toString())

                .when()
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        FEATURE_ID,
                        "humidity")

                .then()
                .statusCode(HttpStatus.SC_BAD_REQUEST);
    }

    @Test
    public void putAttributeWithDoubleSlashInCondition() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "eq(attributes//manufacturer,\"ACME\")")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_BAD_REQUEST);
    }

    @Test
    @Category(Acceptance.class)
    public void conditionallyDeleteFeatureProperty() {
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();
        final var featureId = "Vehicle";
        final var propertyName = "fault";

        RestAssured.given(getBasicThingsRequestSpec(baseCorrelationId.withSuffix(".deleteProperty")))
                .header(HttpHeader.CONDITION.getName(), "eq(features/Vehicle/properties/status/running,true)")

                .when()
                .delete("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        featureId,
                        propertyName)

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        final var featurePropertyOptional = thingsHttpClient.getFeatureProperty(thingId,
                featureId,
                propertyName,
                baseCorrelationId.withSuffix(".getProperty"));

        assertThat(featurePropertyOptional).as("feature property was deleted").isEmpty();
    }

    @Test
    @Category(Acceptance.class)
    public void getAttributeWhenConditionEvaluatesToTrue() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "eq(attributes/manufacturer,\"ACME\")")

                .when()
                .get("/{thingId}/attributes/{attributePath}", thingId.toString(), "model")

                .then()
                .statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void getAttributeWhenConditionEvaluatesToFalse() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "eq(attributes/manufacturer,\"FOO\")")

                .when()
                .get("/{thingId}/attributes/{attributePath}", thingId.toString(), "model")

                .then()
                .statusCode(HttpStatus.SC_PRECONDITION_FAILED);
    }

    @Test
    public void getFeaturesWithSortCondition() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "sort(+featureId)")

                .when()
                .get("/{thingId}/features", thingId.toString())

                .then()
                .statusCode(HttpStatus.SC_BAD_REQUEST);
    }

    @Test
    public void putAttributeWithExistsConditionAndMissingReadPermission() {
        final var policyId = PolicyId.of(thingId);
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        final var policy = getPolicy(policyId, baseCorrelationId.withSuffix(".getPolicy"));
        final var adjustedPolicy = PoliciesModelFactory.newPolicyBuilder(policy.orElseThrow())
                .setRevokedPermissionsFor("DEFAULT", "thing", "/attributes/manufacturer", "READ")
                .build();
        putPolicy(policyId, adjustedPolicy, baseCorrelationId.withSuffix(".putAdjustedPolicy"));

        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "exists(attributes/manufacturer)")
                .body(JsonValue.of("Bar").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), "foo")

                .then()
                .statusCode(HttpStatus.SC_PRECONDITION_FAILED);
    }

    @Test
    public void putAttributeWithNotExistsConditionAndMissingReadPermission() {
        final var policyId = PolicyId.of(thingId);
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        final var policy = getPolicy(policyId, baseCorrelationId.withSuffix(".getPolicy"));
        final var adjustedPolicy = PoliciesModelFactory.newPolicyBuilder(policy.orElseThrow())
                .setRevokedPermissionsFor("DEFAULT", "thing", "/attributes/manufacturer", "READ")
                .build();
        putPolicy(policyId, adjustedPolicy, baseCorrelationId.withSuffix(".putAdjustedPolicy"));

        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.CONDITION.getName(), "not(exists(attributes/manufacturer))")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_PRECONDITION_FAILED);
    }

    @Test
    public void getThingWithConditionalRequestAndConditionHeaderBothEvaluatingToTrue() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header("If-Match", "\"rev:1\"")
                .header("condition", "eq(attributes/manufacturer,\"ACME\")")

                .when()
                .get("/{thingId}", thingId.toString())

                .then()
                .statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void getThingWithConditionalRequestEvaluatingToFalsAndConditionHeaderEvaluatingToTrue() {
        RestAssured.given(getBasicThingsRequestSpec(testNameCorrelationId.getCorrelationId()))
                .header("If-Match", "\"rev:23\"") // is evaluated before "condition" at back-end
                .header("condition", "eq(attributes/manufacturer,\"ACME\")")

                .when()
                .get("/{thingId}", thingId.toString())

                .then()
                .statusCode(HttpStatus.SC_PRECONDITION_FAILED);
    }

    @Test
    public void getFeatureWithEqualConditionOnMetadata() {
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();
        putMetadata(thingId, baseCorrelationId.withSuffix(".putMetadata"));

        RestAssured.given(getBasicThingsRequestSpec(baseCorrelationId))
                .header(HttpHeader.CONDITION.getName(), "eq(_metadata/features/EnvironmentScanner/properties/sensorType,\"self-pushing\")")

                .when()
                .get("/{thingId}/features/EnvironmentScanner", thingId.toString())

                .then()
                .statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void getFeatureWithExistsConditionOnMetadata() {
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();
        putMetadata(thingId, baseCorrelationId.withSuffix(".putMetadata"));

        RestAssured.given(getBasicThingsRequestSpec(baseCorrelationId))
                .header(HttpHeader.CONDITION.getName(), "exists(_metadata/features/EnvironmentScanner/properties/sensorType)")

                .when()
                .get("/{thingId}/features/EnvironmentScanner", thingId.toString())

                .then()
                .statusCode(HttpStatus.SC_OK);
    }

    private static RequestSpecification getBasicThingsRequestSpec(final CorrelationId correlationId) {
        final var correlationIdHeader = correlationId.toHeader();

        final BasicAuth basicAuth = COMMON_TEST_CONFIG.getBasicAuth();
        final AuthenticationScheme auth;
        if (basicAuth.isEnabled()) {
            auth = RestAssured.basic(basicAuth.getUsername(), basicAuth.getPassword());
        } else {
            auth = RestAssured.oauth2(TEST_SOLUTION_RESOURCE.getAccessToken().getToken());
        }

        return new RequestSpecBuilder()
                .setBaseUri(thingsBaseUri)
                .setAuth(auth)
                .setContentType(ContentType.JSON)
                .addHeader(correlationIdHeader.getName(), correlationIdHeader.getValue())
                .build();
    }

    private static Optional<Policy> getPolicy(final PolicyId policyId, final CorrelationId correlationId) {
        final var policiesClient = POLICIES_HTTP_CLIENT_RESOURCE.getPoliciesClient();
        return policiesClient.getPolicy(policyId, correlationId);
    }

    private static void putPolicy(final PolicyId policyId, final Policy policy, final CorrelationId correlationId) {
        final var policiesClient = POLICIES_HTTP_CLIENT_RESOURCE.getPoliciesClient();
        policiesClient.putPolicy(policyId, policy, correlationId);
    }

    private static void deletePolicy(final PolicyId policyId, final CorrelationId correlationId) {
        final var policiesClient = POLICIES_HTTP_CLIENT_RESOURCE.getPoliciesClient();
        policiesClient.deletePolicy(policyId, correlationId);
    }

    private static void putMetadata(final ThingId thingId, final CorrelationId correlationId) {
        final var metaDataArray = JsonFactory.newArrayBuilder()
                .add(JsonFactory.newObject("{\"key\":\"/features/EnvironmentScanner/properties/sensorType\"," +
                        "\"value\":\"self-pushing\"}"))
                .build();

        final var wholeThing = getThing().toJson().set(Thing.JsonFields.ID, thingId.toString());

        RestAssured.given(getBasicThingsRequestSpec(correlationId))
                .header(HttpHeader.PUT_METADATA.getName(), metaDataArray.toString())
                .body(wholeThing.toString())

                .when()
                .put("/{thingId}", thingId.toString())

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

}
