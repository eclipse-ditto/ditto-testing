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
package org.eclipse.ditto.testing.common.things;

import static org.eclipse.ditto.base.model.common.ConditionChecker.argumentNotEmpty;
import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.net.URI;
import java.text.MessageFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.http.HttpStatus;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLogger;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLoggerFactory;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonRuntimeException;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.PolicyIdInvalidException;
import org.eclipse.ditto.testing.common.authentication.AuthenticationSetter;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.restassured.RestAssuredJsonValueMapper;
import org.eclipse.ditto.testing.common.restassured.UnexpectedResponseException;
import org.eclipse.ditto.testing.common.util.StringUtil;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.hamcrest.Matchers;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.Filter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

/**
 * A client for the Things HTTP API.
 */
// Add methods as required.
@Immutable
public final class ThingsHttpClient {

    private static final String THINGS_RESOURCE_PATH = "./things";

    private static final DittoLogger LOGGER = DittoLoggerFactory.getLogger(ThingsHttpClient.class);

    private final URI thingsBaseUri;
    private final AuthenticationSetter authenticationSetter;

    private ThingsHttpClient(final URI httpApiUri, final AuthenticationSetter authenticationSetter) {
        thingsBaseUri = httpApiUri.resolve(THINGS_RESOURCE_PATH);
        this.authenticationSetter = authenticationSetter;
    }

    /**
     * Returns a new instance of {@code ThingsHttpClient}.
     *
     * @param httpApiUri base URI of the HTTP API including the version number.
     * @param authenticationSetter applies the authentication scheme for each request.
     * @return the instance.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public static ThingsHttpClient newInstance(final URI httpApiUri, final AuthenticationSetter authenticationSetter) {
        final var result = new ThingsHttpClient(checkNotNull(httpApiUri, "httpApiUri"),
                checkNotNull(authenticationSetter, "authenticationSetter"));
        LOGGER.info("Initialised for endpoint <{}>.", result.thingsBaseUri);
        return result;
    }

    /**
     * Creates or updates the specified thing for the specified thing ID.
     *
     * @param thingId the ID of the thing.
     * @param thing the thing to be put for {@code thingId}.
     * @param correlationId allows to track this request within the back-end.
     * @param filter optional filter(s) for the REST assured request.
     * @return an {@code Optional} containing the created thing or an empty {@code Optional} if the thing was modified.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public Optional<Thing> putThing(final ThingId thingId,
            final Thing thing,
            final CorrelationId correlationId,
            final Filter... filter) {

        requireThingIdNotNull(thingId);
        requireThingNotNull(thing);
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);
        LOGGER.withCorrelationId(correlationId);
        LOGGER.info("Putting thing for ID <{}> …", thingId);

        final var expectedStatusCodes = Set.of(HttpStatus.SC_CREATED, HttpStatus.SC_NO_CONTENT);

        final var response = RestAssured.given(getBasicRequestSpec(correlationId, expectedStatusCodes, filter))
                .contentType(ContentType.JSON)
                .body(thing.toJsonString())
                .put("/{thingId}", thingId.toString());

        try {
            final Optional<Thing> result;
            final var responseStatusCode = response.getStatusCode();
            if (HttpStatus.SC_CREATED == responseStatusCode) {
                final var thingFromResponse = getThingFromResponseOrThrow(response);
                LOGGER.info("Created thing <{}>.", thingFromResponse);
                result = Optional.of(thingFromResponse);
            } else if (HttpStatus.SC_NO_CONTENT == responseStatusCode) {
                LOGGER.info("Modified thing <{}>.", thingId);
                result = Optional.empty();
            } else {
                throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCodes, response);
            }
            return result;
        } finally {
            LOGGER.discardCorrelationId();
        }
    }

    private static void requireThingIdNotNull(final ThingId thingId) {
        checkNotNull(thingId, "thingId");
    }

    private static void requirePolicyIdNotNull(final PolicyId policyId) {
        checkNotNull(policyId, "policyId");
    }

    private static void requireThingNotNull(final Thing thing) {
        checkNotNull(thing, "thing");
    }

    private static void requireCorrelationIdNotNull(final CorrelationId correlationId) {
        checkNotNull(correlationId, "correlationId");
    }

    private static void requireFilterNotNull(final Filter[] filter) {
        checkNotNull(filter, "filter");
    }

    private RequestSpecification getBasicRequestSpec(final CorrelationId correlationId,
            final Set<Integer> expectedStatusCodes,
            final Filter[] filters) {

        final var correlationIdHeader = correlationId.toHeader();
        final var requestSpecification = new RequestSpecBuilder()
                .setBaseUri(thingsBaseUri)
                .addHeader(correlationIdHeader.getName(), correlationIdHeader.getValue())
                .addFilter(new ResponseLoggingFilter(Matchers.not(Matchers.in(expectedStatusCodes))))
                .addFilters(List.of(filters))
                .build();
        return authenticationSetter.applyAuthenticationSetting(requestSpecification.auth());
    }

    private static Thing getThingFromResponseOrThrow(final Response response) {
        final var responseBodyJsonValue = getResponseBodyAsJsonValue(response);
        if (responseBodyJsonValue.isObject()) {
            try {
                return ThingsModelFactory.newThing(responseBodyJsonValue.asObject());
            } catch (final JsonRuntimeException e) {
                final var message = MessageFormat.format("Failed to deserialize {0} from response: {1}",
                        Thing.class.getSimpleName(),
                        e.getMessage());
                throw new UnexpectedResponseException.WrongBody(message, e, response);
            }
        } else {
            final var pattern = "Expected response body to be a {0} JSON object but it was <{1}>.";
            final var message = MessageFormat.format(pattern,
                    Thing.class.getSimpleName(),
                    responseBodyJsonValue);
            throw new UnexpectedResponseException.WrongBody(message, response);
        }
    }

    private static JsonValue getResponseBodyAsJsonValue(final Response response) {
        final var responseBody = response.getBody();
        return responseBody.as(JsonValue.class, RestAssuredJsonValueMapper.getInstance());
    }

    public Thing postThing(final Thing thing, final CorrelationId correlationId, final Filter... filter) {
        return postThingWithInitialPolicy(thing, null, correlationId, filter);
    }

    public Thing postThingWithInitialPolicy(final Thing thing,
            @Nullable final Policy initialPolicy,
            final CorrelationId correlationId,
            final Filter... filter) {

        requireThingNotNull(thing);
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        final JsonObject jsonBody;
        if (null != initialPolicy) {
            final var initialPolicyWithoutIdJsonObject = JsonFactory.newObjectBuilder(initialPolicy.toJson())
                    .remove(Policy.JsonFields.ID)
                    .build();
            jsonBody = thing.toJson().setValue("_policy", initialPolicyWithoutIdJsonObject);
        } else {
            jsonBody = thing.toJson();
        }

        LOGGER.withCorrelationId(correlationId);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Posting thing <{}>.", StringUtil.truncate(thing.toString()));
        }

        final var expectedStatusCode = HttpStatus.SC_CREATED;

        final var response = RestAssured.given(getBasicRequestSpec(correlationId, Set.of(expectedStatusCode), filter))
                .contentType(ContentType.JSON)
                .body(jsonBody.toString())
                .post();

        try {
            if (response.getStatusCode() == expectedStatusCode) {
                final var thingFromResponse = getThingFromResponseOrThrow(response);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Created thing <{}>.", StringUtil.truncate(thingFromResponse.toString()));
                }
                return thingFromResponse;
            } else {
                throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCode, response);
            }
        } finally {
            LOGGER.discardCorrelationId();
        }
    }

    /**
     * Retrieves the thing with the specified {@code ThingId} argument.
     *
     * @param thingId ID of the thing to be retrieved.
     * @param correlationId allows to track this request within the back-end.
     * @param filter optional filter(s) for the REST assured request.
     * @return the retrieved {@code Thing}.
     * @throws NullPointerException if any argument is {@code null}.
     * @throws UnexpectedResponseException.WrongStatusCode if the response status code is not {@link HttpStatus#SC_OK}.
     * @throws UnexpectedResponseException.WrongBody if the response body does not contain a {@code SearchResult} JSON
     * object.
     */
    public Optional<Thing> getThing(final ThingId thingId, final CorrelationId correlationId, final Filter... filter) {
        requireThingIdNotNull(thingId);
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        LOGGER.withCorrelationId(correlationId);
        LOGGER.info("Getting thing <{}> …", thingId);

        final var expectedStatusCodes = Set.of(HttpStatus.SC_OK, HttpStatus.SC_NOT_FOUND);
        final var response = RestAssured.given(getBasicRequestSpec(correlationId, expectedStatusCodes, filter))
                .get("/{thingId}", thingId.toString());

        try {
            final Optional<Thing> result;
            if (HttpStatus.SC_OK == response.getStatusCode()) {
                final var retrievedThing = getThingFromResponseOrThrow(response);
                LOGGER.info("Got thing <{}>.", retrievedThing);
                result = Optional.of(retrievedThing);
            } else if (HttpStatus.SC_NOT_FOUND == response.getStatusCode()) {
                LOGGER.info("Could not get thing <{}>.", thingId);
                result = Optional.empty();
            } else {
                throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCodes, response);
            }
            return result;
        } finally {
            LOGGER.discardCorrelationId();
        }
    }

    public void deleteThing(final ThingId thingId, final CorrelationId correlationId, final Filter... filter) {
        requireThingIdNotNull(thingId);
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        LOGGER.withCorrelationId(correlationId);
        LOGGER.info("Deleting thing <{}> …", thingId);

        try {
            RestAssured.given(getBasicRequestSpec(correlationId, Set.of(HttpStatus.SC_NO_CONTENT), filter))
                    .delete("/{thingId}", thingId.toString());
            LOGGER.info("Deleted thing <{}>.", thingId);
        } finally {
            LOGGER.discardCorrelationId();
        }
    }

    public void deletePolicy(final PolicyId policyId, final CorrelationId correlationId, final Filter... filter) {
        requirePolicyIdNotNull(policyId);
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        LOGGER.withCorrelationId(correlationId);
        LOGGER.info("Deleting policy <{}> …", policyId);

        try {
            RestAssured.given(getBasicRequestSpec(correlationId, Set.of(HttpStatus.SC_NO_CONTENT), filter))
                    .delete("/{policyId}", policyId.toString());
            LOGGER.info("Deleted policy <{}>.", policyId);
        } finally {
            LOGGER.discardCorrelationId();
        }
    }

    public Optional<PolicyId> putPolicyId(final ThingId thingId,
            final PolicyId policyId,
            final CorrelationId correlationId,
            final Filter... filter) {

        requireThingIdNotNull(thingId);
        checkNotNull(policyId, "policyId");
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        LOGGER.withCorrelationId(correlationId).info("Putting policy ID <{}> to thing <{}> …", policyId, thingId);

        final var policyIdAsJsonValue = JsonValue.of(policyId);

        final var expectedStatusCodes = Set.of(HttpStatus.SC_CREATED, HttpStatus.SC_NO_CONTENT);

        final var response = RestAssured.given(getBasicRequestSpec(correlationId, expectedStatusCodes, filter))
                .contentType(ContentType.JSON)
                .body(policyIdAsJsonValue.toString())
                .put("/{thingId}/policyId", thingId.toString());

        final Optional<PolicyId> result;
        final var responseStatusCode = response.getStatusCode();
        if (HttpStatus.SC_NO_CONTENT == responseStatusCode) {
            LOGGER.withCorrelationId(correlationId).info("Updated policy ID of thing <{}> to <{}>.", thingId, policyId);
            result = Optional.empty();
        } else if (HttpStatus.SC_CREATED == responseStatusCode) {
            final var policyIdFromResponse = getPolicyIdFromResponseOrThrow(response);
            LOGGER.withCorrelationId(correlationId)
                    .info("Created policy ID <{}> for thing <{}>.", policyIdFromResponse, thingId);
            result = Optional.of(policyIdFromResponse);
        } else {
            throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCodes, response);
        }
        return result;
    }

    private static PolicyId getPolicyIdFromResponseOrThrow(final Response response) {
        final var responseBody = response.getBody();
        final var responseBodyAdString = responseBody.asString();
        try {
            return PolicyId.of(responseBodyAdString);
        } catch (final PolicyIdInvalidException e) {
            final var message = MessageFormat.format("Failed to deserialize response body <{0}> as {1}: {2}",
                    responseBody,
                    PolicyId.class.getSimpleName(),
                    e.getMessage());
            throw new UnexpectedResponseException.WrongBody(message, e, response);
        }
    }

    public Response putAttribute(
            final ThingId thingId,
            final CharSequence attributePath,
            final JsonValue attributeValue,
            final CorrelationId correlationId,
            final Filter... filter
    ) {
        requireThingIdNotNull(thingId);
        requireAttributePathNotEmpty(attributePath);
        checkNotNull(attributeValue, "attributeValue");
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        LOGGER.withCorrelationId(correlationId);
        LOGGER.info("Putting attribute <{}> at <{}> to thing <{}> …", attributeValue, attributePath, thingId);

        final var expectedStatusCodes = Set.of(HttpStatus.SC_CREATED, HttpStatus.SC_NO_CONTENT);
        final var response = RestAssured.given(getBasicRequestSpec(correlationId, expectedStatusCodes, filter))
                .contentType(ContentType.JSON)
                .body(attributeValue.toString())
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), attributePath.toString());

        if (response.getStatusCode() == HttpStatus.SC_CREATED) {
            LOGGER.info("Created attribute <{}> at <{}> on thing <{}>.", attributeValue, attributePath, thingId);
        } else if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT) {
            LOGGER.info("Updated attribute <{}> at <{}> on thing <{}>.", attributeValue, attributePath, thingId);
        } else {
            throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCodes, response);
        }
        try {
            return response;
        } finally {
            LOGGER.discardCorrelationId();
        }
    }

    private static void requireAttributePathNotEmpty(final CharSequence attributePath) {
        argumentNotEmpty(attributePath, "attributePath");
    }

    public Optional<JsonValue> getAttribute(final ThingId thingId,
            final CharSequence attributePath,
            final CorrelationId correlationId,
            final Filter... filter) {

        requireThingIdNotNull(thingId);
        requireAttributePathNotEmpty(attributePath);
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        LOGGER.withCorrelationId(correlationId);
        LOGGER.info("Getting value of attribute at <{}> of thing <{}> …", attributePath, thingId);

        final var expectedStatusCodes = Set.of(HttpStatus.SC_OK, HttpStatus.SC_NOT_FOUND);

        final var response = RestAssured.given(getBasicRequestSpec(correlationId, expectedStatusCodes, filter))
                .get("/{thingId}/attributes/{attributePath}", thingId.toString(), attributePath.toString());

        try {
            final Optional<JsonValue> result;
            if (HttpStatus.SC_OK == response.getStatusCode()) {
                final var attributeValue = getResponseBodyAsJsonValue(response);
                result = Optional.of(attributeValue);
                LOGGER.info("Got <{}> at <{}> of thing <{}>.", attributeValue, attributePath, thingId);
            } else if (HttpStatus.SC_NOT_FOUND == response.getStatusCode()) {
                LOGGER.info("Could not find value of attribute <{}> of thing <{}>.", attributePath, thingId);
                result = Optional.empty();
            } else {
                throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCodes, response);
            }
            return result;
        } finally {
            LOGGER.discardCorrelationId();
        }
    }

    public org.eclipse.ditto.base.model.common.HttpStatus putFeatureProperty(
            final ThingId thingId,
            final CharSequence featureId,
            final CharSequence propertyPath,
            final JsonValue propertyValue,
            final CorrelationId correlationId,
            final Filter... filter
    ) {
        requireThingIdNotNull(thingId);
        requireFeatureIdNotEmpty(featureId);
        requirePropertyPathNotEmpty(propertyPath);
        checkNotNull(propertyValue, "propertyValue");
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        LOGGER.withCorrelationId(correlationId);
        LOGGER.info("Putting property <{}> at <{}> to feature <{}> of thing <{}> …",
                propertyValue,
                propertyPath,
                featureId,
                thingId);

        final var expectedStatusCodes = Set.of(HttpStatus.SC_CREATED, HttpStatus.SC_NO_CONTENT);
        final var response = RestAssured.given(getBasicRequestSpec(correlationId, expectedStatusCodes, filter))
                .contentType(ContentType.JSON)
                .body(propertyValue.toString())
                .put("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        featureId.toString(),
                        propertyPath.toString());

        final org.eclipse.ditto.base.model.common.HttpStatus result;
        if (response.getStatusCode() == HttpStatus.SC_CREATED) {
            LOGGER.info("Created property <{}> at <{}> on feature <{}> of thing <{}>.",
                    propertyValue,
                    propertyPath,
                    featureId,
                    thingId);
            result = org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
        } else if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT) {
            LOGGER.info("Updated property <{}> at <{}> on feature <{}> of thing <{}>.",
                    propertyValue,
                    propertyPath,
                    featureId,
                    thingId);
            result = org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT;
        } else {
            throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCodes, response);
        }
        try {
            return result;
        } finally {
            LOGGER.discardCorrelationId();
        }
    }

    private static void requireFeatureIdNotEmpty(final CharSequence featureId) {
        argumentNotEmpty(featureId, "featureId");
    }

    private static void requirePropertyPathNotEmpty(final CharSequence propertyPath) {
        argumentNotEmpty(propertyPath, "propertyPath");
    }

    public Optional<JsonValue> getFeatureProperty(
            final ThingId thingId,
            final CharSequence featureId,
            final CharSequence propertyPath,
            final CorrelationId correlationId,
            final Filter... filter
    ) {
        requireThingIdNotNull(thingId);
        requireFeatureIdNotEmpty(featureId);
        requirePropertyPathNotEmpty(propertyPath);
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        LOGGER.withCorrelationId(correlationId);
        LOGGER.info("Getting property value at <{}> from feature <{}> of thing <{}> …",
                propertyPath,
                featureId,
                thingId);

        final var expectedStatusCodes = Set.of(HttpStatus.SC_OK, HttpStatus.SC_NOT_FOUND);
        final var response = RestAssured.given(getBasicRequestSpec(correlationId, expectedStatusCodes, filter))
                .get("/{thingId}/features/{featureId}/properties/{propertyPath}",
                        thingId.toString(),
                        featureId.toString(),
                        propertyPath.toString());

        try {
            final Optional<JsonValue> result;
            if (HttpStatus.SC_OK == response.getStatusCode()) {
                final var propertyValue = getResponseBodyAsJsonValue(response);
                LOGGER.info("Got property value <{}>.", propertyValue);
                result = Optional.of(propertyValue);
            } else if (HttpStatus.SC_NOT_FOUND == response.getStatusCode()) {
                LOGGER.info("Could not find property value at <{}> from feature <{}> of thing <{}>.",
                        propertyPath,
                        featureId,
                        thingId);
                result = Optional.empty();
            } else {
                throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCodes, response);
            }
            return result;
        } finally {
            LOGGER.discardCorrelationId();
        }
    }

    // Add here missing methods as required.

}
