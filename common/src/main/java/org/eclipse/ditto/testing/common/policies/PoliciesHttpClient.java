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
package org.eclipse.ditto.testing.common.policies;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.net.URI;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import org.apache.http.HttpStatus;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLogger;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLoggerFactory;
import org.eclipse.ditto.json.JsonRuntimeException;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.authentication.AuthenticationSetter;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.restassured.RestAssuredJsonValueMapper;
import org.eclipse.ditto.testing.common.restassured.UnexpectedResponseException;
import org.hamcrest.Matchers;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.Filter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

/**
 * A client for the Policies HTTP API.
 */
// Add methods as required.
@Immutable
public final class PoliciesHttpClient {

    private static final String POLICIES_RESOURCE_PATH = "./policies";

    private static final DittoLogger LOGGER = DittoLoggerFactory.getLogger(PoliciesHttpClient.class);

    private final URI policiesBaseUri;
    private final AuthenticationSetter authenticationSetter;

    private PoliciesHttpClient(final URI httpApiUri, final AuthenticationSetter authenticationSetter) {
        policiesBaseUri = httpApiUri.resolve(POLICIES_RESOURCE_PATH);
        this.authenticationSetter = authenticationSetter;
    }

    /**
     * Returns a new instance of {@code PoliciesClient}.
     *
     * @param httpApiUri base URI of the HTTP API including the version number.
     * @param authenticationSetter applies the authentication scheme for each request.
     * @return the instance.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public static PoliciesHttpClient newInstance(final URI httpApiUri, final AuthenticationSetter authenticationSetter) {
        final var result = new PoliciesHttpClient(checkNotNull(httpApiUri, "httpApiUri"),
                checkNotNull(authenticationSetter, "authenticationSetter"));
        LOGGER.info("Initialised for endpoint <{}>.", result.policiesBaseUri);
        return result;
    }

    /**
     * Creates or updates the specified policy for the specified policy ID.
     *
     * @param policyId the ID of the policy.
     * @param policy the policy to be put for {@code policyId}.
     * @param correlationId allows to track this request within the back-end.
     * @param filter optional filter(s) for the REST assured request.
     * @return an {@code Optional} containing the created policy or an empty {@code Optional} if the policy was
     * modified.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public Optional<Policy> putPolicy(final PolicyId policyId,
            final Policy policy,
            final CorrelationId correlationId,
            final Filter... filter) {

        requirePolicyIdNotNull(policyId);
        checkNotNull(policy, "policy");
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        LOGGER.withCorrelationId(correlationId);
        LOGGER.info("Putting policy for ID <{}> …", policyId);

        final var expectedStatusCodes = Set.of(HttpStatus.SC_CREATED, HttpStatus.SC_NO_CONTENT);

        final var response = RestAssured.given(getBasicRequestSpec(correlationId, expectedStatusCodes, filter))
                .contentType(ContentType.JSON)
                .body(policy.toJsonString())
                .put("/{policyId}", policyId.toString());

        try {
            final Optional<Policy> result;
            if (HttpStatus.SC_CREATED == response.getStatusCode()) {
                final var policyFromResponse = getPolicyFromResponseOrThrow(response);
                LOGGER.info("Created policy <{}>.", policyFromResponse);
                result = Optional.of(policyFromResponse);
            } else if (HttpStatus.SC_NO_CONTENT == response.getStatusCode()) {
                LOGGER.info("Modified policy <{}>.", policyId);
                result = Optional.empty();
            } else {
                throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCodes, response);
            }
            return result;
        } finally {
            LOGGER.discardCorrelationId();
        }
    }

    private static void requirePolicyIdNotNull(final PolicyId policyId) {
        checkNotNull(policyId, "policyId");
    }

    private static void requireCorrelationIdNotNull(final CorrelationId correlationId) {
        checkNotNull(correlationId, "correlationId");
    }

    private static void requireFilterNotNull(final Filter[] filter) {
        checkNotNull(filter, "filter");
    }

    private RequestSpecification getBasicRequestSpec(final CorrelationId correlationId,
            final Collection<Integer> expectedStatusCodes,
            final Filter[] filters) {

        final var correlationIdHeader = correlationId.toHeader();
        final var requestSpecification = new RequestSpecBuilder()
                .setBaseUri(policiesBaseUri)
                .addHeader(correlationIdHeader.getName(), correlationIdHeader.getValue())
                .addFilter(new ResponseLoggingFilter(Matchers.not(Matchers.in(expectedStatusCodes))))
                .addFilters(List.of(filters))
                .build();
        return authenticationSetter.applyAuthenticationSetting(requestSpecification.auth());
    }

    private static Policy getPolicyFromResponseOrThrow(final Response response) {
        final var responseBodyJsonValue = getResponseBodyAsJsonValue(response);
        if (responseBodyJsonValue.isObject()) {
            try {
                return PoliciesModelFactory.newPolicy(responseBodyJsonValue.asObject());
            } catch (final JsonRuntimeException e) {
                final var message = MessageFormat.format("Failed to deserialize {0} from response: {1}",
                        Policy.class.getSimpleName(),
                        e.getMessage());
                throw new UnexpectedResponseException.WrongBody(message, e, response);
            }
        } else {
            final var pattern = "Expecting response body to be a {0} JSON object but it was <{1}>.";
            final var message = MessageFormat.format(pattern,
                    Policy.class.getSimpleName(),
                    responseBodyJsonValue);
            throw new UnexpectedResponseException.WrongBody(message, response);
        }
    }

    private static JsonValue getResponseBodyAsJsonValue(final Response response) {
        final var responseBody = response.getBody();
        return responseBody.as(JsonValue.class, RestAssuredJsonValueMapper.getInstance());
    }

    public Optional<Policy> getPolicy(final PolicyId policyId,
            final CorrelationId correlationId,
            final Filter... filter) {

        requirePolicyIdNotNull(policyId);
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        LOGGER.withCorrelationId(correlationId);
        LOGGER.info("Retrieving policy <{}> …", policyId);

        final var expectedStatusCodes = Set.of(HttpStatus.SC_OK, HttpStatus.SC_NOT_FOUND);
        final var response = RestAssured.given(getBasicRequestSpec(correlationId, expectedStatusCodes, filter))
                .get("/{policyId}", policyId.toString());

        try {
            final Optional<Policy> result;
            if (HttpStatus.SC_OK == response.getStatusCode()) {
                final var retrievedPolicy = getPolicyFromResponseOrThrow(response);
                LOGGER.info("Got policy <{}>.", retrievedPolicy);
                result = Optional.of(retrievedPolicy);
            } else if (HttpStatus.SC_NOT_FOUND == response.getStatusCode()) {
                LOGGER.info("Could not find policy <{}>.", policyId);
                result = Optional.empty();
            } else {
                throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCodes, response);
            }
            return result;
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

    public Response putEntry(final PolicyId policyId,
            final PolicyEntry policyEntry,
            final CorrelationId correlationId,
            final Filter... filter) {

        requirePolicyIdNotNull(policyId);
        checkNotNull(policyEntry, "policyEntry");
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        final var expectedStatusCode = HttpStatus.SC_OK;

        final var response = RestAssured.given(getBasicRequestSpec(correlationId, Set.of(expectedStatusCode), filter))
                .contentType(ContentType.JSON)
                .body(policyEntry.toJson())
                .put("/{policyId}/entries/{label}", policyId.toString(), String.valueOf(policyEntry.getLabel()));

        if (response.getStatusCode() == expectedStatusCode) {
            return response;
        } else {
            throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCode, response);
        }
    }

}
