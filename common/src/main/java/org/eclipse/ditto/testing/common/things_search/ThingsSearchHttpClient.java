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
package org.eclipse.ditto.testing.common.things_search;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.net.URI;
import java.text.MessageFormat;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.apache.http.HttpStatus;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLogger;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLoggerFactory;
import org.eclipse.ditto.json.JsonRuntimeException;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.authentication.AuthenticationSetter;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.restassured.RestAssuredJsonValueMapper;
import org.eclipse.ditto.testing.common.restassured.UnexpectedResponseException;
import org.eclipse.ditto.thingsearch.model.SearchModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchResult;
import org.hamcrest.Matchers;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.Filter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

/**
 * A client for the Things search HTTP API.
 */
@Immutable
public final class ThingsSearchHttpClient {

    private static final String THINGS_SEARCH_RESOURCE_PATH = "./search/things";

    private static final DittoLogger LOGGER = DittoLoggerFactory.getLogger(ThingsSearchHttpClient.class);

    private final URI thingsSearchBaseUri;
    private final AuthenticationSetter authenticationSetter;

    private ThingsSearchHttpClient(final URI httpApiUri, final AuthenticationSetter authenticationSetter) {
        thingsSearchBaseUri = httpApiUri.resolve(THINGS_SEARCH_RESOURCE_PATH);
        this.authenticationSetter = authenticationSetter;
    }

    /**
     * Returns a new instance of {@code ThingsSearchClient}.
     *
     * @param httpApiUri base URI of the HTTP API including the version number.
     * @param authenticationSetter applies the authentication scheme for each request.
     * @return the instance.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public static ThingsSearchHttpClient newInstance(final URI httpApiUri,
            final AuthenticationSetter authenticationSetter) {
        final var result = new ThingsSearchHttpClient(checkNotNull(httpApiUri, "httpApiUri"),
                checkNotNull(authenticationSetter, "authenticationSetter"));
        LOGGER.info("Initialised for endpoint <{}>.", result.thingsSearchBaseUri);
        return result;
    }

    /**
     * List all things the authorized subject is allowed to see.
     * Optionally the search result can be confined using (search) filter predicates.
     *
     * @param searchFilter the filter to confine the search result.
     * @param correlationId allows to track this request within the back-end.
     * @param filter optional filter(s) for the REST assured request.
     * @return the search result.
     * @throws NullPointerException if any argument is {@code null}.
     * @throws UnexpectedResponseException.WrongStatusCode if the response status code is not {@link HttpStatus#SC_OK}.
     * @throws UnexpectedResponseException.WrongBody if the response body does not contain a {@code SearchResult} JSON
     * object.
     */
    public SearchResult searchThings(final ThingsSearchFilter searchFilter,
            final CorrelationId correlationId,
            final Filter... filter) {

        requireSearchFilterNotNull(searchFilter);
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        final var expectedStatusCode = HttpStatus.SC_OK;

        final var response = RestAssured
                .given(getBasicRequestSpec(correlationId, expectedStatusCode, filter)).
                        filter(new ResponseLoggingFilter(Matchers.is(Matchers.not(expectedStatusCode))))
                .when().
                        get("{searchFilter}", searchFilter.toString());

        if (response.getStatusCode() == expectedStatusCode) {
            return getSearchResultOrThrow(response);
        } else {
            throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCode, response);
        }
    }

    private static void requireSearchFilterNotNull(final ThingsSearchFilter searchFilter) {
        checkNotNull(searchFilter, "searchFilter");
    }

    private static void requireCorrelationIdNotNull(final CorrelationId correlationId) {
        checkNotNull(correlationId, "correlationId");
    }

    private static void requireFilterNotNull(final Filter[] filter) {
        checkNotNull(filter, "filter");
    }

    private RequestSpecification getBasicRequestSpec(final CorrelationId correlationId,
            final int expectedStatusCode,
            final Filter[] filters) {

        final var correlationIdHeader = correlationId.toHeader();
        final var requestSpecification = new RequestSpecBuilder()
                .setBaseUri(thingsSearchBaseUri)
                .addHeader(correlationIdHeader.getName(), correlationIdHeader.getValue())
                .addFilter(new ResponseLoggingFilter(Matchers.is(Matchers.not(expectedStatusCode))))
                .addFilters(List.of(filters))
                .build();
        return authenticationSetter.applyAuthenticationSetting(requestSpecification.auth());
    }

    private static SearchResult getSearchResultOrThrow(final Response response) {
        final var responseBodyJsonValue = getResponseBodyAsJsonValue(response);
        if (responseBodyJsonValue.isObject()) {
            try {
                return SearchModelFactory.newSearchResult(responseBodyJsonValue.asObject());
            } catch (final JsonRuntimeException e) {
                final var message = MessageFormat.format("Failed to deserialize {0} from response: {1}",
                        SearchResult.class.getSimpleName(),
                        e.getMessage());
                throw new UnexpectedResponseException.WrongBody(message, e, response);
            }
        } else {
            final var pattern = "Expecting response body to be a {0} JSON object but it was <{1}>.";
            final var message = MessageFormat.format(pattern,
                    SearchResult.class.getSimpleName(),
                    responseBodyJsonValue);
            throw new UnexpectedResponseException.WrongBody(message, response);
        }
    }

    private static JsonValue getResponseBodyAsJsonValue(final Response response) {
        final var responseBody = response.getBody();
        return responseBody.as(JsonValue.class, RestAssuredJsonValueMapper.getInstance());
    }

}
