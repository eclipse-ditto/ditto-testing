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
package org.eclipse.ditto.testing.common;

import static org.eclipse.ditto.base.model.common.ConditionChecker.argumentNotEmpty;
import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.net.URI;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.concurrent.Immutable;

import org.apache.http.HttpStatus;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLogger;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLoggerFactory;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonRuntimeException;
import org.eclipse.ditto.json.JsonValue;
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
 * A client for the Ditto connections HTTP API.
 */
@Immutable
public final class ConnectionsHttpClient {

    private static final DittoLogger LOGGER = DittoLoggerFactory.getLogger(ConnectionsHttpClient.class);

    private final URI connectionsBaseUri;
    private final AuthenticationSetter authenticationSetter;

    private ConnectionsHttpClient(final URI httpApiUri, final AuthenticationSetter authenticationSetter) {
        connectionsBaseUri = httpApiUri.resolve(".");
        this.authenticationSetter = authenticationSetter;
    }

    /**
     * Returns a new instance of {@code ConnectionsClient}.
     *
     * @param httpApiUri base URI of the HTTP API including the version number.
     * @param authenticationSetter applies the authentication scheme for each request.
     * @return the instance.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public static ConnectionsHttpClient newInstance(final URI httpApiUri,
            final AuthenticationSetter authenticationSetter) {
        final var result = new ConnectionsHttpClient(checkNotNull(httpApiUri, "httpApiUri"),
                checkNotNull(authenticationSetter, "authenticationSetter"));
        LOGGER.info("Initialised for endpoint <{}>.", result.connectionsBaseUri);
        return result;
    }

    private static void requireCorrelationIdNotNull(final CorrelationId correlationId) {
        checkNotNull(correlationId, "correlationId");
    }

    private static void requireConnectionIdNotNull(final ConnectionId connectionId) {
        checkNotNull(connectionId, "connectionId");
    }

    private static void requireFilterNotNull(final Filter[] filter) {
        checkNotNull(filter, "filter");
    }


    private static <T> T deserializeResponseBody(final Response response,
            final Function<JsonObject, T> deserializer,
            final Class<T> targetType) {

        final var responseBodyJsonValue = getResponseBodyAsJsonValue(response);
        if (responseBodyJsonValue.isObject()) {
            try {
                return deserializer.apply(responseBodyJsonValue.asObject());
            } catch (final JsonRuntimeException e) {
                final var message = MessageFormat.format("Failed to deserialize {0} from response: {1}",
                        targetType.getSimpleName(),
                        e.getMessage());
                throw new UnexpectedResponseException.WrongBody(message, e, response);
            }
        } else {
            final var pattern = "Expecting response body to be a {0} JSON object but it was <{1}>.";
            final var message = MessageFormat.format(pattern,
                    targetType.getSimpleName(),
                    responseBodyJsonValue);
            throw new UnexpectedResponseException.WrongBody(message, response);
        }
    }

    private static JsonValue getResponseBodyAsJsonValue(final Response response) {
        final var responseBody = response.getBody();
        return responseBody.as(JsonValue.class, RestAssuredJsonValueMapper.getInstance());
    }

    private RequestSpecification getBasicRequestSpec(final CorrelationId correlationId,
            final Set<Integer> expectedStatusCodes,
            final Filter[] filters) {
        return getBasicRequestSpec(correlationId, expectedStatusCodes, filters, Set.of());
    }

    private RequestSpecification getBasicRequestSpec(final CorrelationId correlationId,
            final Set<Integer> expectedStatusCodes,
            final Filter[] filters,
            final Set<Integer> additionalIgnoredStatusCodes) {

        final var combinedStatusCodes = new HashSet<>(expectedStatusCodes);
        combinedStatusCodes.addAll(additionalIgnoredStatusCodes);
        final var correlationIdHeader = correlationId.toHeader();
        final var requestSpecification = new RequestSpecBuilder()
                .setBaseUri(connectionsBaseUri)
                .addHeader(correlationIdHeader.getName(), correlationIdHeader.getValue())
                .addFilter(new ResponseLoggingFilter(Matchers.not(Matchers.in(combinedStatusCodes))))
                .addFilters(List.of(filters))
                .build();

        return authenticationSetter.applyAuthenticationSetting(requestSpecification.auth());
    }

    /**
     * Creates the connection defined in the JSON body.
     * The ID of the connection will be generated by the backend.
     * Thus the ID of the {@code Connection} argument will be removed before sending the request.
     *
     * @param connection the connection data.
     * @param correlationId allows to track this request within the back-end.
     * @param filter optional filter(s) for the REST assured request.
     * @return the response.
     * @throws NullPointerException if any argument is {@code null}.
     * @throws UnexpectedResponseException if the response did not contain the created {@code Connection}.
     */
    public Connection postConnection(final Connection connection,
            final CorrelationId correlationId,
            final Filter... filter) {

        checkNotNull(connection, "connection");
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        // Connections can only be created via POST. Then, however, the request body must not contain the connection ID.
        final var connectionWithoutIdJsonObject = JsonFactory.newObjectBuilder(connection.toJson())
                .remove(Connection.JsonFields.ID)
                .build();

        LOGGER.withCorrelationId(correlationId)
                .info("Posting connection of type <{}>  …",
                        connection.getConnectionType());

        final var expectedStatusCode = HttpStatus.SC_CREATED;

        final var response = RestAssured
                .given(getBasicRequestSpec(correlationId, Set.of(expectedStatusCode), filter)).
                contentType(ContentType.JSON).
                body(connectionWithoutIdJsonObject.toString())
                .when().
                post("/connections");

        try {
            if (response.getStatusCode() == expectedStatusCode) {
                final var result = getConnectionOrThrow(response);
                LOGGER.info("Created connection of type <{}> with ID <{}>.",
                        result.getConnectionType(),
                        result.getId());
                return result;
            } else {
                throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCode, response);
            }
        } finally {
            LOGGER.discardCorrelationId();
        }
    }

    private static Connection getConnectionOrThrow(final Response response) {
        return deserializeResponseBody(response, ConnectivityModelFactory::connectionFromJson, Connection.class);
    }

    /**
     * Sends the specified command to the specified solution connection.
     * The following commands are supported:
     * <ul>
     *     <li>{@code connectivity.commands:openConnection}</li>
     *     <li>{@code connectivity.commands:closeConnection}</li>
     *     <li>{@code connectivity.commands:resetConnectionMetrics}</li>
     *     <li>{@code connectivity.commands:enableConnectionLogs}</li>
     *     <li>{@code connectivity.commands:resetConnectionLogs}</li>
     * </ul>
     *
     * @param connectionId the ID of the connection.
     * @param command the command.
     * @param correlationId allows to track this request within the back-end.
     * @param filter optional filter(s) for the REST assured request.
     * @throws NullPointerException if any argument is {@code null}.
     * @throws IllegalArgumentException if {@code command} is empty.
     * @throws UnexpectedResponseException.WrongStatusCode if the response status code is not {@link org.apache.http.HttpStatus#SC_OK}.
     */
    public void postConnectionCommand(final ConnectionId connectionId,
            final CharSequence command,
            final CorrelationId correlationId,
            final Filter... filter) {

        requireConnectionIdNotNull(connectionId);
        argumentNotEmpty(command, "command");
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        LOGGER.withCorrelationId(correlationId);
        LOGGER.info("Sending command <{}> to connection <{}> …", command, connectionId);

        final var expectedStatusCode = HttpStatus.SC_OK;

        final var response = RestAssured
                .given(getBasicRequestSpec(correlationId, Set.of(expectedStatusCode), filter))
                .contentType(ContentType.TEXT)
                .body(command.toString())
                .post("/connections/{connectionId}/command",
                        connectionId.toString());

        LOGGER.info("Sent command <{}> to connection <{}>.",
                command,
                connectionId);

        LOGGER.discardCorrelationId();
        if (response.getStatusCode() != expectedStatusCode) {
            throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCode, response);
        }
    }

    public JsonObject getConnectionStatus(final ConnectionId connectionId,
            final CorrelationId correlationId,
            final Filter... filter) {

        requireConnectionIdNotNull(connectionId);
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        LOGGER.withCorrelationId(correlationId);
        LOGGER.info("Getting status for connection <{}> …", connectionId);

        final var expectedStatusCode = HttpStatus.SC_OK;

        final var response = RestAssured
                .given(getBasicRequestSpec(correlationId, Set.of(expectedStatusCode), filter, Set.of(HttpStatus.SC_NOT_FOUND)))
                .when()
                .get("/connections/{connectionId}/status", connectionId.toString());

        try {
            if (response.getStatusCode() == expectedStatusCode) {
                try {
                    return getResponseBodyToJsonObject(response);
                } finally {
                    LOGGER.info("Got status for connection <{}>.", connectionId);
                }
            } else {
                throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCode, response);
            }
        } finally {
            LOGGER.discardCorrelationId();
        }
    }

    /**
     * Returns the logs of the connection identified by the specified {@code ConnectionId} path
     * argument.
     *
     * @param connectionId ID of the connection to get the logs for.
     * @param correlationId allows to track this request within the back-end.
     * @param filter optional filter(s) for the REST assured request.
     * @return the response body.
     * @throws UnexpectedResponseException if the response did not contain the connection logs response.
     */
    public JsonObject getConnectionLogs(final ConnectionId connectionId,
            final CorrelationId correlationId,
            final Filter... filter) {

        requireConnectionIdNotNull(connectionId);
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        LOGGER.withCorrelationId(correlationId)
                .info("Getting logs for connection <{}> …", connectionId);

        final var expectedStatusCode = HttpStatus.SC_OK;

        final var response = RestAssured
                .given(getBasicRequestSpec(correlationId, Set.of(expectedStatusCode), filter))
                .get("/connections/{connectionId}/logs", connectionId.toString());

        if (response.getStatusCode() == expectedStatusCode) {
            try {
                return getResponseBodyToJsonObject(response);
            } finally {
                LOGGER.withCorrelationId(correlationId)
                        .info("Got logs for connection <{}>.", connectionId);
            }
        } else {
            throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCode, response);
        }
    }

    private static JsonObject getResponseBodyToJsonObject(final Response response) {
        final var responseBodyJsonValue = getResponseBodyAsJsonValue(response);
        if (responseBodyJsonValue.isObject()) {
            return responseBodyJsonValue.asObject();
        } else {
            final var pattern = "Expecting response body to be a JSON object but it was <{1}>.";
            final var message = MessageFormat.format(pattern, responseBodyJsonValue);
            throw new UnexpectedResponseException.WrongBody(message, response);
        }
    }

    public void deleteConnection(final ConnectionId connectionId,
            final CorrelationId correlationId,
            final Filter... filter) {

        requireConnectionIdNotNull(connectionId);
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        LOGGER.withCorrelationId(correlationId);
        LOGGER.info("Deleting connection <{}> …", connectionId);

        final var expectedStatusCode = HttpStatus.SC_NO_CONTENT;

        final var response = RestAssured
                .given(getBasicRequestSpec(correlationId, Set.of(expectedStatusCode), filter))
                .delete("/connections/{connectionId}", connectionId.toString());

        try {
            if (response.getStatusCode() == expectedStatusCode) {
                LOGGER.info("Deleted connection <{}>.", connectionId);
            } else {
                throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCode, response);
            }
        } finally {
            LOGGER.discardCorrelationId();
        }
    }
}
