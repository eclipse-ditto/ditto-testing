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
package org.eclipse.ditto.testing.common.client;

import static java.util.Objects.requireNonNull;
import static org.eclipse.ditto.json.assertions.DittoJsonAssertions.assertThat;
import static org.eclipse.ditto.testing.common.TestConstants.API_V_2;

import javax.annotation.Nullable;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.connectivity.model.signals.commands.ConnectivityCommand;
import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.testing.common.CommonTestConfig;
import org.eclipse.ditto.testing.common.PiggyBackCommander;
import org.eclipse.ditto.testing.common.ResourcePathBuilder;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PostMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.testing.common.matcher.RestMatcherConfigurer;
import org.eclipse.ditto.testing.common.matcher.RestMatcherFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.http.ContentType;
import io.restassured.response.Response;

/**
 * Client for testing connections REST commands.
 */
public final class ConnectionsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionsClient.class);
    private static final String LOGGER_ENTITY_TYPE_CONNECTIONS = "Connections";
    private static final String LOGGER_ENTITY_TYPE_CONNECTION_STATUS = "ConnectionStatus";
    private static final String LOGGER_ENTITY_TYPE_LOGS = "Logs";

    private static final String CONNECTIVITY_TARGET_ACTOR_SELECTOR = "/system/sharding/connection";

    private static final String CONNECTIVITY_COMMAND_ENABLE_CONNECTION_LOGS =
            "connectivity.commands:enableConnectionLogs";
    private static final String CONNECTIVITY_COMMAND_OPEN_CONNECTION = "connectivity.commands:openConnection";
    private static final String CONNECTIVITY_COMMAND_CLOSE_CONNECTION = "connectivity.commands:closeConnection";
    private static final String CONNECTIVITY_COMMAND_RESET_CONNECTION_LOGS =
            "connectivity.commands:resetConnectionLogs";

    @Nullable private static ConnectionsClient instance = null;

    private final RestMatcherFactory restMatcherFactory;
    private final PiggyBackCommander connectivityPiggyBackCommander;

    private ConnectionsClient() {
        restMatcherFactory = new RestMatcherFactory(RestMatcherConfigurer.empty());
        connectivityPiggyBackCommander = PiggyBackCommander.getInstance("connectivity");
    }

    /**
     * Returns an instance of ConnectionsClient.
     *
     * @return the client
     */
    public static ConnectionsClient getInstance() {
        ConnectionsClient result = instance;
        if (null == result) {
            result = new ConnectionsClient();
            instance = result;
        }
        return result;
    }

    /**
     * Wraps a post request for testing a connection.
     *
     * @param connectionJson the connection JSON.
     * @return the wrapped request.
     */
    public PostMatcher testConnection(final JsonObject connectionJson) {

        requireNonNull(connectionJson);

        return postConnection(connectionJson)
                .withParam("dry-run", String.valueOf(true));
    }

    /**
     * Wraps a get request for the Connections.
     *
     * @return the wrapped request.
     */
    public GetMatcher getConnections() {
        final String path = ResourcePathBuilder.connections().toString();
        final String connectionsUrl = buildApiUrl(API_V_2, path);

        return restMatcherFactory.get(connectionsUrl).withLogging(LOGGER, LOGGER_ENTITY_TYPE_CONNECTIONS);
    }

    /**
     * Wraps a get request for the Connections with the passed fieldSelector.
     *
     * @param fields the fields to select from the connections
     * @return the wrapped request.
     */
    public GetMatcher getConnections(final JsonFieldSelector fields) {
        final String path = ResourcePathBuilder.connections().toString();
        final String connectionsUrl = buildApiUrl(API_V_2, path);

        return restMatcherFactory.get(connectionsUrl)
                .withParam("fields", fields.toString())
                .withLogging(LOGGER, LOGGER_ENTITY_TYPE_CONNECTIONS);
    }

    /**
     * Wraps a get request for all Connection IDs.
     *
     * @return the wrapped request.
     */
    public GetMatcher getConnectionIds() {
        final String path = ResourcePathBuilder.connections().toString();
        final String connectionsUrl = buildApiUrl(API_V_2, path);

        return restMatcherFactory.get(connectionsUrl)
                .withParam("ids-only", "true")
                .withLogging(LOGGER, LOGGER_ENTITY_TYPE_CONNECTIONS);
    }


    /**
     * Wraps a get request for a connection.
     *
     * @param connectionId the connection ID.
     * @return the wrapped request.
     */
    public GetMatcher getConnection(final CharSequence connectionId) {

        requireNonNull(connectionId);

        final String path =
                ResourcePathBuilder.connections().connection(connectionId).toString();
        final String connectionUrl = buildApiUrl(API_V_2, path);

        return restMatcherFactory.get(connectionUrl).withLogging(LOGGER, LOGGER_ENTITY_TYPE_CONNECTIONS);
    }

    /**
     * Wraps a get request for the connection status of a given connection ID.
     *
     * @param connectionId the connection ID.
     * @return the wrapped request.
     */
    public GetMatcher getConnectionStatus(final CharSequence connectionId) {

        requireNonNull(connectionId);

        final String path =
                ResourcePathBuilder.connections().connection(connectionId).status().toString();
        final String connectionUrl = buildApiUrl(API_V_2, path);

        return restMatcherFactory.get(connectionUrl).withLogging(LOGGER, LOGGER_ENTITY_TYPE_CONNECTION_STATUS);
    }

    /**
     * Get the logs of a connection.
     *
     * @param connectionId the connection.
     * @return the wrapped request.
     */
    public GetMatcher getConnectionLogs(final CharSequence connectionId) {
        requireNonNull(connectionId);

        final String path =
                ResourcePathBuilder.connections().connection(connectionId).logs().toString();
        final String connectionsUrl = buildApiUrl(API_V_2, path);

        return restMatcherFactory.get(connectionsUrl).withLogging(LOGGER, LOGGER_ENTITY_TYPE_LOGS);
    }

    /**
     * Wraps a delete request for a connection.
     *
     * @param connectionId the connection ID.
     * @return the wrapped request.
     */
    public DeleteMatcher deleteConnection(final CharSequence connectionId) {

        requireNonNull(connectionId);

        final String path =
                ResourcePathBuilder.connections().connection(connectionId).toString();
        final String connectionUrl = buildApiUrl(API_V_2, path);

        LOGGER.info("Deleting connection with connectionId: {}", connectionId);

        return restMatcherFactory.delete(connectionUrl).withLogging(LOGGER, LOGGER_ENTITY_TYPE_CONNECTIONS);
    }

    /**
     * Wraps a post request for creating a new connection.
     *
     * @param connectionJsonStr the connection JSON.
     * @return the wrapped request.
     */
    public PostMatcher postConnection(final String connectionJsonStr) {

        requireNonNull(connectionJsonStr);

        final String path = ResourcePathBuilder.connections().toString();
        final String connectionsUrl = buildApiUrl(API_V_2, path);

        return restMatcherFactory.post(connectionsUrl, connectionJsonStr)
                .withLogging(LOGGER, LOGGER_ENTITY_TYPE_CONNECTIONS);
    }

    public PutMatcher putConnection(final String connectionId, final JsonObject connection) {
        return putConnection(connectionId, connection.toString());
    }

    /**
     * Wraps a put request for creating a new connection.
     *
     * @param connectionJsonStr the connection JSON.
     * @return the wrapped request.
     */
    public PutMatcher putConnection(final String connectionId, final String connectionJsonStr) {

        requireNonNull(connectionJsonStr);

        final String path =
                ResourcePathBuilder.connections().connection(connectionId).toString();
        final String connectionsUrl = buildApiUrl(API_V_2, path);

        return restMatcherFactory.put(connectionsUrl, connectionJsonStr)
                .withLogging(LOGGER, LOGGER_ENTITY_TYPE_CONNECTIONS);
    }

    public PostMatcher openConnection(final String connectionId) {
        final String path =
                ResourcePathBuilder.connections().connection(connectionId).command().toString();
        final String connectionsUrl = buildApiUrl(API_V_2, path);
        return restMatcherFactory.post(connectionsUrl, ContentType.TEXT,
                CONNECTIVITY_COMMAND_OPEN_CONNECTION.getBytes());
    }

    public PostMatcher closeConnection(final String connectionId) {
        final String path =
                ResourcePathBuilder.connections().connection(connectionId).command().toString();
        final String connectionsUrl = buildApiUrl(API_V_2, path);
        return restMatcherFactory.post(connectionsUrl, ContentType.TEXT,
                CONNECTIVITY_COMMAND_CLOSE_CONNECTION.getBytes());
    }

    public PostMatcher postConnection(final JsonObject connection) {
        return postConnection(connection.toString());
    }

    public PostMatcher enableConnectionLogs(final CharSequence connectionId) {
        final String path =
                ResourcePathBuilder.connections().connection(connectionId).command().toString();
        final String connectionsUrl = buildApiUrl(API_V_2, path);
        return restMatcherFactory.post(connectionsUrl, ContentType.TEXT,
                CONNECTIVITY_COMMAND_ENABLE_CONNECTION_LOGS.getBytes());
    }

    public PostMatcher resetConnectionLogs(final CharSequence connectionId) {
        final String path =
                ResourcePathBuilder.connections().connection(connectionId).command().toString();
        final String connectionsUrl = buildApiUrl(API_V_2, path);
        return restMatcherFactory.post(connectionsUrl, ContentType.TEXT,
                CONNECTIVITY_COMMAND_RESET_CONNECTION_LOGS.getBytes());
    }

    public Response executeCommand(final ConnectivityCommand<?> command,
            final HttpStatus... allowedPiggyCommandResponseStatuses) {

        final var postMatcherBuilder =
                connectivityPiggyBackCommander.preparePost(command, CONNECTIVITY_TARGET_ACTOR_SELECTOR);
        final var headers = DittoHeaders.newBuilder(command.getDittoHeaders())
                .putHeader("is-group-topic", Boolean.TRUE.toString())
                .putHeader("aggregate", Boolean.FALSE.toString())
                .putHeader(DittoHeaderDefinition.DITTO_SUDO.getKey(), Boolean.TRUE.toString())
                .build();
        postMatcherBuilder.withAdditionalHeaders(headers);

        final Response response = postMatcherBuilder.build().fire();
        assertThat(PiggyBackCommander.getPiggybackStatus(response))
                .describedAs(response.body().asString())
                .isIn((Object[]) allowedPiggyCommandResponseStatuses);

        return response;
    }

    private static String buildApiUrl(final int version, final CharSequence apiSubUrl) {
        return CommonTestConfig.getInstance().getGatewayApiUrl(version, apiSubUrl);
    }

}
