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

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonObjectBuilder;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.testing.common.http.HttpTestServer;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityWorker;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.headers.ContentType;

/**
 * Worker for {@link HttpPushConnectivityIT}.
 */
public final class HttpPushConnectivityWorker
        extends AbstractConnectivityWorker<String, HttpRequest> {

    private static final String CORRELATION_ID_HEADER = "correlation-id";

    static final Map<String, String> CORRELATION_ID_MAP =
            Collections.singletonMap(CORRELATION_ID_HEADER, "{{header:correlation-id}}");

    private final HttpTestServer server;

    private HttpPushConnectivityWorker(final HttpTestServer server) {
        super(LoggerFactory.getLogger(HttpPushConnectivityWorker.class));
        this.server = server;
    }

    static HttpPushConnectivityWorker of(final HttpTestServer server) {
        return new HttpPushConnectivityWorker(server);
    }

    @Override
    protected String initResponseConsumer(final String connectionName, final String correlationId) {
        return connectionName;
    }

    @Override
    protected String initTargetsConsumer(final String connectionName) {
        return connectionName;
    }

    @Override
    protected String initTargetsConsumer(final String connectionName, final String t) {
        return connectionName;
    }

    @Override
    protected HttpRequest consumeResponse(final String correlationId, final String path) {
        return server.awaitNextRequest(path);
    }

    @Override
    protected CompletableFuture<HttpRequest> consumeResponseInFuture(final String correlationId, final String path) {
        return CompletableFuture.supplyAsync(() -> server.awaitNextRequest(path));
    }

    @Override
    protected HttpRequest consumeFromTarget(final String connectionName, final String targetsConsumer) {
        return server.awaitNextRequest(connectionName);
    }

    @Override
    protected CompletableFuture<HttpRequest> consumeFromTargetInFuture(final String connectionName,
            final String unused) {
        return CompletableFuture.supplyAsync(() -> server.awaitNextRequest(connectionName));
    }

    @Override
    protected String getCorrelationId(final HttpRequest message) {
        return message.getHeader(CORRELATION_ID_HEADER).map(HttpHeader::value).orElse(null);
    }

    @Override
    protected void sendAsJsonString(final String connectionName, final String correlationId, final String stringMessage,
            final Map<String, String> headersMap) {
        // workaround to set the http response to send back to a http request
        final int statusCode = Integer.parseInt(headersMap.getOrDefault("status", "200"));
        final String contentTypeStr = headersMap.getOrDefault("content-type", "application/json");

        server.setResponseFunction(request -> {
            final String requestPayload = textFrom(request);
            logger.info("Got HTTP request payload: {}", requestPayload);
            return CompletableFuture.completedFuture(HttpResponse.create()
                    .withStatus(statusCode)
                    .addHeader(HttpHeader.parse("x-correlation-id", correlationId))
                    .withEntity(HttpEntities.create(ContentTypes.parse(contentTypeStr),
                            stringMessage.getBytes(Charset.defaultCharset())))
            );
        });
    }

    @Override
    protected void sendAsBytePayload(final String connectionName, final String correlationId, final byte[] byteMessage,
            final Map<String, String> headersMap) {
        // workaround to set the http response to send back to a http request
        final int statusCode = Integer.parseInt(headersMap.getOrDefault("status", "200"));
        final String contentTypeStr = headersMap.getOrDefault("content-type", "application/json");

        server.setResponseFunction(request -> {
            final String requestPayload = textFrom(request);
            logger.info("Got HTTP request payload: {}", requestPayload);
            return CompletableFuture.completedFuture(HttpResponse.create()
                    .withStatus(statusCode)
                    .addHeader(HttpHeader.parse("x-correlation-id", correlationId))
                    .withEntity(HttpEntities.create(ContentTypes.parse(contentTypeStr), byteMessage))
            );
        });
    }

    @Override
    protected JsonifiableAdaptable jsonifiableAdaptableFrom(final HttpRequest message) {
        return jsonifiableAdaptableFrom(textFrom(message));
    }

    @Override
    protected String textFrom(final HttpRequest message) {
        final long timeout = 5000L;
        return message.entity().toStrict(timeout, server.getMat()).toCompletableFuture().join().getData().utf8String();
    }

    @Override
    protected byte[] bytesFrom(final HttpRequest message) {
        final long timeout = 5000L;
        return message.entity().toStrict(timeout, server.getMat()).toCompletableFuture().join().getData().toArray();
    }

    @Override
    protected Map<String, String> checkHeaders(final HttpRequest message, final Consumer<JsonObject> verifyBlockedHeaders) {
        final Map<String, String> headers = new HashMap<>();
        final JsonObjectBuilder builder = JsonObject.newBuilder();
        final ContentType contentType = ContentType.create(message.entity().getContentType());
        for (final Iterator<HttpHeader> iterator =
                Iterators.concat(Iterators.singletonIterator(contentType), message.getHeaders().iterator());
                iterator.hasNext(); ) {
            final HttpHeader header = iterator.next();
            final String headerName = header.lowercaseName();
            headers.put(headerName, header.value());
            if (!headerName.equalsIgnoreCase("authorization")) {
                // skip "authorization" (Basic auth) header from being validated - that one is allowed as additional header
                builder.set(headerName, header.value());
            }
        }
        verifyBlockedHeaders.accept(builder.build());
        return headers;
    }

    @Override
    protected BrokerType getBrokerType() {
        return BrokerType.HTTP_PUSH;
    }
}
