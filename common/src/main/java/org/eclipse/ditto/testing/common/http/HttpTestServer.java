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
package org.eclipse.ditto.testing.common.http;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.http.javadsl.ConnectHttp;
import org.apache.pekko.http.javadsl.Http;
import org.apache.pekko.http.javadsl.ServerBinding;
import org.apache.pekko.http.javadsl.model.HttpRequest;
import org.apache.pekko.http.javadsl.model.HttpResponse;
import org.apache.pekko.japi.function.Function;
import org.apache.pekko.stream.Materializer;
import org.apache.pekko.stream.OverflowStrategy;
import org.apache.pekko.stream.javadsl.Flow;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

/**
 * Test HTTP server with a random port, awaitable requests and settable responses.
 */
@ThreadSafe
public final class HttpTestServer implements AutoCloseable {

    public static final String USERNAME = "httpTestServerUser";
    public static final String PASSWORD = "randomPassword";

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTestServer.class);

    static final String HTTP_PUSH_MOCK_SERVER_PORT_ENV = "HTTP_PUSH_MOCK_SERVER_PORT";

    private final ActorSystem system;
    private final Materializer mat;
    private final Map<String, BlockingQueue<HttpRequest>> requestQueues;
    private final AtomicReference<Function<HttpRequest, CompletableFuture<HttpResponse>>> responseFutureRef;
    private final CompletableFuture<ServerBinding> bindingFuture;
    private final ConcurrentLinkedQueue<String> keyPaths = new ConcurrentLinkedQueue<>();

    private HttpTestServer() {
        system = ActorSystem.create();
        mat = Materializer.matFromSystem(system);
        requestQueues = new ConcurrentSkipListMap<>();
        responseFutureRef = new AtomicReference<>();
        bindingFuture = Http.get(system).bindAndHandle(getHandler(), getConnectHttp(), mat).toCompletableFuture();
    }

    public static HttpTestServer newInstance() {
        return new HttpTestServer();
    }

    @Override
    public void close() throws Exception {
        Await.ready(system.terminate(), Duration.Inf());
    }

    public CompletableFuture<ServerBinding> getBindingFuture() {
        return bindingFuture;
    }

    public ServerBinding getBinding() {
        return bindingFuture.join();
    }

    public int getPort() {
        return getBinding().localAddress().getPort();
    }

    public String getUri(final boolean tunnel, final boolean basicAuth, final String httpTunnel, final String httpHostName) {
        final String host = tunnel ? httpTunnel : httpHostName;

        final StringBuilder stringBuilder = new StringBuilder("http://");
        if (basicAuth) {
            stringBuilder.append(USERNAME).append(":").append(PASSWORD).append("@");
        }
        stringBuilder.append(host).append(":").append(getPort());
        return stringBuilder.toString();
    }

    public HttpRequest awaitNextRequest(final String path) {
        try {
            final String queueName = getQueueName(path);
            LOGGER.info("Waiting for request at queue: {}", queueName);
            return getQueue(queueName).poll(10L, TimeUnit.SECONDS); // TODO replace with waitTimeout parameter
        } catch (final Throwable cause) {
            throw new AssertionError(cause);
        }
    }

    public void setResponseFunction(final Function<HttpRequest, CompletableFuture<HttpResponse>> responseFunction) {
        responseFutureRef.set(responseFunction);
    }

    public void resetServerWithKeyPaths(final String... keyPaths) {
        this.keyPaths.clear();
        this.keyPaths.addAll(Arrays.asList(keyPaths));
        requestQueues.clear();
        responseFutureRef.set(getDefaultResponseFunction());
    }

    public Materializer getMat() {
        return mat;
    }

    private Flow<HttpRequest, HttpResponse, ?> getHandler() {
        // must be called after initialization of requestQueue and responseFuture
        return Flow.<HttpRequest>create()
                .buffer(1, OverflowStrategy.backpressure())
                .mapAsync(1, request -> {
                    final String queueName = getQueueName(request);
                    LOGGER.info("Incoming request is going to queue '{}': {}", queueName, request);
                    getQueue(queueName).offer(request);
                    return responseFutureRef.get().apply(request);
                });
    }

    private BlockingQueue<HttpRequest> getQueue(final String path) {
        return requestQueues.computeIfAbsent(path, k -> new LinkedBlockingQueue<>());
    }

    private ConnectHttp getConnectHttp() {
        final int port = getPortFromEnvOrRandom();
        LOGGER.info("Start HttpTestServer on 0.0.0.0:{}", port);
        return ConnectHttp.toHost("0.0.0.0", port);
    }

    /**
     * Add the option to switch to a static port by setting the corresponding env var, this is necessary when
     * executed on jenkins in a docker-compose network-environment.
     *
     * @return 0 for a random port or the value from the Environment variable.
     */
    private int getPortFromEnvOrRandom() {
        try {
            return Integer.parseInt(System.getenv().get(HTTP_PUSH_MOCK_SERVER_PORT_ENV));
        } catch (Exception e) {
            return 0;
        }
    }

    private String getQueueName(final HttpRequest request) {
        return getQueueName(request.getUri().getPathString());
    }

    private String getQueueName(final String path) {
        final int start = path.startsWith("/") ? 1 : 0;
        final int end = Math.max(start, path.endsWith("/") ? path.length() - 1 : path.length());
        final String candidate = path.substring(start, end);
        return findKeyPath(candidate);
    }

    private String findKeyPath(final String path) {
        return keyPaths.stream().filter(path::contains).findAny().orElse(path);
    }

    private static Function<HttpRequest, CompletableFuture<HttpResponse>> getDefaultResponseFunction() {
        return request -> CompletableFuture.completedFuture(HttpResponse.create());
    }

}
