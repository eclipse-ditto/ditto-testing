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
package org.eclipse.ditto.testing.system.things.sse;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.asynchttpclient.AsyncHttpClient;

/**
 * Driver for SSE-Tests.
 */
final class SseTestDriver {

    private static final Duration WAIT_DURATION = Duration.ofSeconds(30);

    private final AsyncHttpClient client;
    private final String url;
    private final String authorization;
    private final int expectedMessagesCount;

    private volatile CountDownLatch latch = null;
    private volatile List<SseTestHandler.Message> resultMessages;
    private volatile Throwable resultThrowable;

    SseTestDriver(final AsyncHttpClient client, final String url, final String authorization,
            final int expectedMessagesCount) {
        this.client = requireNonNull(client);
        this.url = requireNonNull(url);
        this.authorization = requireNonNull(authorization);
        this.expectedMessagesCount = expectedMessagesCount;
    }

    synchronized void connect() {
        if (latch != null) {
            throw new IllegalStateException("Already connected");
        }

        latch = new CountDownLatch(1);

        final CompletableFuture<List<SseTestHandler.Message>> future =
                client.prepareGet(url)
                        .setHeader("Authorization", authorization)
                        .addHeader("Accept", "text/event-stream")
                        .addHeader("Cache-Control", "no-cache")
                        .execute(new SseTestHandler(expectedMessagesCount))
                        .toCompletableFuture();
        future.whenComplete(this::handleResult);

        // wait a little until SSE is established in backend or until session termination
        final CompletableFuture<Void> waitFuture = new CompletableFuture<>();
        future.whenComplete((result, error) -> {
            if (error != null) {
                waitFuture.completeExceptionally(error);
            } else {
                waitFuture.complete(null);
            }
        });
        // wait for cluster pubsub
        waitFuture.completeOnTimeout(null, 15L, TimeUnit.SECONDS);
        waitFuture.join();
    }

    List<SseTestHandler.Message> getMessages() {
        if (latch == null) {
            throw new IllegalStateException("Not connected");
        }

        final boolean finished;
        try {
            finished = latch.await(WAIT_DURATION.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }

        if (!finished) {
            throw new AssertionError("Did not retrieve the expected " + expectedMessagesCount +
                    " messages after " + WAIT_DURATION + ".");
        }

        if (resultThrowable != null) {
            throw new IllegalStateException(resultThrowable);
        }

        return requireNonNull(resultMessages);
    }

    private void handleResult(@Nullable final List<SseTestHandler.Message> messages, @Nullable final Throwable throwable) {
        this.resultMessages = messages;
        this.resultThrowable = throwable;
        latch.countDown();
    }

}
