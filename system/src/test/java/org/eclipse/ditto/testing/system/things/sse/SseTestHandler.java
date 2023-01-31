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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpHeaders;

/**
 * Helper for SSE integration testing.
 */
class SseTestHandler implements AsyncHandler<List<SseTestHandler.Message>> {

    private final static Logger LOGGER = LoggerFactory.getLogger(SseTestHandler.class);

    private final static String DATA_PREFIX = "data:";

    private final int expectedAmountOfMessages;
    private final List<Message> receivedMessages;

    private int messageCounter = 0;

    SseTestHandler(final int expectedAmountOfMessages) {

        this.expectedAmountOfMessages = expectedAmountOfMessages;
        receivedMessages = new ArrayList<>();
    }

    @Override
    public State onStatusReceived(final HttpResponseStatus responseStatus) {
        if (responseStatus.getStatusCode() == 200) {
            return State.CONTINUE;
        } else {
            LOGGER.warn("Received HTTP status <{}>, aborting..", responseStatus);
            throw new IllegalStateException("Got HTTP status code <" + responseStatus.getStatusCode() + "> where 200 was expected");
        }
    }

    @Override
    public State onHeadersReceived(final HttpHeaders headers) {
        return State.CONTINUE;
    }

    @Override
    public State onBodyPartReceived(final HttpResponseBodyPart bodyPart) {
        final String body = new String(bodyPart.getBodyPartBytes());
        final String[] lines = body.split("\n");
        final String data = getDataFromBody(lines);
        if (!data.isEmpty()) {
            LOGGER.info("received: {}", data);
            receivedMessages.add(new Message(data, getTypeFromBody(lines)));
            messageCounter++;
        } else {
            LOGGER.debug("received keepalive");
        }

        if (messageCounter >= expectedAmountOfMessages) {
            return State.ABORT;
        } else {
            return State.CONTINUE;
        }
    }

    private static String getDataFromBody(final String[] lines) {
        if (lines.length > 0) {
            return lines[0].trim().replaceFirst(DATA_PREFIX, "");
        }
        throw new IllegalArgumentException("Missing data!");
    }

    @Nullable
    private static String getTypeFromBody(final String[] lines) {
        if (lines.length > 1) {
            return lines[1].trim();
        }
        return null;
    }

    @Override
    public void onThrowable(final Throwable t) {
        LOGGER.warn("Got error: {}", t.getMessage(), t);
    }

    @Override
    public List<Message> onCompleted() {
        return receivedMessages;
    }

    static final class Message {
        final String data;
        @Nullable final String type;

        Message(String data, @Nullable String type) {
            this.data = data;
            this.type = type;
        }

        public String getData() {
            return data;
        }

        public Optional<String> getType() {
            return Optional.ofNullable(type);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Message message = (Message) o;
            return data.equals(message.data) && Objects.equals(type, message.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(data, type);
        }

        @Override
        public String toString() {
            return "Message{" +
                    "data='" + data + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }
    }

}
