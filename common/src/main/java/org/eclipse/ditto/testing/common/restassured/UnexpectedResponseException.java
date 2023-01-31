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
package org.eclipse.ditto.testing.common.restassured;

import java.text.MessageFormat;
import java.util.Set;

import javax.annotation.Nullable;

import io.restassured.response.Response;

/**
 * Base class of exceptions that are thrown to indicate that a particular REST Assured response did not match an
 * expectation.
 */
public abstract class UnexpectedResponseException extends RuntimeException {

    private static final long serialVersionUID = -4296151903743048506L;

    private final transient Response unexpectedResponse;

    /**
     * Constructs an {@code UnexpectedResponseException} object.
     *
     * @param message the detail message.
     * @param unexpectedResponse the unexpected response that cause the exception.
     */
    protected UnexpectedResponseException(final String message, final Response unexpectedResponse) {
        super(message);
        this.unexpectedResponse = unexpectedResponse;
    }

    /**
     * Constructs an {@code UnexpectedResponseException} object.
     *
     * @param message the detail message.
     * @param cause the cause of the exception or {@code null} if unknown.
     * @param unexpectedResponse the unexpected response that cause the exception.
     */
    public UnexpectedResponseException(final String message,
            final Throwable cause,
            final Response unexpectedResponse) {

        super(message, cause);
        this.unexpectedResponse = unexpectedResponse;
    }

    /**
     * Returns the unexpected response.
     *
     * @return the unexpected response.
     */
    public Response getUnexpectedResponse() {
        return unexpectedResponse;
    }

    /**
     * This exception is thrown to indicate that the status code of a {@link Response} does not match an expected
     * status code.
     */
    public static final class WrongStatusCode extends UnexpectedResponseException {

        private static final long serialVersionUID = 6832763731374099634L;

        private final Set<Integer> expectedStatusCodes;
        private final int actualStatusCode;

        public WrongStatusCode(final int expectedStatusCode, final Response response) {
            super(getMessage(expectedStatusCode, response.getStatusCode()), response);
            expectedStatusCodes = Set.of(expectedStatusCode);
            actualStatusCode = response.statusCode();
        }

        private static String getMessage(final int expectedStatusCode, final int actualStatusCode) {
            return MessageFormat.format("Expected response to have status code <{0}> but it has <{1}>.",
                    expectedStatusCode,
                    actualStatusCode);
        }

        public WrongStatusCode(final Set<Integer> expectedStatusCodes, final Response response) {
            super(getMessage(expectedStatusCodes, response.getStatusCode()), response);
            this.expectedStatusCodes = Set.copyOf(expectedStatusCodes);
            actualStatusCode = response.statusCode();
        }

        private static String getMessage(final Set<Integer> expectedStatusCodes, final int actualStatusCode) {
            return MessageFormat.format("Expected response to have one of the status codes <{0}> but it has <{1}>.",
                    expectedStatusCodes,
                    actualStatusCode);
        }

        public Set<Integer> getExpectedStatusCodes() {
            return expectedStatusCodes;
        }

        public int getActualStatusCode() {
            return actualStatusCode;
        }
    }

    /**
     * This exception is thrown to indicate that the body of a {@link Response} does not match an expectation.
     */
    public static final class WrongBody extends UnexpectedResponseException {

        private static final long serialVersionUID = 6800251679883710429L;

        /**
         * Constructs a {@code WrongBody} object.
         *
         * @param message the detail message.
         * @param unexpectedResponse the unexpected response that cause the exception.
         */
        public WrongBody(final String message, final Response unexpectedResponse) {
            super(message, unexpectedResponse);
        }

        /**
         * Constructs a {@code WrongBody} object.
         *
         * @param message the detail message.
         * @param cause the cause of the exception or {@code null} if unknown.
         * @param unexpectedResponse the unexpected response that cause the exception.
         */
        public WrongBody(final String message, @Nullable final Throwable cause, final Response unexpectedResponse) {
            super(message, cause, unexpectedResponse);
        }

    }

}
