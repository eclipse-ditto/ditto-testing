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
package org.eclipse.ditto.testing.common.messages;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.net.URI;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.apache.http.HttpStatus;
import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLogger;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLoggerFactory;
import org.eclipse.ditto.testing.common.authentication.AuthenticationSetter;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.restassured.UnexpectedResponseException;
import org.eclipse.ditto.things.model.ThingId;
import org.hamcrest.Matchers;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.Filter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

/**
 * A client for the Things messages HTTP API.
 */
@Immutable
public final class MessagesHttpClient {

    private static final String THINGS_RESOURCE_PATH = "./things";

    private static final DittoLogger LOGGER = DittoLoggerFactory.getLogger(MessagesHttpClient.class);

    private final URI thingsBaseUri;
    private final AuthenticationSetter authenticationSetter;

    private MessagesHttpClient(final URI httpApiUri, final AuthenticationSetter authenticationSetter) {
        thingsBaseUri = httpApiUri.resolve(THINGS_RESOURCE_PATH);
        this.authenticationSetter = authenticationSetter;
    }

    /**
     * Returns a new instance of {@code MessagesClient}.
     *
     * @param httpApiUri base URI of the HTTP API including the version number.
     * @param authenticationSetter applies the authentication scheme for each request.
     * @return the instance.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public static MessagesHttpClient newInstance(final URI httpApiUri, final AuthenticationSetter authenticationSetter) {
        final var result = new MessagesHttpClient(checkNotNull(httpApiUri, "httpApiUri"),
                checkNotNull(authenticationSetter, "authenticationSetter"));
        LOGGER.info("Initialised for endpoint <{}>.", result.thingsBaseUri);
        return result;
    }

    /**
     * Posts a text message to the inbox of the thing with the specified {@code ThingId} argument.
     * The content type of the message is {@link ContentType#TEXT}.
     *
     * @param thingId ID of the thing to send the message to.
     * @param messageSubject the subject of the message.
     * @param messagePayload payload of the message with max size of 250 kB.
     * @param correlationId allows to track this request within the back-end.
     * @param filter optional filter(s) for the REST assured request.
     * @return the raw response.
     * @throws NullPointerException if any argument is {@code null}.
     * @throws org.eclipse.ditto.testing.common.restassured.UnexpectedResponseException.WrongStatusCode if the response status code is not
     * {@link HttpStatus#SC_ACCEPTED}.
     */
    public Response postTextMessageToInbox(final ThingId thingId,
            final URI messageSubject,
            final CharSequence messagePayload,
            final CorrelationId correlationId,
            final Filter... filter) {

        requireThingIdNotNull(thingId);
        requireMessageSubjectNotNull(messageSubject);
        checkNotNull(messagePayload, "messagePayload");
        requireCorrelationIdNotNull(correlationId);
        requireFilterNotNull(filter);

        final var expectedStatusCode = HttpStatus.SC_ACCEPTED;

        final var response = RestAssured
                .given(getBasicRequestSpec(correlationId, expectedStatusCode, filter)).
                        contentType(ContentType.TEXT).
                        body(messagePayload.toString())
                .when().
                        post("/{thingId}/inbox/messages/{messageSubject}",
                                thingId.toString(),
                                messageSubject.toString());

        if (response.getStatusCode() != expectedStatusCode) {
            throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCode, response);
        }
        return response;
    }

    private static void requireThingIdNotNull(final ThingId thingId) {
        checkNotNull(thingId, "thingId");
    }

    private static void requireMessageSubjectNotNull(final URI messageSubject) {
        checkNotNull(messageSubject, "messageSubject");
    }

    private static void requireCorrelationIdNotNull(final CorrelationId correlationId) {
        checkNotNull(correlationId, "correlationId");
    }

    private static void requireFilterNotNull(final Filter[] filter) {
        ConditionChecker.checkNotNull(filter, "filter");
    }

    private RequestSpecification getBasicRequestSpec(final CorrelationId correlationId,
            final int expectedStatusCode,
            final Filter[] filters) {

        final var correlationIdHeader = correlationId.toHeader();
        final var requestSpecification = new RequestSpecBuilder()
                .setBaseUri(thingsBaseUri)
                .addHeader(correlationIdHeader.getName(), correlationIdHeader.getValue())
                .addFilter(new ResponseLoggingFilter(Matchers.not(Matchers.is(expectedStatusCode))))
                .addFilters(List.of(filters))
                .build();
        return authenticationSetter.applyAuthenticationSetting(requestSpecification.auth());
    }

}
