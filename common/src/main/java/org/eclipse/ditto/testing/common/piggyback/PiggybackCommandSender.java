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
package org.eclipse.ditto.testing.common.piggyback;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLogger;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLoggerFactory;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;

import io.restassured.RestAssured;
import io.restassured.filter.Filter;
import io.restassured.filter.log.ErrorLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.http.Header;
import io.restassured.response.Response;

/**
 * Sends {@link PiggybackRequest}s to a particular piggyback command endpoint via HTTP POST.
 */
@Immutable
public final class PiggybackCommandSender {

    private static final String PIGGYBACK_PATH = "/devops/piggyback";

    private static final DittoLogger LOGGER = DittoLoggerFactory.getLogger(PiggybackCommandSender.class);

    private final String piggybackCommandUri;
    private final String devOpsCommandTimeout;

    private PiggybackCommandSender(final URI piggybackCommandUri, final Duration devOpsCommandTimeout) {
        this.piggybackCommandUri = piggybackCommandUri.toString();
        this.devOpsCommandTimeout = devOpsCommandTimeout.toMillis() + "ms";
    }

    /**
     * Returns a new instance of {@code PiggybackCommandSender}.
     *
     * @param devOpsUri the endpoint URI where to send piggyback commands to.
     * @param devOpsCommandTimeout the timeout for piggyback commands.
     * @return the instance.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public static PiggybackCommandSender newInstance(final URI devOpsUri,
            final Duration devOpsCommandTimeout) {

        checkNotNull(devOpsUri, "devOpsUri");
        final var piggybackCommandUri = devOpsUri.resolve(PIGGYBACK_PATH);
        try {
            return new PiggybackCommandSender(piggybackCommandUri,
                    checkNotNull(devOpsCommandTimeout, "devOpsCommandTimeout"));
        } finally {
            LOGGER.info("Initialised for endpoint <{}>.", piggybackCommandUri);
        }
    }

    /**
     * Posts the specified {@code PiggybackRequest} argument.
     *
     * @param piggybackRequest provides everything for executing the piggyback command.
     * @param filter optional filter(s) for the REST assured request.
     * @return the response.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public Response postRequest(final PiggybackRequest piggybackRequest, final Filter... filter) {
        checkNotNull(piggybackRequest, "piggybackRequest");
        checkNotNull(filter, "filter");

        final var executePiggybackCommand = piggybackRequest.getPiggybackCommand();

        final var dittoHeaders = executePiggybackCommand.getDittoHeaders();

        LOGGER.withCorrelationId(dittoHeaders).info("Posting <{}>.", executePiggybackCommand);

        var requestSpecification = RestAssured.with()
                .log().ifValidationFails()
                .baseUri(piggybackCommandUri)
                .contentType(ContentType.JSON)
                .header("timeout", devOpsCommandTimeout);
        requestSpecification = getCorrelationIdHeader(dittoHeaders)
                .map(requestSpecification::header)
                .orElse(requestSpecification);
        requestSpecification = requestSpecification.body(executePiggybackCommand.toJsonString());
        requestSpecification = requestSpecification.filters(new ErrorLoggingFilter(), filter);
        return requestSpecification.post("/{serviceName}", piggybackRequest.getServiceName());
    }

    private static Optional<Header> getCorrelationIdHeader(final DittoHeaders dittoHeaders) {
        return dittoHeaders.getCorrelationId().map(CorrelationId::of).map(CorrelationId::toHeader);
    }

}
