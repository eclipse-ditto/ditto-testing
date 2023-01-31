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

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.Filter;
import io.restassured.filter.FilterContext;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.Response;
import io.restassured.specification.FilterableRequestSpecification;
import io.restassured.specification.FilterableResponseSpecification;

/**
 * This filter allows to enable and configure logging for a HTTP request.
 */
@Immutable
public final class RequestLoggingFilter implements Filter {

    private final LogDetail logDetail;

    private RequestLoggingFilter(final LogDetail logDetail) {
        this.logDetail = logDetail;
    }

    /**
     * Returns a new instance of {@code RequestLoggingFilter}.
     *
     * @param logDetail enables logging of the request with this log detail.
     * @return the instance.
     * @throws NullPointerException if {@code logDetail} is {@code null}.
     */
    public static RequestLoggingFilter newInstance(final LogDetail logDetail) {
        return new RequestLoggingFilter(ConditionChecker.checkNotNull(logDetail, "logDetail"));
    }

    @Override
    public Response filter(final FilterableRequestSpecification requestSpec,
            final FilterableResponseSpecification responseSpec,
            final FilterContext ctx) {

        final var requestSpecWithAdjustedLogDetail = new RequestSpecBuilder()
                .addRequestSpecification(requestSpec)
                .log(logDetail)
                .build();

        return ctx.next((FilterableRequestSpecification) requestSpecWithAdjustedLogDetail, responseSpec);
    }

}
