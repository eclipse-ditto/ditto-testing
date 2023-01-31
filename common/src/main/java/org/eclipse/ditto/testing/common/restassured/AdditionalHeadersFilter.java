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

import static org.eclipse.ditto.base.model.common.ConditionChecker.argumentNotEmpty;
import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.util.Map;

import javax.annotation.concurrent.Immutable;

import io.restassured.filter.Filter;
import io.restassured.filter.FilterContext;
import io.restassured.response.Response;
import io.restassured.specification.FilterableRequestSpecification;
import io.restassured.specification.FilterableResponseSpecification;

/**
 * This filter allows to set additional headers to the HTTP request.
 */
@Immutable
public final class AdditionalHeadersFilter implements Filter {

    private final Map<String, String> additionalHeaders;

    private AdditionalHeadersFilter(final Map<String, String> additionalHeaders) {
        this.additionalHeaders = additionalHeaders;
    }

    public static AdditionalHeadersFilter of(final CharSequence headerName, final CharSequence headerValue) {
        argumentNotEmpty(headerName, "headerName");
        checkNotNull(headerValue, "headerValue");
        return new AdditionalHeadersFilter(Map.of(headerName.toString(), headerValue.toString()));
    }

    public static AdditionalHeadersFilter of(final Map<String, String> additionalHeaders) {
        return new AdditionalHeadersFilter(Map.copyOf(checkNotNull(additionalHeaders, "additionalHeaders")));
    }

    @Override
    public Response filter(final FilterableRequestSpecification requestSpec,
            final FilterableResponseSpecification responseSpec,
            final FilterContext ctx) {

        final var newRequestSpec = requestSpec.headers(additionalHeaders);

        return ctx.next((FilterableRequestSpecification) newRequestSpec, responseSpec);
    }

}
