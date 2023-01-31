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

import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

import io.restassured.filter.Filter;
import io.restassured.filter.FilterContext;
import io.restassured.response.Response;
import io.restassured.specification.FilterableRequestSpecification;
import io.restassured.specification.FilterableResponseSpecification;

/**
 * This filter allows setting additional query parameters to the HTTP request.
 */
@Immutable
public final class AdditionalQueryParamsFilter implements Filter {

    private final Map<String, String> additionalQueryParams;

    private AdditionalQueryParamsFilter(final Map<String, String> additionalQueryParams) {
        this.additionalQueryParams = additionalQueryParams;
    }

    public static AdditionalQueryParamsFilter of(final CharSequence parameterName, final CharSequence parameterValue) {
        argumentNotEmpty(parameterName, "parameterName");
        checkNotNull(parameterValue, "parameterValue");
        return new AdditionalQueryParamsFilter(Map.of(parameterName.toString(), parameterValue.toString()));
    }

    public static AdditionalQueryParamsFilter of(final Map<String, String> additionalQueryParams) {
        return new AdditionalQueryParamsFilter(Map.copyOf(checkNotNull(additionalQueryParams,
                "additionalQueryParams")));
    }

    @Override
    public Response filter(final FilterableRequestSpecification requestSpec,
            final FilterableResponseSpecification responseSpec,
            final FilterContext ctx) {

        final var queryParams = new HashMap<>(requestSpec.getQueryParams());
        queryParams.putAll(additionalQueryParams);

        final var newRequestSpec = requestSpec.queryParams(queryParams);

        return ctx.next((FilterableRequestSpecification) newRequestSpec, responseSpec);
    }

}
