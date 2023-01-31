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
package org.eclipse.ditto.testing.common.matcher;

import java.util.List;

import javax.annotation.Nullable;

import org.eclipse.ditto.json.JsonFieldSelector;
import org.hamcrest.Matcher;

import io.restassured.response.Response;
import io.restassured.specification.ResponseSpecification;

/**
 * Abstract base class for GetMatchers.
 */
public abstract class AbstractGetMatcher<T extends AbstractGetMatcher> extends HttpVerbMatcher<T> {

    /**
     * Parameter fields.
     */
    public static final String PARAM_FIELDS = "fields";

    /**
     * Constructs a {@code AbstractGetMatcher} object.
     *
     * @param path the HTTP resource path.
     * @throws NullPointerException if {@code path} is {@code null}.
     * @throws IllegalArgumentException if {@code path} is empty.
     */
    protected AbstractGetMatcher(final String path) {
        super(path);
    }

    @Override
    protected Response doFire(final String path, final ResponseSpecification responseSpecification,
            final List<Matcher<String>> bodyMatchers) {

        bodyMatchers.forEach(responseSpecification::body);

        return responseSpecification.request().get(path);
    }

    /**
     * Sets the "fields" parameter to be used for the REST request.
     *
     * @param fields the field selector which will be converted to a parameter.
     * @return this instance to allow Method Chaining.
     */
    public T withFields(@Nullable final JsonFieldSelector fields) {
        if (fields == null) {
            parameters.remove(PARAM_FIELDS);
            return getThis();
        } else {
            return withParam(PARAM_FIELDS, fields.toString());
        }
    }

}
