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
import javax.annotation.concurrent.NotThreadSafe;

import org.hamcrest.Matcher;
import org.slf4j.Logger;

import io.restassured.response.Response;
import io.restassured.specification.ResponseSpecification;

/**
 * This matcher requests a DELETE for a resource of Ditto and checks if it worked as expected.
 */
@NotThreadSafe
public final class DeleteMatcher extends HttpVerbMatcher<DeleteMatcher> {

    private static final String DELETE_MESSAGE_TEMPLATE = "DELETing <{}> from <{}>.";

    private DeleteMatcher(final String path) {
        super(path);
    }

    /**
     * Creates a new {@code DeleteMatcher} for the given {@code path}.
     *
     * @param path the path of the resource to delete.
     * @throws NullPointerException if {@code path} is {@code null}.
     * @throws IllegalArgumentException if {@code path} is empty.
     */
    public static DeleteMatcher newInstance(final String path) {
        return new DeleteMatcher(path);
    }

    @Override
    protected Response doFire(final String path, final ResponseSpecification responseSpecification,
            final List<Matcher<String>> bodyMatchers) {

        return responseSpecification.request().delete(path);
    }

    @Override
    protected void doLog(final Logger logger, final String path, @Nullable final String entityType) {
        logger.info(DELETE_MESSAGE_TEMPLATE, entityType, path);
    }

}
