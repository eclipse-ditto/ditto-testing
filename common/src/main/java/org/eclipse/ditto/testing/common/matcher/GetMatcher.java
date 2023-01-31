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

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;

/**
 * This matcher requests a GET for a resource of Ditto and checks if it worked as expected.
 */
@NotThreadSafe
public final class GetMatcher extends AbstractGetMatcher<GetMatcher> {

    private static final String GET_MESSAGE_TEMPLATE = "GETing <{}> from <{}>.";

    private GetMatcher(final String path) {
        super(path);
    }

    /**
     * Creates a new {@code GetMatcher} for the given {@code path}.
     *
     * @param path the path of the resource to get.
     * @throws NullPointerException if {@code path} is {@code null}.
     * @throws IllegalArgumentException if {@code path} is empty.
     */
    public static GetMatcher newInstance(final String path) {
        return new GetMatcher(path);
    }

    @Override
    protected void doLog(final Logger logger, final String path, @Nullable final String entityType) {
        logger.info(GET_MESSAGE_TEMPLATE, entityType, path);
    }

}
