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

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.util.List;
import java.util.function.LongConsumer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.hamcrest.Matcher;
import org.slf4j.Logger;

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import io.restassured.specification.ResponseSpecification;

/**
 * This matcher requests a PUT for a resource of Ditto and checks if it worked as expected.
 */
@NotThreadSafe
public final class PutMatcher extends HttpVerbMatcher<PutMatcher> {

    private static final String PUT_MESSAGE_TEMPLATE = "PUTing <{}> to <{}> with contentType {}: {}";
    private static final String SHORT_MESSAGE_TEMPLATE = "PUTing <{}> to <{}> with contentType {}";

    private final String contentType;
    @Nullable private final String payload;

    private PutMatcher(final String path, final String contentType, @Nullable final String payload) {
        super(path);
        this.contentType = contentType;
        this.payload = payload;
    }

    private PutMatcher(final String path, @Nullable final String jsonString) {
        this(path, ContentType.JSON.toString(), jsonString);
    }

    /**
     * Creates a new {@code PutMatcher} for the given {@code path}.
     *
     * @param path the path of the resource to put.
     * @throws NullPointerException if {@code path} is {@code null}.
     * @throws IllegalArgumentException if {@code path} is empty.
     */
    public static PutMatcher newInstance(final String path) {
        return new PutMatcher(path, null);
    }

    /**
     * Creates a new {@code PutMatcher} for the given {@code path} and {@code jsonString}.
     *
     * @param path the path of the resource to put.
     * @param jsonString the JSON to put.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public static PutMatcher newInstance(final String path, final String jsonString) {
        return new PutMatcher(path, checkNotNull(jsonString, "JSON string"));
    }

    /**
     * Creates a new {@code PutMatcher} for the given {@code path} and {@code payload}.
     *
     * @param path the path of the resource to post.
     * @param contentType the ContentType to use to post.
     * @param payload the payload post.
     * @throws NullPointerException if any argument is {@code null}.
     * @throws IllegalArgumentException if {@code path} is empty.
     */
    public static PutMatcher newInstance(final String path, final String contentType, @Nullable final String payload) {
        checkNotNull(path, "path");
        checkNotNull(contentType, "content-type");
        return new PutMatcher(path, contentType, payload);
    }

    @Override
    protected Response doFire(final String path, final ResponseSpecification responseSpecification,
            final List<Matcher<String>> bodyMatchers) {

        bodyMatchers.forEach(responseSpecification::body);

        final RequestSpecification requestSpec = responseSpecification.given().contentType(contentType);
        if (payload != null) {
            requestSpec.given().body(payload);
        }

        return responseSpecification.when().put(path);
    }

    @Override
    protected void doLog(final Logger logger, final String path, final String entityType) {
        if (logger.isDebugEnabled()) {
            logger.debug(PUT_MESSAGE_TEMPLATE, entityType, path, contentType, payload);
        } else {
            logger.info(SHORT_MESSAGE_TEMPLATE, entityType, path, contentType);
        }
    }

    @Override
    public PutMatcher registerRequestSizeConsumer(@Nullable final LongConsumer requestSizeConsumer) {
        if (null != requestSizeConsumer) {
            final var headersSize = getHeaderBytes();
            final var payloadSize = null != payload ? getBytes(payload) : 0;
            requestSizeConsumer.accept(headersSize + payloadSize);
        }
        return getThis();
    }

}
