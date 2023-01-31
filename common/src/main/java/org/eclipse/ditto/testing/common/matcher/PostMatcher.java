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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.testing.common.HttpHeader;
import org.hamcrest.Matcher;
import org.slf4j.Logger;

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import io.restassured.specification.ResponseSpecification;

/**
 * This matcher requests a POST for a resource of Ditto and checks if it worked as expected.
 */
@NotThreadSafe
public final class PostMatcher extends HttpVerbMatcher<PostMatcher> {

    private static final String POST_MESSAGE_TEMPLATE = "POSTing <{}> to <{}> with content-type {}: {}";
    private static final String SHORT_MESSAGE_TEMPLATE = "POSTing <{}> to <{}> with content-type {}";

    private final String contentType;
    @Nullable private final byte[] payload;

    private PostMatcher(final String path, final String contentType, @Nullable final byte[] payload) {
        super(path);
        this.contentType = contentType;
        this.payload = payload;
    }

    private PostMatcher(final String path, @Nullable final String jsonString) {
        this(path, ContentType.JSON.toString(), jsonString != null ? jsonString.getBytes() : null);
    }

    /**
     * Creates a new {@code PostMatcher} for the given {@code path}.
     *
     * @param path the path of the resource to post.
     * @throws NullPointerException if any argument is {@code null}.
     * @throws IllegalArgumentException if {@code path} is empty.
     */
    public static PostMatcher newInstance(final String path) {
        return new PostMatcher(path, null);
    }

    /**
     * Creates a new {@code PostMatcher} for the given {@code path} and {@code payload}.
     *
     * @param path the path of the resource to post.
     * @param jsonString the JSON to post.
     * @throws NullPointerException if any argument is {@code null}.
     * @throws IllegalArgumentException if {@code path} is empty.
     */
    public static PostMatcher newInstance(final String path, @Nullable final String jsonString) {
        return new PostMatcher(path, jsonString);
    }

    /**
     * Creates a new {@code PostMatcher} for the given {@code path} and {@code payload}.
     *
     * @param path the path of the resource to post.
     * @param contentType the ContentType to use to post.
     * @param payload the payload post.
     * @throws NullPointerException if any argument is {@code null}.
     * @throws IllegalArgumentException if {@code path} is empty.
     */
    public static PostMatcher newInstance(final String path, final String contentType, final byte[] payload) {
        checkNotNull(contentType, "content-type");

        return new PostMatcher(path, contentType, payload);
    }

    @Override
    protected Response doFire(final String path, final ResponseSpecification responseSpecification,
            final List<Matcher<String>> bodyMatchers) {

        bodyMatchers.forEach(responseSpecification::body);

        final RequestSpecification requestSpec = responseSpecification.given()
                .contentType(contentType)
                .header(HttpHeader.CONTENT_TYPE.getName(), contentType);

        if (payload != null) {
            requestSpec.given().body(payload);
        }

        return requestSpec.when().post(path);
    }

    @Override
    protected void doLog(final Logger logger, final String path, @Nullable final String entityType) {
        if (!logger.isDebugEnabled()) {
            logger.info(SHORT_MESSAGE_TEMPLATE,
                    entityType != null ? entityType : (payload != null ? new String(payload) : "?"),
                    path, contentType);
            return;
        }
        @Nullable final String payloadToLog;
        if (payload == null) {
            payloadToLog = null;
        } else if (isTextContentType(contentType)) {
            payloadToLog = new String(payload);
        } else {
            final int maxPrefixSize = 16;
            final int prefixSize = Math.min(maxPrefixSize, payload.length);

            payloadToLog = Stream.concat(
                    IntStream.range(0, prefixSize).mapToObj(i -> Byte.toString(payload[i])),
                    Stream.of("...")
            ).collect(Collectors.joining(",", "[", "]"));
        }
        logger.debug(POST_MESSAGE_TEMPLATE, entityType, path, contentType, payloadToLog);
    }

    private static boolean isTextContentType(final String contentType) {
        if (contentType == null) {
            // assume content is text by default
            return true;
        } else {
            final String lowerCaseContentType = contentType.toLowerCase();
            return "text".equals(lowerCaseContentType.substring(0, 4)) ||
                    lowerCaseContentType.matches("application/(json|xml)");
        }
    }

}
