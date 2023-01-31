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

import static java.util.Objects.requireNonNull;

import javax.annotation.Nullable;

import io.restassured.http.ContentType;

/**
 * Factory for generating Rest Matchers.
 */
public final class RestMatcherFactory {

    private final RestMatcherConfigurer configurer;

    public RestMatcherFactory(final RestMatcherConfigurer configurer) {
        this.configurer = requireNonNull(configurer);
    }

    public GetMatcher get(final String path) {
        return configure(GetMatcher.newInstance(path));
    }

    public PostMatcher post(final String path) {
        return configure(PostMatcher.newInstance(path));
    }

    public PostMatcher post(final String path, @Nullable final String jsonString) {
        return configure(PostMatcher.newInstance(path, jsonString));
    }

    public PostMatcher post(final String path, final String contentType, @Nullable final byte[] payload) {
        return configure(PostMatcher.newInstance(path, contentType, payload));
    }

    public PostMatcher post(final String path, final ContentType contentType, final byte[] payload) {
        return post(path, contentType.toString(), payload);
    }

    public PutMatcher put(final String path) {
        return configure(PutMatcher.newInstance(path));
    }

    public PutMatcher put(final String path, final String jsonString) {
        return configure(PutMatcher.newInstance(path, jsonString));
    }

    public PutMatcher put(final String path, final ContentType contentType, @Nullable final String payload) {
        return configure(PutMatcher.newInstance(path, contentType.toString(), payload));
    }

    public PutMatcher put(final String path, final CharSequence contentType, @Nullable final String payload) {
        return configure(PutMatcher.newInstance(path, contentType.toString(), payload));
    }

    public PatchMatcher patch(final String path, final String jsonString) {
        return configure(PatchMatcher.newInstance(path, jsonString));
    }

    public PatchMatcher patch(final String path, final CharSequence contentType, @Nullable final String payload) {
        return configure(PatchMatcher.newInstance(path, contentType.toString(), payload));
    }

    public DeleteMatcher delete(final String path) {
        return configure(DeleteMatcher.newInstance(path));
    }

    private <T extends HttpVerbMatcher> T configure(final T matcher) {
        configurer.configure(matcher);

        return matcher;
    }

}
