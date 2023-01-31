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
package org.eclipse.ditto.testing.common.client.ditto_protocol;

import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.internal.utils.protocol.config.DefaultProtocolConfig;
import org.eclipse.ditto.internal.utils.protocol.config.ProtocolConfig;
import org.eclipse.ditto.json.JsonField;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.testing.common.config.TestConfig;

/**
 * Checks whether the keys of blocklisted headers from {@link ProtocolConfig#getBlockedHeaderKeys()} are contained in
 * a {@link JsonObject}.
 * If so, an {@code AssertionError} is thrown.
 */
@Immutable
public final class HeaderBlocklistChecker {

    private static final Set<String> BLOCKED_HEADER_KEYS;

    static {
        final var testConfig = TestConfig.getInstance();
        final ProtocolConfig protocolConfig = DefaultProtocolConfig.of(testConfig.getConfigOrThrow("ditto"));
        BLOCKED_HEADER_KEYS = Set.copyOf(protocolConfig.getBlockedHeaderKeys());
    }

    private HeaderBlocklistChecker() {
        throw new AssertionError();
    }

    /**
     * Check headers do not include those in the blocklist.
     *
     * @param externalHeaders headers received in a Ditto protocol message.
     */
    public static void assertHeadersDoNotIncludeBlocklisted(final JsonObject externalHeaders) {
        final var foundBlocklistedHeaderKeys = externalHeaders.stream()
                .map(JsonField::getKeyName)
                .filter(BLOCKED_HEADER_KEYS::contains)
                .collect(Collectors.toList());
        if (!foundBlocklistedHeaderKeys.isEmpty()) {
            throw new AssertionError(String.format("Unexpected headers <%s> found in <%s>",
                    foundBlocklistedHeaderKeys,
                    externalHeaders));
        }
    }

}
