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

package org.eclipse.ditto.testing.system.connectivity.httppush;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.eclipse.ditto.connectivity.model.MessageSendingFailedException;

interface HmacSigning {

    /**
     * Compute HMAC using SHA256 hash function.
     *
     * @param key the key.
     * @param input the message.
     * @return the HMAC.
     */
    default byte[] hmacSha256(final byte[] key, final String input) {
        try {
            final String hmacSha256 = "HmacSHA256";
            final Mac mac = Mac.getInstance(hmacSha256);
            mac.init(new SecretKeySpec(key, hmacSha256));
            return mac.doFinal(input.getBytes(StandardCharsets.UTF_8));
        } catch (final NoSuchAlgorithmException | InvalidKeyException e) {
            throw MessageSendingFailedException.newBuilder()
                    .message("Request signing failed.")
                    .cause(e)
                    .build();
        }
    }
}
