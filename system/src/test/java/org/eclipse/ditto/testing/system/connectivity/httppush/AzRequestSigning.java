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

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Locale;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.pekko.http.javadsl.model.ContentType;
import org.apache.pekko.http.javadsl.model.HttpMethod;
import org.apache.pekko.http.javadsl.model.HttpRequest;
import org.apache.pekko.http.javadsl.model.headers.HttpCredentials;
import org.apache.pekko.util.ByteString;

/**
 * Signing of HTTP requests to authenticate at Azure.
 */
@ThreadSafe
public final class AzRequestSigning implements HmacSigning {

    private static final String X_MS_DATE_HEADER = "x-ms-date";
    private static final DateTimeFormatter X_MS_DATE_FORMAT =
            DateTimeFormatter.ofPattern("EEE, dd MMM uuuu HH:mm:ss zzz", Locale.ENGLISH).withZone(ZoneId.of("GMT"));

    final private String workspaceId;
    final private String sharedKey;

    private AzRequestSigning(final String workspaceId, final String sharedKey) {
        this.workspaceId = workspaceId;
        this.sharedKey = sharedKey;
    }

    public static AzRequestSigning newInstance(final String workspaceId, final String sharedKey) {
        return new AzRequestSigning(workspaceId, sharedKey);
    }

    HttpCredentials generateSignedAuthorizationHeader(final HttpRequest request) {
        final Instant parsedSingingTime = getInstant(request);
        final String stringToSign = getStringToSignAZ(request, parsedSingingTime);
        final ByteString sharedKeyBytes = ByteString.fromArray(Base64.getDecoder().decode(sharedKey));
        final byte[] signature = hmacSha256(sharedKeyBytes.toArray(), stringToSign);
        return toHttpCredentials(signature);
    }

    private Instant getInstant(final HttpRequest request) {
        final String singingTime =
                request.getHeader(X_MS_DATE_HEADER)
                        .map(org.apache.pekko.http.javadsl.model.HttpHeader::value)
                        .orElseThrow();
        return Instant.from(X_MS_DATE_FORMAT.parse(singingTime));
    }

    private static String getStringToSignAZ(final HttpRequest strictRequest, final Instant timestamp) {
        final HttpMethod verb = strictRequest.method();
        // a strict request entity always has a content length
        final long contentLength = strictRequest.entity().getContentLengthOption().orElseThrow();
        final ContentType contentType = strictRequest.entity().getContentType();
        final String xMsDate = X_MS_DATE_HEADER + ":" + X_MS_DATE_FORMAT.format(timestamp);
        final String resource = strictRequest.getUri().path();
        return String.join("\n", verb.name(), String.valueOf(contentLength), contentType.toString(), xMsDate, resource);
    }

    private HttpCredentials toHttpCredentials(final byte[] signature) {
        final String encodedSignature = Base64.getEncoder().encodeToString(signature);
        final String token = workspaceId + ":" + encodedSignature;
        return HttpCredentials.create("SharedKey", token);
    }

}
