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

import static org.assertj.core.api.Assertions.assertThat;

import java.text.MessageFormat;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

import org.eclipse.ditto.base.service.UriEncoding;
import org.eclipse.ditto.connectivity.model.UserPasswordCredentials;

import org.apache.pekko.http.javadsl.model.HttpRequest;
import org.apache.pekko.http.javadsl.model.headers.HttpCredentials;

/**
 * Signing of HTTP requests to authenticate at Azure.
 */
@ThreadSafe
public final class AzSaslRequestSigning implements HmacSigning {

    private final String endpoint;
    private final String sharedKeyName;
    private final String sharedKey;

    private AzSaslRequestSigning(final String endpoint, final String sharedKeyName, final String sharedKey) {
        this.endpoint = endpoint;
        this.sharedKeyName = sharedKeyName;
        this.sharedKey = sharedKey;
    }

    public static AzSaslRequestSigning newInstance(final String hostname, final String sharedKeyName, final String sharedKey) {
        return new AzSaslRequestSigning(hostname, sharedKeyName, sharedKey);
    }

    HttpCredentials generateSignedAuthorizationHeader(final HttpRequest request) {
        final String parsedExpriy = extractExpiry(request);
        final String stringToSign = getStringToSign(parsedExpriy);
        final byte[] sharedKeyBytes = Base64.getDecoder().decode(sharedKey);
        final byte[] signature = hmacSha256(sharedKeyBytes, stringToSign);
        return toHttpCredentials(signature, parsedExpriy);
    }

    public UserPasswordCredentials generateSignedUserCredentials(final String sharedKeyName, final String endpoint,
            final UserPasswordCredentials actualCredentials) {
        final String username = MessageFormat.format("{0}@sas.root.{1}", sharedKeyName, endpoint);

        final String parsedExpriy = extractExpiry(actualCredentials);
        final String stringToSign = getStringToSign(parsedExpriy);
        final byte[] sharedKeyBytes = Base64.getDecoder().decode(sharedKey);
        final byte[] signature = hmacSha256(sharedKeyBytes, stringToSign);
        final String token = toToken(signature, parsedExpriy);
        final String sasToken = MessageFormat.format("SharedAccessSignature {0}", token);

        return UserPasswordCredentials.newInstance(username, sasToken);
    }

    private String extractExpiry(final HttpRequest request) {
        final String authorization =
                request.getHeader("authorization").map(org.apache.pekko.http.javadsl.model.HttpHeader::value).orElseThrow();
        return extractExpiry(authorization);
    }

    private String extractExpiry(final UserPasswordCredentials originalCredentials) {
        return extractExpiry(originalCredentials.getPassword());
    }

    private String extractExpiry(final String sasToken) {
        final Pattern authorizationPattern = Pattern.compile(
                "^SharedAccessSignature sr=(?<sr>[^&]++)&sig=(?<sig>[^&]+)&se=(?<se>\\d++)&skn=(?<skn>.++)$");
        final Matcher matcher = authorizationPattern.matcher(sasToken);

        assertThat(matcher.matches())
                .describedAs("Authorization header does not match az-sasl signature pattern: <%s>", sasToken)
                .isTrue();
        return matcher.group("se");
    }

    private String getStringToSign(final String expiry) {
        return MessageFormat.format("{0}\n{1}", UriEncoding.encodeAllButUnreserved(endpoint), expiry);
    }

    private HttpCredentials toHttpCredentials(final byte[] signature, final String expiry) {
        final String token = toToken(signature, expiry);
        return HttpCredentials.create("SharedAccessSignature", token);
    }

    private String toToken(final byte[] signature, final String expiry) {
        final String encodedSignature = UriEncoding.encodeAllButUnreserved(Base64.getEncoder().encodeToString(signature));
        return MessageFormat.format("sr={0}&sig={1}&se={2}&skn={3}",
                endpoint,
                encodedSignature,
                expiry,
                sharedKeyName
        );
    }

}
