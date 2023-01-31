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
package org.eclipse.ditto.testing.common.client.oauth;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import org.asynchttpclient.AsyncHttpClient;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.policies.model.SubjectType;

/**
 * Utility class for retrieving Auth Tokens.
 */
public final class AuthClient extends CommonOAuth2Client {

    private final Subject subject;
    private final Subject defaultSubject;

    private AuthClient(final String tokenEndpoint,
            final SubjectIssuer subjectIssuer,
            final String clientId,
            final String clientSecret,
            final String scope,
            final AsyncHttpClient httpClient) {

        super(tokenEndpoint, clientId, clientSecret, scope, httpClient);
        this.subject = Subject.newInstance(subjectIssuer, clientId, SubjectType.newInstance("ditto-auth"));

        this.defaultSubject =
                Subject.newInstance(subjectIssuer, clientId, SubjectType.newInstance("ditto-auth"));
    }

    public static AuthClient newInstance(final String tokenEndpoint,
            final SubjectIssuer subjectIssuer,
            final String clientId,
            final String clientSecret,
            final String scope,
            final AsyncHttpClient httpClient) {

        checkNotNull(subjectIssuer, "subjectIssuer");
        checkNotNull(scope, "scope");

        return new AuthClient(tokenEndpoint, subjectIssuer, clientId, clientSecret, scope, httpClient);
    }

    /**
     * Returns the Suite Auth client id subject, e.g. ditto:0a61b502-4ce2-4d11-a962-80f6d3521875
     *
     * @return the subject.
     */
    public Subject getSubject() {
        return subject;
    }

    /**
     * Returns the Suite Auth default subject.
     *
     * @return the subject.
     */
    public Subject getDefaultSubject() {
        return defaultSubject;
    }

}
