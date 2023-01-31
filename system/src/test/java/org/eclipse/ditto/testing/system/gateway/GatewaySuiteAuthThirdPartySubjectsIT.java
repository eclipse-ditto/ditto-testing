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
package org.eclipse.ditto.testing.system.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.base.model.common.HttpStatus.OK;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.ditto.testing.common.CommonTestConfig;
import org.eclipse.ditto.testing.common.HttpResource;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.ThingsSubjectIssuer;
import org.eclipse.ditto.testing.common.client.http.AsyncHttpClientFactory;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.junit.Test;

public class GatewaySuiteAuthThirdPartySubjectsIT extends IntegrationTest {

    /**
     * Verifies that the subject generation for additional third party scopes of a suite auth token is correct.
     * Creates a custom suite auth client, to fetch tokens containing some additional scopes from the suite-auth mock.
     * Request the 'whoAmI' endpoint to validate that the subjects are derived as expected.
     */
    @Test
    public void createSubjectsWithAdditionalInsightsThirdPartyScopes() {

        final TestingContext defaultTestingContext = serviceEnv.getDefaultTestingContext();
        final AuthClient defaultSuiteAuthClient = defaultTestingContext.getOAuthClient();

        // Additional Third Party Scopes for insights service
        final String thirdPartyScopeRole = "ext-iam.insights.testProject_testRole";
        final String thirdPartyScopeAdmin = "ext-iam.insights.admin";
        final String thirdPartyScopeDataLogging = "ext-iam.insights.master-data-logging";

        final List<String> thirdPartyScopes = List.of(thirdPartyScopeRole,
                thirdPartyScopeAdmin,
                thirdPartyScopeDataLogging);
        // ditto:/<ext-iam-prefixed-scope>
        final List<String> expectedThirdPartySubjects = thirdPartyScopes.stream()
                .map(scope -> ThingsSubjectIssuer.DITTO + ":/" + scope)
                .collect(Collectors.toList());

        final AuthClient suiteAuthClientWithThirdPartyScopes = AuthClient.newInstance(
                defaultSuiteAuthClient.getTokenEndpoint(),
                ThingsSubjectIssuer.DITTO,
                defaultSuiteAuthClient.getClientId(),
                defaultSuiteAuthClient.getClientSecret(),
                Stream.concat(defaultSuiteAuthClient.getScope().stream(), thirdPartyScopes.stream())
                        .collect(Collectors.joining(" ")),
                AsyncHttpClientFactory.newInstance(CommonTestConfig.getInstance()));

        final List<String> actualSubjects = get(thingsServiceUrl(TestConstants.API_V_2,
                HttpResource.WHOAMI.getPath()))
                .withLogging(LOGGER, "Whoami")
                .withJWT(suiteAuthClientWithThirdPartyScopes.getAccessToken())
                .expectingHttpStatus(OK)
                .fire()
                .body()
                .path("subjects");

        assertThat(actualSubjects).containsAll(expectedThirdPartySubjects);
    }
}
