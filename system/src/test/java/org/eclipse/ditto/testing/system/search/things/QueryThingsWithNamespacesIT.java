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
package org.eclipse.ditto.testing.system.search.things;

import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;

import java.util.Arrays;

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.thingsearch.model.SearchResult;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This Test tests the retrieval of things in a specific namespace via the Search API.
 */
public final class QueryThingsWithNamespacesIT extends VersionedSearchIntegrationTest {

    private static ThingId thingId1;
    private static ThingId thingId2;

    @Override
    protected void createTestData() {
        thingId1 = createThingInNamespace(serviceEnv.getDefaultNamespaceName(),
                serviceEnv.getDefaultTestingContext().getOAuthClient());
        thingId2 = createThingInNamespace(serviceEnv.getTesting2NamespaceName(),
                serviceEnv.getTestingContext2().getOAuthClient());
    }

    @Test
    public void queryThingInExplicitNamespace() {
        searchForMyThings(apiVersion)
                .namespaces(serviceEnv.getDefaultNamespaceName())
                .expectingBody(isEqualTo(toThingResult(thingId1)))
                .fire();
    }

    @Test
    @Category(Acceptance.class)
    public void queryThingInNonSolutionNamespace() {
        searchForMyThings(apiVersion)
                .namespaces(serviceEnv.getTesting2NamespaceName())
                .expectingBody(isEqualTo(toThingResult(thingId2)))
                .fire();
    }

    @Test
    public void queryThingsInZeroNamespaces() {
        searchForMyThings(apiVersion)
                .namespaces()
                .expectingBody(isEqualTo(toThingResult()))
                .fire();
    }

    @Test
    public void queryThingsInBothNamespaces() {
        // expected result is listed in ascending order of thingID
        final SearchResult expectedResult = toSortedThingResult(Arrays.asList(thingId2, thingId1));

        searchForMyThings(apiVersion)
                .namespaces(serviceEnv.getDefaultNamespaceName(), serviceEnv.getTesting2NamespaceName())
                .expectingBody(isEqualTo(expectedResult))
                .fire();
    }

    private SearchMatcher searchForMyThings(final JsonSchemaVersion apiVersion) {
        return searchThings(apiVersion).filter(idsFilter(Arrays.asList(thingId1, thingId2)));
    }

    private ThingId createThingInNamespace(final String namespace, final AuthClient authClient) {
        final ThingId thingId = ThingId.of(idGenerator(namespace).withRandomName());
        final ThingBuilder.FromScratch thingBuilder = Thing.newBuilder().setId(thingId);

        final Thing thing = thingBuilder.build();
        final Policy policy = Policy.newBuilder().forLabel("DEFAULT")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setSubject(serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();

        return persistThingAndWaitTillAvailable(thing, policy, authClient);
    }

}
