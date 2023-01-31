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


import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.testing.common.Timeout;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchResponseExtractors;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SearchResult;
import org.junit.Test;

/**
 * Sanity test for test data creation and cleanup.
 */
@Timeout(millis = Long.MAX_VALUE)
public final class CreateThingWithFixedIdsIT extends VersionedSearchIntegrationTest {

    @Test
    public void createAndGetThingViaThingsApi() {
        final ThingId thingId = persistThingAndWaitTillAvailable(createThing("getThing"));

        getThing(apiVersion.toInt(), thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void queryFullThings() {
        final ThingId thingId1 = persistThingAndWaitTillAvailable(createThing("retrieveFull1"));
        final ThingId thingId2 = persistThingAndWaitTillAvailable(createThing("retrieveFull2"));

        final String jsonStr = searchThings(apiVersion)
                .filter(idsFilter(Arrays.asList(thingId1, thingId2)))
                .returnIdsOnly(false)
                .fire()
                .asString();
        final SearchResult result = SearchResponseExtractors.asSearchResult(jsonStr);

        Assertions.assertThat(result.getItems().getSize()).isEqualTo(2);
    }

    @Test
    public void deletedThingShouldNotAppearInSearchResult() {
        final ThingId thingId =
                persistThingAndWaitTillAvailable(createThing("deletedThingShouldNotAppearInSearchResult"));

        final SearchResult beforeDelete = searchForThingId(thingId);
        Assertions.assertThat(beforeDelete.getItems()).isNotEmpty();

        deleteThing(2, thingId).fire();
        deletePolicy(thingId).fire();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
            final SearchResult afterDelete = searchForThingId(thingId);
            return afterDelete.isEmpty();
        });
    }

    private static Thing createThing(final String prefix) {
        final String thingId = idGenerator()
                .withPrefixedRandomName(CreateThingWithFixedIdsIT.class.getSimpleName(), prefix);

        return createRandomThing(thingId);
    }

    private SearchResult searchForThingId(final ThingId thingId) {
        final SearchFilter searchFilter = idFilter(thingId);
        final String jsonStr = searchThings()
                .filter(searchFilter)
                .fire()
                .asString();
        return SearchResponseExtractors.asSearchResult(jsonStr);
    }

}
