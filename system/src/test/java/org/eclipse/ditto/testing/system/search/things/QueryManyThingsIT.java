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

import static org.eclipse.ditto.testing.common.assertions.IntegrationTestAssertions.assertThat;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.satisfiesSearchResult;
import static org.eclipse.ditto.testing.system.search.things.QueryThingsWithMultipleFieldsIT.searchBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonFieldDefinition;
import org.eclipse.ditto.testing.common.HttpParameter;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.Test;

/**
 * Tests querying of many things.
 */
public class QueryManyThingsIT extends AbstractQueryManyThingsIT {

    private List<ThingId> thingIds;
    private List<Thing> manyThings;

    @Override
    protected void createTestData() {
        manyThings = new ArrayList<>();
        thingIds = new ArrayList<>();

        final Function<Long, Thing> thingBuilder = (i) -> {
            final String thingNamePrefix = "many" + apiVersion;
            final String thingId = idGenerator().withPrefixedRandomName(thingNamePrefix);
            final Thing thing = createRandomThing(thingId);
            manyThings.add(thing);
            return thing;
        };

        final long thingsCount = 150; // don't set it too high (*not* MAX_PAGE_SIZE), otherwise URIs get too long
        persistThingsAndWaitTillAvailable(thingBuilder, thingsCount);

        thingIds = extractIds(manyThings);
    }

    @Test(timeout = 120_000) // timeout is handled by the test itself
    public void queryManyThings() {

        // precondition: correct amount of things is returned from things store
        final String responseBody = getThings(apiVersion.toInt())
                .withParam(HttpParameter.IDS, String.join(",", thingIds))
                .withParam(HttpParameter.FIELDS, "thingId,attributes,features")
                .expectingHttpStatus(HttpStatus.OK)
                .fire()
                .getBody()
                .asString();
        final JsonArray thingsArrayJson = JsonFactory.newArray(responseBody);
        final List<Thing> fetchedThings = toThings(thingsArrayJson);
        final List<ThingId> fetchedFromStore = extractIds(fetchedThings);
        assertThat(fetchedFromStore).isEqualTo(thingIds);
        assertThat(fetchedThings).isEqualTo(manyThings);

        // test
        // for easier error analysis (we have many things..), just return the ID
        final List<JsonFieldDefinition> thingIdOnly = Collections.singletonList(Thing.JsonFields.ID);
        searchBuilder(idsFilter(thingIds), true, apiVersion)
                .expectingBody(satisfiesSearchResult(
                        result -> {
                            final String actual = result.toJson().toString();
                            final String expected = thingsToSearchResult(manyThings, thingIdOnly).toJson().toString();
                            assertThat(actual).isEqualTo(expected);
                        }));
    }
}
