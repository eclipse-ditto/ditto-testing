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


import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.definition;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSortOption;

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.testing.common.SearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.testing.common.matcher.search.SearchProperties;
import org.eclipse.ditto.testing.common.matcher.search.SortProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingDefinition;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SearchModelFactory;
import org.eclipse.ditto.thingsearch.model.SortOptionEntry;
import org.junit.Test;

/**
 *
 */
public class QueryThingsWithFilterByThingDefinitionIT extends SearchIntegrationTest {

    private static final String THING1_DEFINITION_VALUE = "example:test:1.0";

    private static final String THING2_DEFINITION_VALUE = "foo.doo:bar:0.11";

    private static final String THING4_DEFINITION_VALUE = "example:test:1.0";

    private static ThingId thing1Id;
    private static ThingId thing2Id;
    private static ThingId thing3Id;
    private static ThingId thing4Id;

    private static ThingId createThingId(final int i) {
        return ThingId.of(idGenerator()
                .withPrefixedRandomName(QueryThingsWithFilterByThingDefinitionIT.class.getSimpleName(),
                        String.valueOf(i)));
    }

    private static SearchMatcher search(final SearchFilter searchFilter) {
        final var testFilter = SearchModelFactory.and(searchFilter, SearchProperties.thingId()
                .in(thing1Id.toString(), thing2Id.toString(), thing3Id.toString(), thing4Id.toString()));
        return searchThings(JsonSchemaVersion.V_2).filter(testFilter)
                .option(newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC));
    }

    protected void createTestData() {
        thing1Id = persistThingAndWaitTillAvailable(createThing1(), JsonSchemaVersion.V_2,
                serviceEnv.getDefaultTestingContext());
        thing2Id = persistThingAndWaitTillAvailable(createThing2(), JsonSchemaVersion.V_2,
                serviceEnv.getDefaultTestingContext());
        thing3Id = persistThingAndWaitTillAvailable(createThing3(), JsonSchemaVersion.V_2,
                serviceEnv.getDefaultTestingContext());
        thing4Id = persistThingAndWaitTillAvailable(createThing4(), JsonSchemaVersion.V_2,
                serviceEnv.getDefaultTestingContext());
    }

    private Thing createThing1() {
        final ThingDefinition definition = ThingsModelFactory.newDefinition(THING1_DEFINITION_VALUE);
        return Thing.newBuilder()
                .setId(createThingId(1))
                .setDefinition(definition)
                .build();
    }

    private Thing createThing2() {
        final ThingDefinition definition = ThingsModelFactory.newDefinition(THING2_DEFINITION_VALUE);
        return Thing.newBuilder()
                .setId(createThingId(2))
                .setDefinition(definition)
                .build();
    }

    private Thing createThing3() {
        final ThingDefinition definition = ThingsModelFactory.nullDefinition();
        return Thing.newBuilder()
                .setId(createThingId(3))
                .setDefinition(definition)
                .build();
    }

    private Thing createThing4() {
        final ThingDefinition definition = ThingsModelFactory.newDefinition(THING4_DEFINITION_VALUE);
        return Thing.newBuilder()
                .setId(createThingId(4))
                .setDefinition(definition)
                .build();
    }

    @Test
    public void queryByEqString() {
        search(definition().eq(THING1_DEFINITION_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing4Id)))
                .fire();
    }
    
    @Test
    public void queryByEqNull() {
        search(definition().eq(null))
                .expectingBody(isEqualTo(toThingResult(thing3Id)))
                .fire();
    }
    
    @Test
    public void queryByNeString() {
        search(definition().ne(THING1_DEFINITION_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing2Id, thing3Id)))
                .fire();
    }
    
    @Test
    public void queryByNeNull() {
        search(definition().ne(null))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing4Id)))
                .fire();
    }

}
