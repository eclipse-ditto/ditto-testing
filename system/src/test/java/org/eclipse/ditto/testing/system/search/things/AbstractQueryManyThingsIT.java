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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonFieldDefinition;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonObjectBuilder;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchResult;
import org.eclipse.ditto.thingsearch.model.SearchResultBuilder;

/**
 * Common methods of {@link QueryManyThingsIT} and {@link QueryThingsWithMultipleFieldsIT}.
 */
abstract class AbstractQueryManyThingsIT extends VersionedSearchIntegrationTest {

    static SearchResult thingToSearchResult(final Thing thing, final Collection<JsonFieldDefinition> fields) {
        return SearchModelFactory.newSearchResultBuilder()
                .add(getJsonObjectThing(thing, fields))
                .nextPageOffset(null)
                .build();
    }

    private static JsonObject getJsonObjectThing(final Thing thing, final Collection<JsonFieldDefinition> fields) {
        JsonObject thingObject = thing.toJson(JsonSchemaVersion.V_2);

        if (!fields.isEmpty()) {
            final JsonObjectBuilder objectBuilder = JsonFactory.newObjectBuilder();
            for (final JsonFieldDefinition fieldName : fields) {
                thingObject.getValue(fieldName).ifPresent(jsonValue -> objectBuilder.set(fieldName, jsonValue));
            }
            thingObject = objectBuilder.build();
        }
        return thingObject;
    }

    static List<Thing> toThings(final JsonArray thingsArrayJson) {
        final List<Thing> things = new ArrayList<>();
        thingsArrayJson.forEach(v -> things.add(ThingsModelFactory.newThingBuilder((JsonObject) v).build()));
        return things;
    }

    static List<ThingId> extractIds(final Collection<Thing> manyThings) {
        return manyThings.stream()
                .map(Thing::getEntityId)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    static SearchResult thingsToSearchResult(final Iterable<Thing> things,
            final Collection<JsonFieldDefinition> fields) {
        final SearchResultBuilder builder = SearchModelFactory.newSearchResultBuilder();
        things.forEach((thing) -> builder.add(getJsonObjectThing(thing, fields)));
        return builder.build();
    }

}
