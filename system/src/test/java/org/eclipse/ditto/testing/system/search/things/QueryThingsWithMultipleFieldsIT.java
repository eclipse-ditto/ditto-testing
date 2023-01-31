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
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEmpty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.satisfiesSearchResult;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSizeOption;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSortOption;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.ditto.base.model.acks.DittoAcknowledgementLabel;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonFieldDefinition;
import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.testing.common.matcher.search.SortProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SizeOption;
import org.eclipse.ditto.thingsearch.model.SortOption;
import org.eclipse.ditto.thingsearch.model.SortOptionEntry;
import org.junit.Test;

/**
 * This Test tests some scenarios with querying multiple or all fields of things. (The other tests mostly just check the
 * thing id.)
 */
public final class QueryThingsWithMultipleFieldsIT extends AbstractQueryManyThingsIT {

    private static String thingIdFromStore1;
    private static List<String> thingIdsInStore;
    private static List<Thing> thingsInStore;

    private static final JsonFieldSelector FIELD_SELECTOR_FOR_REGULAR_FIELDS =
            JsonFactory.newFieldSelector(Thing.JsonFields.ID, Thing.JsonFields.ATTRIBUTES, Thing.JsonFields.FEATURES);

    @Override
    protected void createTestData() {
        thingIdFromStore1 = idGenerator().withPrefixedRandomName("thingIdFromStore1");
        final String thingIdFromStore2 = idGenerator().withPrefixedRandomName("thingIdFromStore2");
        thingIdsInStore =
                Collections.unmodifiableList(Arrays.asList(thingIdFromStore1, thingIdFromStore2));

        thingsInStore = new ArrayList<>(thingIdsInStore.size());
        thingIdsInStore.forEach((id) -> {
            final Thing thing = createRandomThing(id);
            persistThingAndWaitTillAvailable(thing);
            thingsInStore.add(thing);
        });
    }

    static SearchMatcher searchBuilder(final SearchFilter searchFilter, final boolean idsOnly,
            final JsonSchemaVersion apiVersion) {

        final SortOption sortOption = newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC);
        final SizeOption sizeOption = newSizeOption(MAX_PAGE_SIZE);

        return searchThings(apiVersion)
                .returnIdsOnly(idsOnly)
                .filter(searchFilter)
                .options(sortOption, sizeOption);
    }
    
    @Test
    public void queryOneThingWhichExistsInThingsStore() {
        searchBuilder(idFilter(thingIdFromStore1), false, apiVersion)
                .withFields(FIELD_SELECTOR_FOR_REGULAR_FIELDS)
                .expectingBody(satisfiesSearchResult(result -> {
                    final String actual = result.getItems().get(0).get().asObject()
                            .toString();
                    final JsonObject expected = thingsInStore.get(0).toJson();
                    assertThat(JsonFactory.newObject(actual)).isEqualToIgnoringFieldDefinitions(expected);
                }))
                .fire();
    }
    
    @Test
    public void queryMultipleThingsWhichExistInThingsStore() {
        searchBuilder(idsFilter(thingIdsInStore), false, apiVersion)
                .withFields(FIELD_SELECTOR_FOR_REGULAR_FIELDS)
                .expectingBody(satisfiesSearchResult(result -> {
                    final List<Thing> responseThingList = result.getItems()
                            .stream()
                            .filter(JsonValue::isObject)
                            .map(JsonValue::asObject)
                            .map(ThingsModelFactory::newThing)
                            .collect(Collectors.toList());
                    assertThat(responseThingList).isEqualTo(thingsInStore);
                }))
                .fire();
    }
    
    @Test
    public void queryWithIdFieldOnly() {
        // prepare
        final List<JsonFieldDefinition> fieldsList = Collections.singletonList(Thing.JsonFields.ID);

        // test
        searchBuilder(idFilter(thingIdFromStore1), true, apiVersion)
                .expectingBody(satisfiesSearchResult(
                        result -> {
                            final String actual = result.toJson().toString();
                            final JsonObject expected = thingToSearchResult(thingsInStore.get(0), fieldsList).toJson();
                            assertThat(JsonFactory.newObject(actual)).isEqualToIgnoringFieldDefinitions(expected);
                        }))
                .fire();
    }

    /**
     * Regression test.
     */
    @Test
    public void queryOnDeletedThing() {
        // prepare
        final String thingIdForDelete = idGenerator()
                .withPrefixedRandomName("thingIdDeleted", String.valueOf(apiVersion));
        final Thing thingForDelete = createRandomThing(thingIdForDelete);
        persistThingAndWaitTillAvailable(thingForDelete);

        // test
        deleteThing(apiVersion.toInt(), thingIdForDelete)
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                        DittoAcknowledgementLabel.SEARCH_PERSISTED.toString())
                .withHeader(DittoHeaderDefinition.TIMEOUT.getKey(), "30s")
                .expectingStatusCodeSuccessful()
                .fire();

        // must delete the auto-created policy in order not to make thing ID unusable
        if (apiVersion == JsonSchemaVersion.V_2) {
            deletePolicy(thingIdForDelete).expectingStatusCodeSuccessful().fire();
        }

        // verify the thing must not be found any longer in the thing store
        getThing(apiVersion.toInt(), thingIdForDelete)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        // the thing must not be found any longer via search
        searchBuilder(idFilter(thingIdForDelete), false, apiVersion)
                .expectingBody(isEmpty())
                .fire();
    }

    /**
     * Regression: Query for things which don't exist in both persistences.
     */
    @Test
    public void queryNonExistingThings() {
        searchBuilder(idsFilter(Arrays.asList("doesNotExist1", "doesNotExist2")), false, apiVersion)
                .expectingBody(isEmpty())
                .fire();
    }

}
