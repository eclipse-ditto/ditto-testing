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
package org.eclipse.ditto.testing.system.search.sync.common.things;

import static org.eclipse.ditto.json.assertions.DittoJsonAssertions.assertThat;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.attribute;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.featureProperty;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.and;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.or;

import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchResponseExtractors;
import org.eclipse.ditto.testing.system.search.sync.common.SearchSyncTestConfig;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.Features;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SearchResult;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * When searching a Thing through the search api, the search service will check its own mongodb collection to find the
 * thingid and afterwards ask the things service for the thing itself. Since the index size violations only happen in
 * the search service, the user is not aware of the problem. Therefore possible index size violations are prevented by
 * cutting too long attributes and properties in the search service. To verify that the search service does not have
 * errors, it is possible to search for properties or attributes that would cause the index constraint to fail when
 * inserting the document to mongodb.
 */
public final class ThingIndexViolationsIT extends VersionedSearchIntegrationTest {

    private static final SearchSyncTestConfig CONF = SearchSyncTestConfig.getInstance();

    private static final int ALLOWED_VALUE_LENGTH = 500;
    private static final String ATTRIBUTE_KEY = "version";
    private static final String ATTRIBUTE_VALUE = createString(1020);
    private static final String FEATURE_ID = "text-to-speech";
    private static final String FEATURE_PROPERTY_KEY = "last-message";
    private static final String FEATURE_PROPERTY_VALUE = createString(1020);
    private static final Attributes EMPTY_ATTRIBUTES = Attributes.newBuilder().build();
    private static final Features EMPTY_FEATURES = Features.newBuilder().build();
    private static final Attributes ATTRIBUTES = Attributes.newBuilder()
            .set(ATTRIBUTE_KEY, ATTRIBUTE_VALUE).build();
    private static final Features FEATURES = Features.newBuilder().set(
            Feature.newBuilder().withId(FEATURE_ID).build().setProperty(FEATURE_PROPERTY_KEY, FEATURE_PROPERTY_VALUE)
    ).build();

    private static ThingId thingId1;
    private static ThingId thingId2;
    private static ThingId thingId3;
    private static ThingId thingId4;

    @BeforeClass
    public static void initThingIds() {
        thingId1 = ThingId.of(idGenerator().withPrefixedRandomName("indexviolationthing001"));
        thingId2 = ThingId.of(idGenerator().withPrefixedRandomName("indexviolationthing002"));
        thingId3 = ThingId.of(idGenerator().withPrefixedRandomName("indexviolationthing003"));
        thingId4 = ThingId.of(idGenerator().withPrefixedRandomName("indexviolationthing004"));
    }

    @After
    public void deleteThings() {
        Stream.of(thingId1, thingId2, thingId3, thingId4)
                .forEach(id -> {
                    deleteThing(apiVersion.toInt(), id).fire();
                    deletePolicy(id).fire();
                });
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> search(or(idFilter(thingId1),
                        idFilter(thingId2),
                        idFilter(thingId3),
                        idFilter(thingId4)), apiVersion).getItems().isEmpty());
    }

    @Test
    public void enforceAttributeLengthOnInsert() {
        final SearchResult response = createAndReadThing(thingId1, ATTRIBUTES, EMPTY_FEATURES);

        // verify
        assertThat(response.getItems()).isNotEmpty();

        // also verify it contains the complete attribute value
        final Thing resultAsThing = ThingsModelFactory.newThing(response.getItems().get(0).get().asObject());
        assertThat(resultAsThing.getAttributes().get().getValue(ATTRIBUTE_KEY).get().asString())
                .isEqualTo(ATTRIBUTE_VALUE);

        // search for attribute with long value should not return any thing
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> search(attSearch(thingId1), apiVersion).getItems()
                        .isEmpty());
        // search for attribute with restricted value should return the thing
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(attSearchPrefix(thingId1), apiVersion).getItems()
                        .isEmpty());

        // delete the thing afterwards
        deleteThing(apiVersion.toInt(), thingId1).fire();
        deletePolicy(thingId1).fire();

        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> search(idFilter(thingId1), apiVersion).getItems().isEmpty());
    }


    @Test
    public void enforceFeaturePropertyLengthOnInsert() {
        final SearchResult response = createAndReadThing(thingId2, EMPTY_ATTRIBUTES, FEATURES);

        // verify
        assertThat(response.getItems()).isNotEmpty();

        // also verify it contains the complete attribute value
        final Thing resultAsThing = ThingsModelFactory.newThing(response.getItems().get(0).get().asObject());
        assertThat(resultAsThing.getFeatures().get()
                .getFeature(FEATURE_ID).get()
                .getProperty(FEATURE_PROPERTY_KEY).get()
                .asString())
                .isEqualTo(FEATURE_PROPERTY_VALUE);

        // search for attribute with long value should not return any thing
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> search(ftSearch(thingId2), apiVersion).getItems().isEmpty());
        // search for attribute with restricted value should return the thing
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(ftSearchPrefix(thingId2), apiVersion).getItems().isEmpty());

        // delete the thing afterwards
        deleteThing(apiVersion.toInt(), thingId2).fire();
        deletePolicy(thingId2).fire();

        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> search(idFilter(thingId2), apiVersion).getItems().isEmpty());

    }

    @Test
    public void enforceAttributeLengthOnUpdate() {
        final SearchResult response = createAndReadThing(thingId3, EMPTY_ATTRIBUTES, EMPTY_FEATURES);

        // verify
        assertThat(response.getItems()).isNotEmpty();

        // update thing
        final Thing newThing = thing(thingId3, ATTRIBUTES, EMPTY_FEATURES);
        putThing(apiVersion.toInt(), newThing, apiVersion).fire();

        // search for attribute with restricted value should return the thing
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(attSearchPrefix(thingId3), apiVersion).getItems()
                        .isEmpty());

        // search for attribute with long value should not return any thing
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> search(attSearch(thingId3), apiVersion).getItems()
                        .isEmpty());

        // delete the thing afterwards
        deleteThing(apiVersion.toInt(), thingId3).fire();
        deletePolicy(thingId3).fire();

        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> search(idFilter(thingId3), apiVersion).getItems().isEmpty());

    }

    @Test
    public void enforceFeaturePropertyLengthOnUpdate() {
        final SearchResult response = createAndReadThing(thingId4, EMPTY_ATTRIBUTES, EMPTY_FEATURES);

        // verify
        assertThat(response.getItems()).isNotEmpty();

        // update thing
        final Thing newThing = thing(thingId4, EMPTY_ATTRIBUTES, FEATURES);
        putThing(apiVersion.toInt(), newThing, apiVersion).fire();

        // search for attribute with restricted value should return the thing
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(ftSearchPrefix(thingId4), apiVersion).getItems().isEmpty());

        // search for attribute with long value should not return any thing
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> search(ftSearch(thingId4), apiVersion).getItems().isEmpty());

        // delete the thing afterwards
        deleteThing(apiVersion.toInt(), thingId4).fire();
        deletePolicy(thingId4).fire();

        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> search(idFilter(thingId4), apiVersion).getItems().isEmpty());

    }

    private static SearchFilter ftSearch(final ThingId thingId) {
        return and(
                idFilter(thingId),
                featureProperty(FEATURE_ID, FEATURE_PROPERTY_KEY).eq(FEATURE_PROPERTY_VALUE)
        );
    }

    private static SearchFilter ftSearchPrefix(final ThingId thingId) {
        final String prefix = FEATURE_PROPERTY_VALUE.substring(0, ALLOWED_VALUE_LENGTH);
        return and(
                idFilter(thingId),
                featureProperty(FEATURE_ID, FEATURE_PROPERTY_KEY).like(prefix + "*")
        );
    }

    private static SearchFilter attSearch(final ThingId thingId) {
        return and(
                idFilter(thingId),
                attribute(ATTRIBUTE_KEY).eq(ATTRIBUTE_VALUE)
        );
    }

    private static SearchFilter attSearchPrefix(final ThingId thingId) {
        final String prefix = ATTRIBUTE_VALUE.substring(0, ALLOWED_VALUE_LENGTH);
        return and(
                idFilter(thingId),
                attribute(ATTRIBUTE_KEY).like(prefix + "*")
        );
    }

    private static Thing thing(final ThingId thingId,
            final Attributes attributes,
            final Features features) {
        return ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttributes(attributes)
                .setFeatures(features)
                .build();
    }

    private SearchResult createAndReadThing(final ThingId thingId,
            final Attributes attributes,
            final Features features) {return createAndReadThing(thingId, attributes, features, apiVersion);}

    private static SearchResult createAndReadThing(
            final ThingId thingId,
            final Attributes attributes,
            final Features features, final JsonSchemaVersion apiVersion) {
        // prepare
        final Thing thing = thing(thingId, attributes, features);

        putThing(apiVersion.toInt(), thing, apiVersion).fire();

        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(idFilter(thingId), apiVersion).getItems().isEmpty());

        return search(idFilter(thingId), apiVersion);
    }

    private static SearchResult search(final SearchFilter searchFilter,
            final JsonSchemaVersion apiVersion) {
        return SearchResponseExtractors.asSearchResult(
                searchThings(apiVersion).returnIdsOnly(false).filter(searchFilter).fire()
                        .asString());
    }

    private static String createString(final int length) {
        return Stream.iterate(0, i -> i + 1)
                .limit(length)
                .map(i -> "$")
                .reduce("", (a, b) -> a + b);
    }
}
