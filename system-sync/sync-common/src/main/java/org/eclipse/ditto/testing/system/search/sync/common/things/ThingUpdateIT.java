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


import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.attribute;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.featureProperty;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.and;

import java.util.Arrays;
import java.util.Collections;

import javax.annotation.Nullable;

import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonObjectBuilder;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchResponseExtractors;
import org.eclipse.ditto.testing.system.search.sync.common.SearchSyncTestConfig;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SearchResult;
import org.junit.Test;

/**
 * Test for the synchronization mechanism for things.
 */
public final class ThingUpdateIT extends VersionedSearchIntegrationTest {

    private static final SearchSyncTestConfig CONF = SearchSyncTestConfig.getInstance();

    private static final String KNOWN_ATTR_KEY = "attrKey";
    private static final String KNOWN_ATTR_VAL = "attrVal";
    private static final String NEW_ATTR_VAL = "attrNewVal";
    private static final String FEATURE_PROPERTY_KEY = "featurePropertyKey";
    private static final String FEATURE_PROPERTY_VAL = "featurePropertyVal";

    private static FeatureProperties newFeatureProperties(final String key, final String value) {
        final JsonObjectBuilder featurePropertiesBuilder = JsonFactory.newObjectBuilder();

        featurePropertiesBuilder.set(JsonFactory.newKey(key), value);

        return ThingsModelFactory.newFeatureProperties(featurePropertiesBuilder.build());
    }

    private static Feature createFeature(final String featureId) {
        final JsonObjectBuilder featurePropertiesBuilder = JsonFactory.newObjectBuilder();

        featurePropertiesBuilder.set(JsonFactory.newKey(FEATURE_PROPERTY_KEY), FEATURE_PROPERTY_VAL);
        final FeatureProperties properties = ThingsModelFactory.newFeatureProperties(featurePropertiesBuilder.build());

        return ThingsModelFactory.newFeature(featureId, properties);
    }

    @Test(timeout = Long.MAX_VALUE) // effectively disable global timeout, because timeout is handled by the test itself
    public void create() {
        final ThingId thingId = thingId("create");
        createAndPutThing(thingId);
    }

    @Test
    public void update() {
        // GIVEN
        final ThingId thingId = thingId("update");
        final Thing thing = createAndPutThing(thingId);

        final JsonObject attributes = JsonFactory.newObjectBuilder()
                .set(JsonFactory.newKey(KNOWN_ATTR_KEY), NEW_ATTR_VAL)
                .build();

        final Thing newThing = thing.toBuilder()
                .setAttributes(attributes)
                .build();

        // WHEN
        putThing(apiVersion.toInt(), newThing, apiVersion).fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            attribute(KNOWN_ATTR_KEY).eq(NEW_ATTR_VAL));
                    assertThat(result.getItems()).isNotEmpty();
                });
    }

    @Test
    public void updateAllAttributes() {
        // GIVEN
        final ThingId thingId = thingId("updateAllAttributes");
        createAndPutThing(thingId);

        final JsonObject attributes = JsonFactory.newObjectBuilder()
                .set(JsonFactory.newKey(KNOWN_ATTR_KEY), NEW_ATTR_VAL)
                .build();

        // WHEN
        putAttributes(apiVersion.toInt(), thingId, attributes.toString()).fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            attribute(KNOWN_ATTR_KEY).eq(NEW_ATTR_VAL));
                    assertThat(result.getItems()).isNotEmpty();
                });
    }

    @Test
    public void updateSingleAttribute() {
        // GIVEN
        final ThingId thingId = thingId("updateSingleAttribute");
        createAndPutThing(thingId);

        // WHEN
        putAttribute(apiVersion.toInt(), thingId, KNOWN_ATTR_KEY, "\"" + NEW_ATTR_VAL + "\"").fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            attribute(KNOWN_ATTR_KEY).eq(NEW_ATTR_VAL));
                    assertThat(result.getItems()).isNotEmpty();
                });
    }

    @Test
    public void updateSingleComplexAttribute() {
        // GIVEN
        final ThingId thingId = thingId("updateSingleComplexAttribute");
        createAndPutThing(thingId);

        final String subKey = "subKey";
        final String subValue = "subValue";
        final JsonObject complexValue = JsonObject.newBuilder()
                .set(subKey, subValue)
                .set("otherSubKey", "otherSubValue")
                .build();


        // WHEN
        putAttribute(apiVersion.toInt(), thingId, KNOWN_ATTR_KEY, complexValue.toString()).fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            attribute(KNOWN_ATTR_KEY + "/" + subKey).eq(subValue));
                    assertThat(result.getItems()).isNotEmpty();
                });
    }

    @Test
    public void deleteSingleAttribute() {
        // GIVEN
        final ThingId thingId = thingId("deleteSingleAttribute");
        createAndPutThing(thingId);

        // WHEN
        deleteAttribute(apiVersion.toInt(), thingId, KNOWN_ATTR_KEY).fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            attribute(KNOWN_ATTR_KEY).eq(KNOWN_ATTR_VAL));
                    assertThat(result.getItems()).isEmpty();
                });
    }

    @Test
    public void createFeatureAndChangeProperty() {
        // GIVEN
        final ThingId thingId = thingId("createFeatureAndChangeProperty");
        createAndPutThing(thingId);
        final String featureId = "knownFeatureId";
        final String newPropertyValue = "newPropertyValue";

        // create feature
        // WHEN
        putFeature(apiVersion.toInt(), thingId, featureId, createFeature(featureId).toJsonString()).fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            featureProperty(FEATURE_PROPERTY_KEY).eq(FEATURE_PROPERTY_VAL));
                    assertThat(result.getItems()).isNotEmpty();
                });


        // update property
        // WHEN
        putProperty(apiVersion.toInt(), thingId, featureId, FEATURE_PROPERTY_KEY,
                "\"" + newPropertyValue + "\"").fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            featureProperty(FEATURE_PROPERTY_KEY).eq(newPropertyValue));
                    assertThat(result.getItems()).isNotEmpty();
                });

        // delete property
        // WHEN
        deleteProperty(apiVersion.toInt(), thingId, featureId, FEATURE_PROPERTY_KEY).fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            featureProperty(FEATURE_PROPERTY_KEY).eq(newPropertyValue));
                    assertThat(result.getItems()).isEmpty();
                });
    }

    @Test
    public void updateFeatures() {
        // GIVEN
        final ThingId thingId = thingId("updateFeatures");
        createAndPutThing(thingId);

        final Feature f1 = createFeature("f1");
        final Feature f2 = createFeature("f2");

        // update features
        // WHEN
        putFeatures(apiVersion.toInt(), thingId, ThingsModelFactory.newFeatures(Arrays.asList(f1, f2))
                .toJsonString()).fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            featureProperty(FEATURE_PROPERTY_KEY).eq(FEATURE_PROPERTY_VAL));
                    assertThat(result.getItems()).isNotEmpty();
                });

        // update features
        // WHEN
        deleteFeatures(apiVersion.toInt(), thingId).fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            featureProperty(FEATURE_PROPERTY_KEY).eq(FEATURE_PROPERTY_VAL));
                    assertThat(result.getItems()).isEmpty();
                });

    }

    @Test
    public void updateFeaturesWithDottedFeatureId() {
        // GIVEN
        final ThingId thingId = thingId("updateFeaturesWithDottedFeatureId");
        createAndPutThing(thingId);

        final String featureId = "the.feature.id.0";
        final Feature feature = createFeature(featureId);

        // WHEN
        putFeatures(apiVersion.toInt(), thingId, ThingsModelFactory.newFeatures(Collections.singletonList(feature))
                .toJsonString()).fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            featureProperty(FEATURE_PROPERTY_KEY).eq(FEATURE_PROPERTY_VAL));
                    assertThat(result.getItems()).isNotEmpty();
                });
    }

    @Test
    public void updateFeatureProperties() {
        // GIVEN
        final ThingId thingId = thingId("updateFeatureProperties");
        createAndPutThing(thingId);

        final String featureId = "knownFeatureId";
        final Feature feature = createFeature(featureId);
        final String newPropertyKey = "newPropertyKey";
        final String newPropertyVal = "newPropertyVal";

        // create feature
        // WHEN
        putFeature(apiVersion.toInt(), thingId, featureId, feature.toJsonString()).fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            featureProperty(FEATURE_PROPERTY_KEY).eq(FEATURE_PROPERTY_VAL));
                    assertThat(result.getItems()).isNotEmpty();
                });

        // update properties
        // WHEN
        final FeatureProperties newProperties = newFeatureProperties(newPropertyKey, newPropertyVal);
        putProperties(apiVersion.toInt(), thingId, featureId, newProperties.toJsonString()).fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            featureProperty(newPropertyKey).eq(newPropertyVal));
                    assertThat(result.getItems()).isNotEmpty();
                });

        // delete properties
        // WHEN
        deleteProperties(apiVersion.toInt(), thingId, featureId).fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            featureProperty(newPropertyKey).eq(newPropertyVal));
                    assertThat(result.getItems()).isEmpty();
                });
    }

    @Test(timeout = Long.MAX_VALUE) // effectively disable global timeout, because timeout is handled by the test itself
    public void crudAndRecreate() {
        final ThingId thingId = thingId("crudAndRecreate");
        final Thing thing = createAndPutThing(thingId);

        // test that the syncer adds the new thing to the search index
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId);
                    assertThat(result.getItems()).isNotEmpty();
                });

        // change the thing by updating an attribute
        putAttribute(apiVersion.toInt(), thingId, KNOWN_ATTR_KEY, "\"" + NEW_ATTR_VAL + "\"").fire();

        // test that the syncer updates the relation in the search index
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            attribute(KNOWN_ATTR_KEY).eq(NEW_ATTR_VAL));
                    assertThat(result.getItems()).isNotEmpty();
                });

        // delete the thing
        deleteThing(apiVersion.toInt(), thingId).expectingHttpStatus(HttpStatus.NO_CONTENT).fire();
        deletePolicy(thingId).fire();

        //now the thing should be deleted from the search index by the syncer after some time
        Awaitility.await().atMost(CONF.getThingsSyncWaitTimeout()).untilAsserted(() -> {
            final SearchResult result = searchWithIdRestriction(thingId);
            assertThat(result.getItems()).isEmpty();
        });

        putThing(apiVersion.toInt(), thing, apiVersion)
                .useAwaitility(Awaitility.await())
                .expectingHttpStatus(HttpStatus.CREATED).fire();

        // test that the syncer adds the new thing to the search index again
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId);
                    assertThat(result.getItems()).isNotEmpty();
                });
    }

    private Thing createAndPutThing(final ThingId thingId) {
        final Attributes attributes = ThingsModelFactory.newAttributesBuilder()
                .set(KNOWN_ATTR_KEY, KNOWN_ATTR_VAL)
                .build();
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttributes(attributes)
                .build();

        putThing(apiVersion.toInt(), thing, apiVersion).expectingHttpStatus(HttpStatus.CREATED).fire();

        // THEN
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId);
                    assertThat(result.getItems()).isNotEmpty();
                });

        return thing;
    }

    private SearchResult search(final SearchFilter searchFilter) {
        return SearchResponseExtractors.asSearchResult(searchThings(apiVersion).filter(searchFilter).fire().asString());
    }

    private SearchResult searchWithIdRestriction(final ThingId id) {
        requireNonNull(id);

        final SearchFilter filter = createFilterWithIdRestriction(id, null);

        return search(filter);
    }

    private SearchResult searchWithIdRestriction(final ThingId id,
            final SearchFilter additionalFilter) {
        requireNonNull(id);
        requireNonNull(additionalFilter);

        final SearchFilter filter = createFilterWithIdRestriction(id, additionalFilter);

        return search(filter);
    }

    private SearchFilter createFilterWithIdRestriction(final ThingId id,
            @Nullable final SearchFilter additionalFilter) {
        requireNonNull(id);

        final SearchFilter idFilter = idFilter(id);

        if (additionalFilter == null) {
            return idFilter;
        }

        return and(idFilter, additionalFilter);
    }
}
