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


import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.attribute;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEmpty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.satisfiesSearchResult;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.and;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSizeOption;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSortOption;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.or;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.property;

import org.eclipse.ditto.base.model.acks.DittoAcknowledgementLabel;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.IdGenerator;
import org.eclipse.ditto.testing.common.SearchIntegrationTest;
import org.eclipse.ditto.testing.common.Timeout;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.testing.common.matcher.search.SortProperties;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureDefinition;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SizeOption;
import org.eclipse.ditto.thingsearch.model.SortOption;
import org.eclipse.ditto.thingsearch.model.SortOptionEntry;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Timeout(millis = Long.MAX_VALUE)
@RunWith(Enclosed.class)
@Category(Acceptance.class)
public final class QueryThingsIT {

    @RunWith(Parameterized.class)
    public static final class QueryThingsWithV2 extends VersionedSearchIntegrationTest {

        private static final String ATTR1_KEY = "attr1";
        private static final String ATTR2_KEY = "attr2";
        private static final String ATTR3_KEY = "attr3";
        private static final String ATTR4_KEY = "attr4";

        private static final String THING1_ATTR1_VALUE = "value1_1";
        private static final int THING1_ATTR2_VALUE = 1234;
        private static final boolean THING1_ATTR3_VALUE = true;
        private static final double THING1_ATTR4_VALUE = 12.987;

        private static final String THING2_ATTR1_VALUE = "value2_1";
        private static final String THING2_ATTR2_VALUE = "value2_2";

        private static final String NOT_MATCHED = "notMatched";

        private static final int USER1_THINGS_COUNT = 12;

        private static ThingId thing1Id;
        private static ThingId thing2Id;

        protected void createTestData() {
            persistThingsAndWaitTillAvailable(i ->
                    createThingWithRandomAttrValues(createThingId("user1", idGenerator())), 10);
            thing1Id = persistThingAndWaitTillAvailable(createThing1());
            thing2Id = persistThingAndWaitTillAvailable(createThing2());
            final AuthClient authClient = serviceEnv.getDefaultTestingContext().getOAuthClient();
            persistThingsAndWaitTillAvailable(
                    i -> createThingWithRandomAttrValues(createThingId("user2", idGenerator())),
                    authClient, 4, V_2);
        }

        private static Thing createThingWithRandomAttrValues(final ThingId thingId) {
            final Attributes attrs = Attributes.newBuilder()
                    .set(ATTR1_KEY, randomString())
                    .set(ATTR2_KEY, randomString())
                    .set(ATTR3_KEY, randomString())
                    .build();

            return Thing.newBuilder().setId(thingId)
                    .setAttributes(attrs)
                    .build();
        }

        private static Thing createThing1() {
            final Attributes attrs = Attributes.newBuilder()
                    .set(ATTR1_KEY, THING1_ATTR1_VALUE)
                    .set(ATTR2_KEY, THING1_ATTR2_VALUE)
                    .set(ATTR3_KEY, THING1_ATTR3_VALUE)
                    .set(ATTR4_KEY, THING1_ATTR4_VALUE)
                    .build();

            final Feature feature = Feature.newBuilder()
                    .definition(FeatureDefinition.fromIdentifier("feature:definition:1", "feature:definition:2"))
                    .withId("feature")
                    .build();

            return Thing.newBuilder().setId(createThingId("thing1", idGenerator()))
                    .setAttributes(attrs)
                    .setFeature(feature)
                    .build();
        }

        private static Thing createThing2() {
            final Attributes attrs = Attributes.newBuilder()
                    .set(ATTR1_KEY, THING2_ATTR1_VALUE)
                    .set(ATTR2_KEY, THING2_ATTR2_VALUE)
                    .set(ATTR3_KEY, randomString())
                    .build();

            return Thing.newBuilder().setId(createThingId("thing2", idGenerator()))
                    .setAttributes(attrs)
                    .build();
        }

        @Test
        public void queryOneThing() {
            search(attribute(ATTR1_KEY).eq(THING1_ATTR1_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id)));
        }

        @Test
        public void queryThingsWithoutMatching() {
            search(attribute(ATTR1_KEY).eq(NOT_MATCHED))
                    .expectingBody(isEmpty())
                    .fire();
        }

        @Test
        public void queryOneThingWithOr() {
            search(or(attribute(ATTR1_KEY).eq(THING1_ATTR1_VALUE),
                    attribute(ATTR1_KEY).eq(NOT_MATCHED)))
                    .expectingBody(isEqualTo(toThingResult(thing1Id)))
                    .fire();
        }

        @Test
        public void queryTwoThingWithOr() {
            search(or(attribute(ATTR1_KEY).eq(THING1_ATTR1_VALUE),
                    attribute(ATTR1_KEY).eq(THING2_ATTR1_VALUE)))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                    .fire();
        }

        @Test
        public void queryThingsWithoutMatchingWithAnd() {
            search(and(attribute(ATTR1_KEY).eq(THING2_ATTR1_VALUE),
                    attribute(ATTR1_KEY).eq(NOT_MATCHED)))
                    .expectingBody(isEmpty())
                    .fire();
        }

        @Test
        public void queryThingWithAnd() {
            search(and(attribute(ATTR1_KEY).eq(THING2_ATTR1_VALUE),
                    attribute(ATTR2_KEY).eq(THING2_ATTR2_VALUE)))
                    .expectingBody(isEqualTo(toThingResult(thing2Id)))
                    .fire();
        }

        @Test
        public void queryThingWithNumber() {
            search(attribute(ATTR2_KEY).eq(THING1_ATTR2_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id)))
                    .fire();
        }

        @Test
        public void queryThingWithFloatNumber() {
            search(attribute(ATTR4_KEY).eq(THING1_ATTR4_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id)))
                    .fire();
        }

        @Test
        public void queryThingWithBoolean() {
            search(attribute(ATTR3_KEY).eq(THING1_ATTR3_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id)))
                    .fire();
        }

        @Test
        public void queryThingIDWithEmptySearchParam() {
            /* check if size is ge than USER1_THINGS_COUNT, which are the things created by this test with user1.
             * We have to use ge, because other tests also create things. */
            search(100).returnIdsOnly(true)
                    .expectingBody(satisfiesSearchResult(result ->
                            assertThat(result.getItems().getSize()).isGreaterThanOrEqualTo(USER1_THINGS_COUNT)))
                    .fire();
        }

        @Test
        public void queryFullThingWithEmptySearchParam() {
            /* check if size is ge than USER1_THINGS_COUNT, which are the things created by this test with user1.
             * We have to use ge, because other tests also create things. */
            search(100).returnIdsOnly(false)
                    .expectingBody(satisfiesSearchResult(result ->
                            assertThat(result.getItems().getSize()).isGreaterThanOrEqualTo(USER1_THINGS_COUNT)))
                    .fire();
        }

        @Test
        public void searchForThingsWithoutPermission() {
            unauthorizedSearch(attribute(ATTR1_KEY).eq(THING1_ATTR1_VALUE))
                    .expectingBody(isEmpty())
                    .fire();
        }

        @Test
        public void searchForFeatureDefinition() {
            search(property("features/feature/definition").eq("feature:definition:1"))
                    .expectingBody(isEqualTo(toThingResult(thing1Id)))
                    .fire();
        }

        @Test
        public void searchForWildcardFeatureDefinition() {
            search(property("features/*/definition").eq("feature:definition:1"))
                    .expectingBody(isEqualTo(toThingResult(thing1Id)))
                    .fire();
        }

        private SearchMatcher search(final SearchFilter searchFilter) {
            return search(searchFilter, apiVersion);
        }

        private static SearchMatcher search(final SearchFilter searchFilter, final JsonSchemaVersion apiVersion) {
            return searchThings(apiVersion)
                    .filter(searchFilter)
                    .option(newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC));
        }

        private SearchMatcher search(final int limit) {return search(limit, apiVersion);}

        private static SearchMatcher search(
                final int limit, final JsonSchemaVersion apiVersion) {
            final SortOption sortOption = newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC);
            final SizeOption sizeOption = newSizeOption(limit);

            return searchThings(apiVersion).options(sortOption, sizeOption);
        }

        private SearchMatcher unauthorizedSearch(final SearchFilter searchFilter) {
            return search(searchFilter).withJWT(serviceEnv.getTestingContext5().getOAuthClient().getAccessToken());
        }
    }

    public static final class QueryThingsOnlyForV2 extends SearchIntegrationTest {

        private static final JsonSchemaVersion JSON_SCHEMA_VERSION = V_2;
        private static ThingId thing1Id;

        protected void createTestData() {
            final AuthClient authClient = serviceEnv.getDefaultTestingContext().getOAuthClient();
            final Thing thing1 =
                    ThingsModelFactory.newThingBuilder().setId(createThingId("thing1", idGenerator())).build();
            thing1Id = persistThingAndWaitTillAvailable(thing1, JSON_SCHEMA_VERSION, authClient);
        }

        @Test
        public void queryOneThingAfterMergeToCheckIndexUpdate() {
            final String mergeAttr = "attributes/mergeAttr";
            final String checkValue = "checkValue";
            final JsonPointer mergeFullMergeAttributePath = JsonPointer.of(mergeAttr);

            patchThing(thing1Id, mergeFullMergeAttributePath, JsonValue.of(checkValue))
                    .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                            DittoAcknowledgementLabel.SEARCH_PERSISTED.toString())
                    .withHeader(DittoHeaderDefinition.TIMEOUT.getKey(), "30s")
                    .expectingStatusCodeSuccessful()
                    .fire();

            search(attribute(mergeAttr).eq(checkValue))
                    .expectingBody(isEqualTo(toThingResult(thing1Id)));
        }

        private static SearchMatcher search(final SearchFilter searchFilter) {
            return searchThings(JSON_SCHEMA_VERSION)
                    .filter(searchFilter)
                    .option(newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC));
        }
    }

    private static ThingId createThingId(final String prefix, final IdGenerator idGenerator) {
        return ThingId.of(idGenerator.withPrefixedRandomName(QueryThingsOnlyForV2.class.getSimpleName(), prefix));
    }
}
