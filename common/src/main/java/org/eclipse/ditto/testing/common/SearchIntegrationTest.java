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
package org.eclipse.ditto.testing.common;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.eclipse.ditto.base.model.acks.DittoAcknowledgementLabel;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.json.FieldType;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonMissingFieldException;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.categories.Search;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.testing.common.matcher.search.SearchProperties;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureDefinition;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SearchModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchResult;
import org.eclipse.ditto.thingsearch.model.SearchResultBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for search integration tests.
 */
@Category(Search.class)
public abstract class SearchIntegrationTest extends IntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SearchIntegrationTest.class);

    /**
     * The maximum value of the limit parameter.
     */
    protected static final int MAX_PAGE_SIZE = 200;

    /**
     * The maximum value of the limit parameter.
     */
    protected static final int DEFAULT_PAGE_SIZE = 25;

    protected static List<JsonSchemaVersion> apiVersion2 = Collections.singletonList(JsonSchemaVersion.V_2);

    private static boolean isFirstTest = true;

    protected static SearchMatcher searchThings(final JsonSchemaVersion version) {
        final SearchMatcher searchMatcher = SearchMatcher.getResult(version);
        // per default only return the ids -> makes checking results easier
        searchMatcher.returnIdsOnly(true);
        return restMatcherConfigurer.configure(searchMatcher);
    }

    protected static SearchMatcher searchCount(final JsonSchemaVersion version) {
        return restMatcherConfigurer.configure(SearchMatcher.getCount(version));
    }

    protected static SearchFilter idFilter(final CharSequence id) {
        return SearchProperties.thingId().eq(id.toString());
    }

    protected static SearchFilter idsFilter(final List<? extends CharSequence> ids) {
        requireNonNull(ids);

        if (ids.isEmpty()) {
            return SearchProperties.thingId().in(Collections.emptyList());
        }

        final String firstId = ids.get(0).toString();
        final String[] furtherIds = ids.subList(1, ids.size()).stream().map(String::valueOf).toArray(String[]::new);
        return SearchProperties.thingId().in(firstId, furtherIds);
    }

    protected static SearchResult toSortedThingResult(final Iterable<ThingId> thingIds) {
        final List<ThingId> thingIdsSorted = new ArrayList<>();
        thingIds.forEach(thingIdsSorted::add);
        Collections.sort(thingIdsSorted);

        return buildThingsResult(thingIdsSorted, FIELD_NAME_THING_ID, SearchResult.NO_NEXT_PAGE);
    }

    protected static SearchResult toThingResult(final ThingId... thingIds) {
        final List<ThingId> thingIdList = Arrays.asList(thingIds);
        return buildThingsResult(thingIdList, FIELD_NAME_THING_ID, SearchResult.NO_NEXT_PAGE);
    }

    protected static SearchResult toThingResult(final long nextPageOffset, final ThingId... thingIds) {
        final List<ThingId> thingIdList = Arrays.asList(thingIds);
        return buildThingsResult(thingIdList, FIELD_NAME_THING_ID, nextPageOffset);
    }

    protected static SearchResult toThingResult(final long nextPageOffset, final Iterable<ThingId> thingIds) {
        return buildThingsResult(thingIds, FIELD_NAME_THING_ID, nextPageOffset);
    }

    private static SearchResult buildThingsResult(final Iterable<ThingId> thingIds, final CharSequence fieldName,
            final long nextPageOffset) {
        final SearchResultBuilder resultBuilder = SearchModelFactory.newSearchResultBuilder();
        resultBuilder.nextPageOffset(nextPageOffset);

        thingIds.forEach(id -> {
            final JsonValue thingJsonObject = JsonFactory.newObjectBuilder()
                    .set(fieldName.toString(), id.toString())
                    .build();
            resultBuilder.add(thingJsonObject);
        });
        return resultBuilder.build();
    }

    @BeforeClass
    public static void resetIsFirstTest() {
        isFirstTest = true;
    }

    @Before
    public void before() {
        if (isFirstTest) {
            createTestData();
            isFirstTest = false;
        }
    }

    /**
     * Initialize the testdata.
     */
    protected void createTestData() {
        // no test data per default
    }

    protected static Thing createRandomThing(final String thingId) {
        return createRandomThing(ThingId.of(thingId));
    }

    protected static Thing createRandomThing(final ThingId thingId) {
        final Attributes randomAttributes = Attributes.newBuilder()
                .set(randomString(), randomString())
                .set(randomString(),
                        RandomUtils.nextInt(0, Integer.MAX_VALUE))
                .set(randomString(),
                        RandomUtils.nextFloat(Float.MIN_VALUE, Float.MAX_VALUE))
                .build();

        final FeatureProperties randomFeatureProperties = FeatureProperties.newBuilder()
                .set(randomString(), randomString())
                .set(randomString(),
                        RandomUtils.nextInt(0, Integer.MAX_VALUE))
                .set(randomString(),
                        RandomUtils.nextFloat(Float.MIN_VALUE, Float.MAX_VALUE))
                .build();
        final String randomFeatureDefId = RandomStringUtils.randomAlphabetic(8) + ":" +
                UUID.randomUUID() + ":" + RandomStringUtils.randomNumeric(1);
        final Feature randomFeature = Feature.newBuilder()
                .properties(randomFeatureProperties)
                .definition(FeatureDefinition.fromIdentifier(randomFeatureDefId))
                .withId(randomString())
                .build();

        return Thing.newBuilder()
                .setAttributes(randomAttributes)
                .setFeature(randomFeature)
                .setId(thingId).build();
    }

    protected static String randomString() {
        return RandomStringUtils.randomAlphabetic(10);
    }

    /**
     * Create a Thing with random ID and wait until it is in search index.
     *
     * @param thing Content of the thing. Thing ID is randomized.
     * @param version The Json schema version.
     * @return Thing ID of the created thing.
     */
    protected static ThingId persistThingAndWaitTillAvailable(final Thing thing, final JsonSchemaVersion version,
            final AuthClient authClient) {
        final String thingJson = thing.toJsonString(version, FieldType.regularOrSpecial());
        return persistThingsAndWaitTillAvailable(thingJson, 1, version, authClient).iterator().next();
    }

    /**
     * Create a Thing with random ID and wait until it is in search index.
     *
     * @param thing Content of the thing. Thing ID is randomized.
     * @param policy the policy of the thing.
     * @return Thing ID of the created thing.
     */
    protected ThingId persistThingAndWaitTillAvailable(final Thing thing, final Policy policy,
            final AuthClient authClient) {
        final String jsonString =
                thing.toJson().toBuilder().set(Policy.INLINED_FIELD_NAME, policy.toJson()).build().toString();
        return persistThingsAndWaitTillAvailable(jsonString, 1, JsonSchemaVersion.V_2, authClient).iterator()
                .next();
    }

    protected static Set<ThingId> persistThingsAndWaitTillAvailable(final String thingJson, final long n,
            final JsonSchemaVersion version, final AuthClient authClient) {

        return persistThingsAndWaitTillAvailable(thingJson, authClient, n, version);
    }

    public static Set<ThingId> persistThingsAndWaitTillAvailable(final Function<Long, Thing> thingBuilder,
            final long n, final JsonSchemaVersion version) {

        final AuthClient authClient = serviceEnv.getDefaultTestingContext().getOAuthClient();

        return persistThingsAndWaitTillAvailable(thingBuilder, authClient, n, version);
    }

    public static Set<ThingId> persistThingsAndWaitTillAvailable(final Function<Long, Thing> thingBuilder,
            final AuthClient authClient, final long n, final JsonSchemaVersion version) {
        if (n == 0) {
            return new LinkedHashSet<>();
        }

        final Set<ThingId> thingIds = new LinkedHashSet<>();
        for (long i = 0; i < n; i++) {
            final Thing thing = thingBuilder.apply(i);
            final JsonObject replacedJsonObject = thing.toJson(version);
            thingIds.add(replacedJsonObject.getValue(Thing.JsonFields.ID)
                    .map(ThingId::of)
                    .orElseThrow(() -> new JsonMissingFieldException(Thing.JsonFields.ID)));
            logProgress("Persisting Thing", i, n);
            putThing(version.toInt(), replacedJsonObject, version)
                    .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                            DittoAcknowledgementLabel.SEARCH_PERSISTED.toString())
                    .withHeader(DittoHeaderDefinition.TIMEOUT.getKey(), "30s")
                    .withJWT(authClient.getAccessToken())
                    .expectingStatusCodeSuccessful()
                    .fire();
            logProgress("Successfully persisted Thing", i, n);
        }

        return thingIds;
    }

    private static Set<ThingId> persistThingsAndWaitTillAvailable(final String thingJson,
            final AuthClient authClient,
            final long n, final JsonSchemaVersion version) {
        if (n == 0) {
            return new LinkedHashSet<>();
        }

        final Set<ThingId> thingIds = new LinkedHashSet<>();
        for (long i = 0; i < n; i++) {
            // Do NOT convert the JSON to a Thing, in this case an inline-policy will get lost!
            final JsonObject thingJsonObject = JsonFactory.newObject(thingJson);
            thingIds.add(thingJsonObject.getValue(Thing.JsonFields.ID)
                    .map(ThingId::of)
                    .orElseThrow(() -> new JsonMissingFieldException(Thing.JsonFields.ID)));
            logProgress("Persisting Thing", i, n);
            final var response = putThing(version.toInt(), thingJsonObject, version)
                    .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                            DittoAcknowledgementLabel.SEARCH_PERSISTED.toString())
                    .withHeader(DittoHeaderDefinition.TIMEOUT.getKey(), "30s")
                    .withJWT(authClient.getAccessToken())
                    .expectingStatusCodeSuccessful()
                    .fire();
            if (n == 1) {
                LOGGER.info("SearchPersisted <{}> body=<{}>", response.getStatusLine(), response.body().asString());
            }
            logProgress("Successfully persisted Thing", i, n);
        }

        return thingIds;
    }

    private static void logProgress(final String message, final long i, final long n) {
        if (i % 500 == 0 || i == (n - 1)) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("{} ({}/{})", message, i + 1, n);
            }
        }
    }

    public static ThingId thingId(final String idPrefix) {
        requireNonNull(idPrefix);
        return ThingId.of(idGenerator().withPrefixedRandomName("thing-" + idPrefix));
    }

    public static PolicyId policyId(final String idPrefix) {
        requireNonNull(idPrefix);
        return PolicyId.of(idGenerator().withPrefixedRandomName("policy-" + idPrefix));
    }

    private static Set<String> extractThingIds(final JsonArray itemsArray) {
        return itemsArray.stream()
                .map(JsonValue::asObject)
                .map(item -> item.getValue(FIELD_NAME_THING_ID))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(JsonValue::asString)
                // use LinkedHashSet to preserve order
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
