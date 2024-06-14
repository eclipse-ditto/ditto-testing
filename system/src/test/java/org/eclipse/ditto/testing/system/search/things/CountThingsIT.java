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

import static org.awaitility.Awaitility.await;
import static org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.attribute;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isCount;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isCountGte;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.and;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.or;
import static org.junit.Assume.assumeFalse;

import java.time.Duration;

import org.assertj.core.api.Assumptions;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.testing.common.ServiceEnvironment;
import org.eclipse.ditto.testing.common.Timeout;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.matcher.search.SearchProperties;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.response.Response;

/**
 *
 */
@Timeout(millis = Long.MAX_VALUE)
@Category(Acceptance.class)
public final class CountThingsIT extends VersionedSearchIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CountThingsIT.class);

    private static final String ATTR1_KEY = "countThingsIt-attr1";
    private static final String ATTR2_KEY = "countThingsIt-attr2";
    private static final String ATTR3_KEY = "countThingsIt-attr3";
    private static final String ATTR4_KEY = "countThingsIt-attr4";

    private static final String THING1_ATTR1_VALUE = "value1_1";
    private static final int THING1_ATTR2_VALUE = 9876;
    private static final boolean THING1_ATTR3_VALUE = true;
    private static final double THING1_ATTR4_VALUE = 13.163;

    private static final String THING2_ATTR1_VALUE = "value2_1";
    private static final String THING2_ATTR2_VALUE = "value2_2";

    private static final String NOT_MATCHED = "notMatched";
    private static final int USER1_THINGS_COUNT = 12;

    private static final String RANDOM_NAMESPACE = ServiceEnvironment.createRandomDefaultNamespace();

    protected void createTestData() {
        persistThingsAndWaitTillAvailable(i -> createRandomThing(createThingId("user1")), 10);
        persistThingAndWaitTillAvailable(createThing1());
        persistThingAndWaitTillAvailable(createThing2());
        persistThingsAndWaitTillAvailable(i -> createRandomThing(createThingId("user2")),
                serviceEnv.getDefaultTestingContext(), 4, V_2);
    }

    private static Thing createThing1() {
        final Attributes attrs = Attributes.newBuilder()
                .set(ATTR1_KEY, THING1_ATTR1_VALUE)
                .set(ATTR2_KEY, THING1_ATTR2_VALUE)
                .set(ATTR3_KEY, THING1_ATTR3_VALUE)
                .set(ATTR4_KEY, THING1_ATTR4_VALUE)
                .build();

        return Thing.newBuilder().setId(createThingId("thing1"))
                .setAttributes(attrs)
                .build();
    }

    private static Thing createThing2() {
        final Attributes attrs = Attributes.newBuilder()
                .set(ATTR1_KEY, THING2_ATTR1_VALUE)
                .set(ATTR2_KEY, THING2_ATTR2_VALUE)
                .set(ATTR3_KEY, randomString())
                .build();

        return Thing.newBuilder().setId(createThingId("thing2"))
                .setAttributes(attrs)
                .build();
    }

    private static ThingId createThingId(final String prefix) {
        return ThingId.of(
                idGenerator(RANDOM_NAMESPACE).withPrefixedRandomName(CountThingsIT.class.getSimpleName(), prefix));
    }

    @Test
    public void countOneThing() {
        assertCount(attribute(ATTR1_KEY).eq(THING1_ATTR1_VALUE), 1);
    }

    @Test
    public void countThingsWithoutMatching() {
        assertCount(attribute(ATTR1_KEY).eq(NOT_MATCHED), 0);
    }

    @Test
    public void queryOneThingWithOr() {
        assertCount(or(attribute(ATTR1_KEY).eq(THING1_ATTR1_VALUE),
                attribute(ATTR1_KEY).eq(NOT_MATCHED)), 1);
    }

    @Test
    public void queryTwoThingWithOr() {
        assertCount(or(attribute(ATTR1_KEY).eq(THING1_ATTR1_VALUE),
                attribute(ATTR1_KEY).eq(THING2_ATTR1_VALUE)), 2);
    }

    @Test
    public void queryThingsWithoutMatchingWithAnd() {
        assertCount(and(attribute(ATTR1_KEY).eq(THING1_ATTR1_VALUE),
                attribute(ATTR1_KEY).eq(NOT_MATCHED)), 0);
    }

    @Test
    public void queryThingWithAnd() {
        assertCount(and(attribute(ATTR1_KEY).eq(THING2_ATTR1_VALUE),
                attribute(ATTR2_KEY).eq(THING2_ATTR2_VALUE)), 1);
    }

    @Test
    public void queryThingWithNumber() {
        assertCount(attribute(ATTR2_KEY).eq(THING1_ATTR2_VALUE), 1);
    }

    @Test
    public void queryThingWithFloatNumber() {
        assertCount(attribute(ATTR4_KEY).eq(THING1_ATTR4_VALUE), 1);
    }

    @Test
    public void queryThingWithBoolean() {
        assertCount(attribute(ATTR3_KEY).eq(THING1_ATTR3_VALUE), 1);
    }

    @Test
    public void countThingWithEmptySearchParam() {
        /* check if size is ge than USER1_THINGS_COUNT which are the things created by this test with default user.
         * We have to use ge, because other tests also create things. */
        searchCount(apiVersion)
                .namespaces(RANDOM_NAMESPACE)
                .expectingBody(isCountGte(USER1_THINGS_COUNT))
                .fire();
    }

    @Test
    public void countThingsWithoutPermission() {
        assumeFalse(serviceEnv.getDefaultTestingContext().getBasicAuth().isEnabled());
        searchCount(apiVersion).filter(attribute(ATTR1_KEY).eq(THING1_ATTR1_VALUE))
                .namespaces(RANDOM_NAMESPACE)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isCount(0))
                .fire();
    }

    @Test
    public void thingsWithoutPolicyAreNotCounted() {
        // create a thing. it should be counted.
        Assumptions.assumeThat(apiVersion.toInt()).isEqualTo(V_2.toInt());

        final ThingId thingId = ThingId.of(idGenerator(RANDOM_NAMESPACE).withRandomName());
        persistThingAndWaitTillAvailable(Thing.newBuilder().setId(thingId).build());

        searchCount(apiVersion).filter(SearchProperties.thingId().eq(thingId.toString()))
                .namespaces(RANDOM_NAMESPACE)
                .expectingBody(isCount(1))
                .fire();

        final Response policy = getPolicy(thingId).fire();

        // delete the thing's policy. the thing is locked and should be removed from search index eventually.
        deletePolicy(thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // wait up to 2 minutes because deletion is scheduled by MongoDB.
        searchCount(apiVersion).filter(SearchProperties.thingId().eq(thingId.toString()))
                .namespaces(RANDOM_NAMESPACE)
                .useAwaitility(await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofSeconds(10)))
                .expectingBody(isCount(0))
                .fire();

        // Put the policy back to allow final cleanup of the Thing
        putPolicy(thingId, JsonObject.of(policy.print())).fire();
    }

    private void assertCount(final SearchFilter searchFilter, final long expected) {
        searchCount(apiVersion)
                .filter(searchFilter)
                .namespaces(RANDOM_NAMESPACE)
                .expectingBody(isCount(expected))
                .registerResponseConsumer(response -> {
                    if (null == response) { // in case of a bad request (for example) the response can be null
                        return;
                    }
                    final String responseBody = response.getBody().print();
                    if (Long.parseLong(responseBody) != expected) {
                        LOGGER.warn("Got not expected count <{}> where expected was: <{}>", responseBody, expected);
                        final Response searchResponse = searchThings(apiVersion)
                                .filter(searchFilter)
                                .namespaces(RANDOM_NAMESPACE)
                                .fire();
                        LOGGER.info("Search response: <{}>", searchResponse.print());
                    }
                })
                .fire();
    }
}
