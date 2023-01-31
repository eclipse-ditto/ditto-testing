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

import static org.eclipse.ditto.testing.common.assertions.IntegrationTestAssertions.assertThat;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.attribute;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchResponseExtractors;
import org.eclipse.ditto.testing.system.search.sync.common.SearchSyncTestConfig;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchResult;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tests that the search updater eventually converges a thing to the correct value.
 */
public final class ThingConcurrentUpdatesIT extends VersionedSearchIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThingConcurrentUpdatesIT.class);

    private static final SearchSyncTestConfig CONF = SearchSyncTestConfig.getInstance();

    private static final int PARALLEL_RUNS = 3;
    private static final int UPDATES_PER_RUN = 500;

    private static final String KNOWN_STRING_ATTRIBUTE_KEY = "turn_it_up";
    private static final String KNOWN_STRING_ATTRIBUTE_VALUE = "value1";

    private static final Attributes KNOWN_ATTRIBUTES = ThingsModelFactory.newAttributes(JsonFactory.newObjectBuilder()
            .set(JsonFactory.newKey(KNOWN_STRING_ATTRIBUTE_KEY), KNOWN_STRING_ATTRIBUTE_VALUE).build());

    @Test(timeout = Long.MAX_VALUE) // timeout is handled by the test
    public void triggerConcurrentUpdatesOnOneProperty()
            throws InterruptedException, ExecutionException, TimeoutException {
        final ThingId thingId = thingId("concurrentUpdatesOnOneProperty");
        createThing(thingId);

        //getAndExtractResult several updates on the same attribute with multiple clients
        LOGGER.info("Running updates in parallel...");
        runUpdatesInParallel(thingId);
        LOGGER.info("Updates finished.");

        //set final value of attribute
        final String finalValue = "finalValue";
        updateAttribute(thingId, JsonValue.of(finalValue));

        // check that the final value gets eventually updated
        // AtomicReference is just used to be able to change the value from lambda expression
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> {
                    final SearchResult result = SearchResponseExtractors.asSearchResult(
                            searchThings()
                                    .filter(attribute(KNOWN_STRING_ATTRIBUTE_KEY).eq(finalValue)).fire()
                                    .asString());
                    assertThat(result.getItems()).isNotEmpty();
                    assertThat(result).isEqualTo(toThingResult(thingId));
                });
    }

    private void runUpdatesInParallel(final CharSequence thingId)
            throws InterruptedException, ExecutionException, TimeoutException {
        final List<CompletableFuture> futures = new ArrayList<>();
        IntStream.range(0, PARALLEL_RUNS).forEach(i -> futures.add(runUpdates(thingId)));

        CompletableFuture
                .allOf(futures.toArray(new CompletableFuture[futures.size()]))
                .get(2, TimeUnit.MINUTES);
    }

    private CompletableFuture<Void> runUpdates(final CharSequence thingId) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        new Thread(() -> {
            IntStream.range(0, UPDATES_PER_RUN)
                    .mapToObj(JsonValue::of)
                    .forEach(val -> updateAttribute(thingId, val));
            future.complete(null);
        }).start();
        return future;
    }

    private void updateAttribute(final CharSequence thingId, final JsonValue value) {
        updateAttribute(thingId, value, apiVersion);
    }

    private static void updateAttribute(final CharSequence thingId,
            final JsonValue value, final JsonSchemaVersion apiVersion) {
        putAttribute(apiVersion.toInt(), thingId, KNOWN_STRING_ATTRIBUTE_KEY, value.toString())
                .expectingHttpStatus(HttpStatus.CREATED, HttpStatus.NO_CONTENT)
                .fire();
    }

    private void createThing(final ThingId thingId) {createThing(thingId, apiVersion);}

    private static void createThing(final ThingId thingId,
            final JsonSchemaVersion apiVersion) {
        // prepare
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttributes(KNOWN_ATTRIBUTES)
                .build();

        // test
        putThing(apiVersion.toInt(), thing, apiVersion).fire();
    }

}
