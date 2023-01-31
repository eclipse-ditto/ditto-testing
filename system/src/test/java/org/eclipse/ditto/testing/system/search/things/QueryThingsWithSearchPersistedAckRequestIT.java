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

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.attribute;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.created;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.definition;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.and;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.testing.common.Timeout;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.matcher.search.SearchResponseExtractors;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingDefinition;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SearchResult;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Timeout(millis = Long.MAX_VALUE)
@Category(Acceptance.class)
public final class QueryThingsWithSearchPersistedAckRequestIT extends VersionedSearchIntegrationTest {

    private static final String KNOWN_ATTR_KEY = "attrKey";
    private static final String KNOWN_ATTR_VAL = "attrVal";
    private static final String NEW_ATTR_VAL = "attrNewVal";

    @Test
    public void createAndUpdateWithSearchPersistedAck() {
        // GIVEN
        final ThingId thingId = thingId("createAndUpdateWithSearchPersistedAck");
        final Attributes attributes = ThingsModelFactory.newAttributesBuilder()
                .set(KNOWN_ATTR_KEY, KNOWN_ATTR_VAL)
                .build();
        final ThingDefinition thingDefinition = ThingsModelFactory.newDefinition("foo:bar:123");
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttributes(attributes)
                .setDefinition(thingDefinition)
                .build();

        // WHEN
        putThing(apiVersion.toInt(), thing, apiVersion)
                .withHeader("requested-acks", "twin-persisted,search-persisted")
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        putAttribute(apiVersion.toInt(), thingId, KNOWN_ATTR_KEY, "\"" + NEW_ATTR_VAL + "\"")
                .withHeader("requested-acks", "twin-persisted,search-persisted")
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        // THEN
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    final SearchResult result = searchWithIdRestriction(thingId,
                            and(
                                    created().le(Instant.now().toString()),
                                    definition().eq(thingDefinition.toString()),
                                    attribute(KNOWN_ATTR_KEY).eq(NEW_ATTR_VAL)
                            )
                    );
                    assertThat(result.getItems()).isNotEmpty();
                });
    }

    private SearchResult searchWithIdRestriction(final ThingId id, final SearchFilter additionalFilter) {
        requireNonNull(id);
        requireNonNull(additionalFilter);

        final SearchFilter filter = createFilterWithIdRestriction(id, additionalFilter);

        return search(filter);
    }

    private SearchResult search(final SearchFilter searchFilter) {
        return SearchResponseExtractors.asSearchResult(searchThings(apiVersion).filter(searchFilter).fire().asString());
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
