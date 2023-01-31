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


import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.attribute;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSortOption;

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.testing.common.matcher.search.SortProperties;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SortOptionEntry;
import org.junit.Test;

/**
 *
 */
public final class QueryThingsEncodingIT extends VersionedSearchIntegrationTest {

    private static final String SIMPLE_ATTR_KEY = "test";
    private static final String SIMPLE_ATTR_VALUE = "foo";
    private static final String ENCODED_URL_KEY = "encodedUrl";
    private static final String COMPLEX_KEY = "complex";
    private static final String ENCODED_URL_VALUE = "http%3A%2F%2FsomeUrl%2FsomeId";
    private static final String COMPLEX_URL_VALUE = "!#$%&'()*+,/:;=?@[\\]{|}\" äaZ0";
    private static final String COMPLEX_THING_NAME_PREFIX =
            "com.prosyst.mprm.ngpoc.mprm.osgi.device::AVGUSTIN::mprm.osgi.system.props::System+Properties";

    private static ThingId encodingThingId;
    private static ThingId complexThingId;

    protected void createTestData() {
        encodingThingId = persistThingAndWaitTillAvailable(createEncodingThing());
        complexThingId = persistThingAndWaitTillAvailable(createThingWithComplexThingId());
    }

    private Thing createEncodingThing() {
        final Attributes attrs = Attributes.newBuilder()
                .set(SIMPLE_ATTR_KEY, SIMPLE_ATTR_VALUE)
                .set(ENCODED_URL_KEY, ENCODED_URL_VALUE)
                .set(COMPLEX_KEY, COMPLEX_URL_VALUE)
                .build();
        return Thing.newBuilder()
                .setId(createThingId("encoding"))
                .setAttributes(attrs)
                .build();
    }

    private Thing createThingWithComplexThingId() {
        return Thing.newBuilder()
                .setId(createThingId(COMPLEX_THING_NAME_PREFIX))
                .build();
    }

    private ThingId createThingId(final String prefix) {
        return ThingId.of(idGenerator().withPrefixedRandomName(getClass().getSimpleName(), prefix));
    }

    @Test
    public void queryForSimpleAttribute() {
        search(attribute(SIMPLE_ATTR_KEY).eq(SIMPLE_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(encodingThingId)))
                .fire();
    }

    @Test
    public void queryForEncodedUrlAttribute() {
        search(attribute(ENCODED_URL_KEY).eq(ENCODED_URL_VALUE))
                .expectingBody(isEqualTo(toThingResult(encodingThingId)))
                .fire();
    }

    @Test
    public void queryForComplexSpecialCharsAttribute() {
        search(attribute(COMPLEX_KEY).eq(COMPLEX_URL_VALUE))
                .expectingBody(isEqualTo(toThingResult(encodingThingId)))
                .fire();
    }

    @Test
    public void queryForComplexThingId() {
        search(idFilter(complexThingId))
                .expectingBody(isEqualTo(toThingResult(complexThingId)))
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
}
