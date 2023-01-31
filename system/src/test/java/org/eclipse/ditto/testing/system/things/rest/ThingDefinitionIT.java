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
package org.eclipse.ditto.testing.system.things.rest;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.things.model.ThingDefinition;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.Test;

/**
 * Integration test for the Thing Definition REST API.
 */
public final class ThingDefinitionIT extends IntegrationTest {

    private static void executePutGetAndDeleteDefinition(final int apiVersion) {
        final String thingId = prepareThing(apiVersion);

        final ThingDefinition definition = ThingsModelFactory.newDefinition("org.eclipse.ditto:awesomeness:42.0");
        final JsonValue definitionJson = JsonValue.of(definition.toString());

        getDefinition(apiVersion, thingId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        putDefinition(apiVersion, thingId, definitionJson.toString())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getDefinition(apiVersion, thingId)
                .expectingBody(contains(definitionJson))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteDefinition(apiVersion, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getDefinition(apiVersion, thingId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    private static void executePutInvalidDefinition(final int apiVersion) {
        final String thingId = prepareThing(apiVersion);

        putInvalidDefinition(apiVersion, thingId, "[]"); // ThingDefinition requires at least 1 identifier
        putInvalidDefinition(apiVersion, thingId, "[\"org.eclipse.ditto:wontwork\"]"); // invalid identifier
    }

    private static void executePutInvalidDefinitionSpecialChars(final int apiVersion) {
        final String thingId = prepareThing(apiVersion);

        putInvalidDefinition(apiVersion, thingId, "[\"org.eclipse.ditto:foo,bar:42:21\"]"); // no , allowed
        putInvalidDefinition(apiVersion, thingId, "[\"org.eclipse.ditto:foo.bar:42.1/3\"]"); // no / allowed
        putInvalidDefinition(apiVersion, thingId, "[\"org.eclipse.ditto::foo.bar:0\"]"); // no : allowed
        putInvalidDefinition(apiVersion, thingId, "[\"org#eclipse#ditto:foo.bar:1.0.0\"]"); // no # allowed
    }

    private static void putInvalidDefinition(final int apiVersion,
            final CharSequence thingId,
            final String definitionJson) {

        putDefinition(apiVersion, thingId, definitionJson) // no # allowed
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    private static String prepareThing(final int apiVersion) {
        final String location = postThing(apiVersion)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        return thingId;
    }

    @Test
    public void putGetAndDeleteDefinitionApiV2() {
        executePutGetAndDeleteDefinition(TestConstants.API_V_2);
    }

    @Test
    public void putInvalidDefinitionApiV2() {
        executePutInvalidDefinition(TestConstants.API_V_2);
    }

    @Test
    public void putInvalidDefinitionSpecialCharsApiV2() {
        executePutInvalidDefinitionSpecialChars(TestConstants.API_V_2);
    }

}
