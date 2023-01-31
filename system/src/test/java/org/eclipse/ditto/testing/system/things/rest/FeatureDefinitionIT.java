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
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.things.model.FeatureDefinition;
import org.junit.Test;

/**
 * Integration test for the Feature Definition REST API.
 */
public final class FeatureDefinitionIT extends IntegrationTest {

    private static final String VALID_IDENTIFIER = "org.eclipse.ditto:awesomeness:42.0";

    @Test
    public void putGetAndDeleteDefinitionApiV2() {
        executePutGetAndDeleteDefinition(TestConstants.API_V_2, "awesome2");
    }

    @Test
    public void putInvalidDefinitionApiV2() {
        final String featureId = "invalid-definition2";
        executePutInvalidDefinition(TestConstants.API_V_2, featureId);
    }

    @Test
    public void putInvalidDefinitionSpecialCharsApiV2() {
        final String featureId = "special-chars2";
        executePutInvalidDefinitionSpecialChars(TestConstants.API_V_2, featureId);
    }

    @Test
    public void putMultipleDefinitionIdentifiersApiV2() {
        final String featureId = "multiple2";
        executePutMultipleDefinitionIdentifiers(TestConstants.API_V_2, featureId);
    }

    private static void executePutGetAndDeleteDefinition(final int apiVersion, final CharSequence featureId) {
        final String thingId = prepareThingAndFeature(apiVersion, featureId);

        final FeatureDefinition definition = FeatureDefinition.fromIdentifier(VALID_IDENTIFIER);
        final JsonArray definitionJsonArray = definition.toJson();

        getDefinition(apiVersion, thingId, featureId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        putDefinition(apiVersion, thingId, featureId, definitionJsonArray.toString())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getDefinition(apiVersion, thingId, featureId)
                .expectingBody(contains(definitionJsonArray))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteDefinition(apiVersion, thingId, featureId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getDefinition(apiVersion, thingId, featureId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    private static void executePutInvalidDefinition(final int apiVersion, final CharSequence featureId) {
        final String thingId = prepareThingAndFeature(apiVersion, featureId);

        putInvalidDefinition(apiVersion, thingId, featureId, "[]"); // FeatureDefinition requires at least 1 identifier
        putInvalidDefinition(apiVersion, thingId, featureId, "[\"org.eclipse.ditto:wontwork\"]"); // invalid identifier
        putInvalidDefinition(apiVersion, thingId, featureId, "[1.5]"); // invalid identifier type
    }

    private static void executePutInvalidDefinitionSpecialChars(final int apiVersion, final CharSequence featureId) {
        final String thingId = prepareThingAndFeature(apiVersion, featureId);

        putInvalidDefinition(apiVersion, thingId, featureId, "[\"org.eclipse.ditto:foo,bar:42\"]"); // no , allowed
        putInvalidDefinition(apiVersion, thingId, featureId, "[\"org.eclipse.ditto:foo.bar:42.1/3\"]"); // no / allowed
        putInvalidDefinition(apiVersion, thingId, featureId, "[\"org.eclipse.ditto::foo.bar:0\"]"); // no : allowed
        putInvalidDefinition(apiVersion, thingId, featureId, "[\"org#eclipse#ditto:foo.bar:1.0.0\"]"); // no # allowed
    }

    private static void putInvalidDefinition(final int apiVersion,
            final CharSequence thingId,
            final CharSequence featureId,
            final String definitionJson){

        putDefinition(apiVersion, thingId, featureId, definitionJson) // no # allowed
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    private static void executePutMultipleDefinitionIdentifiers(final int apiVersion, final CharSequence featureId) {
        final String thingId = prepareThingAndFeature(apiVersion, featureId);

        FeatureDefinition definition = FeatureDefinition.fromIdentifier(VALID_IDENTIFIER);
        final JsonArray definitionArray = definition.toJson();

        putDefinition(apiVersion, thingId, featureId, definitionArray.toString())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getDefinition(apiVersion, thingId, featureId)
                .expectingBody(contains(definitionArray))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        definition = FeatureDefinition.fromIdentifier(VALID_IDENTIFIER, "org.eclipse.ditto:bar:2.0.0",
                "org.eclipse.ditto:bar.test.foo:0815");
        final JsonArray definitionArrayMultiple = definition.toJson();

        putDefinition(apiVersion, thingId, featureId, definitionArrayMultiple.toString())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getDefinition(apiVersion, thingId, featureId)
                .expectingBody(contains(definitionArrayMultiple))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        putDefinition(apiVersion, thingId, featureId, "null")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getDefinition(apiVersion, thingId, featureId)
                .expectingBody(contains(JsonFactory.nullArray()))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    private static String prepareThingAndFeature(final int apiVersion, final CharSequence featureId) {
        final String location = postThing(apiVersion)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        putFeature(apiVersion, thingId, featureId, "{}")
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        return thingId;
    }

}
