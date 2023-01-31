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
package org.eclipse.ditto.testing.system.things.rest.conditionalheaders;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ThingJsonProducer;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.Test;

/**
 * Tests ETag and Conditional Headers on the Things API.
 */
public class ConditionalHeadersThingOnRootResourceIT extends IntegrationTest {

    private static final ThingJsonProducer THING_JSON_PRODUCER = new ThingJsonProducer();
    private static final JsonSchemaVersion VERSION = JsonSchemaVersion.V_2;
    private static final int VERSION_INT = VERSION.toInt();

    @Test
    public void successfulRequestReturnsExpectedETag() {
        final ThingId thingId = createThingWithGeneratedId();

        assertGetProvidesExpectedETag(thingId, ConditionalHeadersTestHelper.E_TAG_1);

        putThingMatcher(thingId, ConditionalHeadersTestHelper.VALUE_STR_2)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_2)
                .fire();

        assertGetProvidesExpectedETag(thingId, ConditionalHeadersTestHelper.E_TAG_2);
    }

    @Test
    public void getWithIfMatchReturnsEntityIfETagMatches() {
        final ThingId thingId = createThingWithGeneratedId();

        getThingMatcher(thingId)
                .withHeader(ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(not(isEmptyOrNullString()))
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .fire();
    }

    @Test
    public void getWithIfMatchReturnsPreconditionFailedIfETagDoesNotMatch() {
        final ThingId thingId = createThingWithGeneratedId();

        getThingMatcher(thingId)
                .withHeader(ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_2)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingBody(not(isEmptyOrNullString()))
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .fire();
    }

    @Test
    public void getWithIfMatchAsteriskReturns404IfEntityDoesNotYetExist() {
        final String thingId = idGenerator().withPrefixedRandomName(getClass().getName());

        getThingMatcher(thingId)
                .withHeader(ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.ASTERISK)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void getWithIfMatchAsteriskReturnsEntityIfEntityAlreadyExists() {
        final ThingId thingId = createThingWithGeneratedId();

        getThingMatcher(thingId)
                .withHeader(ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.ASTERISK)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(not(isEmptyOrNullString()))
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchReturnsEntityIfETagDoesNotMatch() {
        final ThingId thingId = createThingWithGeneratedId();

        getThingMatcher(thingId)
                .withHeader(ConditionalHeadersTestHelper.IF_NONE_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_2)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(not(isEmptyOrNullString()))
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchReturnsNotModifiedIfETagMatches() {
        final ThingId thingId = createThingWithGeneratedId();

        getThingMatcher(thingId)
                .withHeader(ConditionalHeadersTestHelper.IF_NONE_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .expectingHttpStatus(HttpStatus.NOT_MODIFIED)
                .expectingBody(isEmptyString())
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchAsteriskReturns404IfEntityDoesNotYetExist() {
        final ThingId thingId = ThingId.of(idGenerator().withPrefixedRandomName(getClass().getName()));

        getThingMatcher(thingId)
                .withHeader(ConditionalHeadersTestHelper.IF_NONE_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.ASTERISK)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchAsteriskReturnsNotModifiedIfEntityAlreadyExists() {
        final ThingId thingId = createThingWithGeneratedId();

        getThingMatcher(thingId)
                .withHeader(ConditionalHeadersTestHelper.IF_NONE_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.ASTERISK)
                .expectingHttpStatus(HttpStatus.NOT_MODIFIED)
                .expectingBody(isEmptyString())
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .fire();
    }

    @Test
    public void putWithIfMatchSucceedsIfETagMatches() {
        final ThingId thingId = createThingWithGeneratedId();

        putThingMatcher(thingId, ConditionalHeadersTestHelper.VALUE_STR_2)
                .withHeader(ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_2)
                .fire();

        getThingMatcher(thingId).expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_2).fire();
    }

    @Test
    public void putWithIfMatchFailsIfETagDoesNotMatch() {
        final ThingId thingId = createThingWithGeneratedId();

        putThingMatcher(thingId, ConditionalHeadersTestHelper.VALUE_STR_2)
                .withHeader(ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_2)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .expectingBody(containsString(ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY))
                .fire();

        getThingMatcher(thingId).expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1).fire();
    }

    @Test
    public void putWithIfMatchAsteriskFailsIfEntityDoesNotYetExist() {
        final ThingId thingId = ThingId.of(idGenerator().withPrefixedRandomName(getClass().getName()));

        putThingMatcher(thingId, ConditionalHeadersTestHelper.VALUE_STR_1)
                .withHeader(ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.ASTERISK)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, (String) null)
                .expectingBody(containsString(ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY))
                .fire();

        getThingMatcher(thingId).expectingHttpStatus(HttpStatus.NOT_FOUND).fire();
    }

    @Test
    public void putWithIfMatchAsteriskSucceedsIfEntityAlreadyExists() {
        final ThingId thingId = createThingWithGeneratedId();

        putThingMatcher(thingId, ConditionalHeadersTestHelper.VALUE_STR_2)
                .withHeader(ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.ASTERISK)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_2)
                .fire();

        getThingMatcher(thingId).expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_2).fire();
    }

    @Test
    public void putWithIfNoneMatchSucceedsIfETagDoesNotMatch() {
        final ThingId thingId = createThingWithGeneratedId();

        putThingMatcher(thingId, ConditionalHeadersTestHelper.VALUE_STR_2)
                .withHeader(ConditionalHeadersTestHelper.IF_NONE_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_2)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_2)
                .fire();

        getThingMatcher(thingId).expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_2).fire();
    }

    @Test
    public void putWithIfNoneMatchFailsIfETagMatches() {
        final ThingId thingId = createThingWithGeneratedId();

        putThingMatcher(thingId, ConditionalHeadersTestHelper.VALUE_STR_2)
                .withHeader(ConditionalHeadersTestHelper.IF_NONE_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .expectingBody(containsString(ConditionalHeadersTestHelper.IF_NONE_MATCH_HEADER_KEY))
                .fire();

        getThingMatcher(thingId).expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1).fire();
    }

    @Test
    public void putWithIfNoneMatchAsteriskSucceedsIfEntityDoesNotYetExist() {
        final ThingId thingId = ThingId.of(idGenerator().withPrefixedRandomName(getClass().getName()));

        putThingMatcher(thingId, ConditionalHeadersTestHelper.VALUE_STR_1)
                .withHeader(ConditionalHeadersTestHelper.IF_NONE_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.ASTERISK)
                .expectingHttpStatus(HttpStatus.CREATED)
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .fire();

        getThingMatcher(thingId).expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1).fire();
    }

    @Test
    public void putWithIfNoneMatchAsteriskFailsIfEntityAlreadyExists() {
        final ThingId thingId = createThingWithGeneratedId();

        putThingMatcher(thingId, ConditionalHeadersTestHelper.VALUE_STR_2)
                .withHeader(ConditionalHeadersTestHelper.IF_NONE_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.ASTERISK)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .expectingBody(containsString(ConditionalHeadersTestHelper.IF_NONE_MATCH_HEADER_KEY))
                .fire();

        getThingMatcher(thingId).expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1).fire();
    }

    @Test
    public void deleteWithIfMatchSucceedsIfETagMatches() {
        final ThingId thingId = createThingWithGeneratedId();

        deleteThingMatcher(thingId)
                .withHeader(ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, (String) null)
                .fire();

        getThingMatcher(thingId).expectingHttpStatus(HttpStatus.NOT_FOUND).fire();
    }

    @Test
    public void deleteWithIfMatchFailsIfETagDoesNotMatch() {
        final ThingId thingId = createThingWithGeneratedId();

        deleteThingMatcher(thingId)
                .withHeader(ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_2)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .expectingBody(containsString(ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY))
                .fire();

        getThingMatcher(thingId).expectingHttpStatus(HttpStatus.OK).fire();
    }

    private void assertGetProvidesExpectedETag(final CharSequence thingId, final String expectedETagValue) {
        getThingMatcher(thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, expectedETagValue)
                .fire();
    }

    private GetMatcher getThingMatcher(final CharSequence thingId) {
        return getThing(VERSION_INT, thingId);
    }

    private static ThingId createThingWithGeneratedId() {
        final JsonObject thingJson = thingBuilder(ConditionalHeadersTestHelper.VALUE_STR_1).build().toJson();

        return ThingId.of(parseIdFromLocation(postThing(VERSION_INT, thingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .expectingHeader(ConditionalHeadersTestHelper.E_TAG_HEADER_KEY, ConditionalHeadersTestHelper.E_TAG_1)
                .fire()
                .header("Location")));
    }

    private static PutMatcher putThingMatcher(final ThingId thingId, final String changedValue) {
        final Thing thing = thingBuilder(changedValue)
                .setId(thingId)
                .build();

        return putThing(VERSION_INT, thing, VERSION);
    }

    private static DeleteMatcher deleteThingMatcher(final CharSequence thingId) {
        return deleteThing(VERSION_INT, thingId);
    }

    private static ThingBuilder.FromCopy thingBuilder(final String attributeValue) {
        return ThingsModelFactory.newThingBuilder(THING_JSON_PRODUCER.getJsonForV2())
                .setAttribute(JsonPointer.of("attributeToChange"), JsonValue.of(attributeValue));
    }
}
