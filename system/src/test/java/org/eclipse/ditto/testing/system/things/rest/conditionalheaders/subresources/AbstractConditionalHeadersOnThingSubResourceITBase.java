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
package org.eclipse.ditto.testing.system.things.rest.conditionalheaders.subresources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.testing.system.things.rest.conditionalheaders.ConditionalHeadersTestHelper.ASTERISK;
import static org.eclipse.ditto.testing.system.things.rest.conditionalheaders.ConditionalHeadersTestHelper.E_TAG_HEADER_KEY;
import static org.eclipse.ditto.testing.system.things.rest.conditionalheaders.ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY;
import static org.eclipse.ditto.testing.system.things.rest.conditionalheaders.ConditionalHeadersTestHelper.IF_NONE_MATCH_HEADER_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.HttpVerbMatcher;
import org.eclipse.ditto.testing.common.matcher.PatchMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.Test;

public abstract class AbstractConditionalHeadersOnThingSubResourceITBase extends IntegrationTest {

    private static final String NON_MATCH_E_TAG = "\"never-match-etag-value-32bbbb57-73ae-4151-82e2-a6af828005d8\"";

    protected static final JsonPointer ATTRIBUTES_JSON_POINTER = JsonPointer.of("attributes");
    protected static final JsonPointer FEATURES_JSON_POINTER = JsonPointer.of("features");
    protected static final JsonPointer PROPERTIES_JSON_POINTER = JsonPointer.of("properties");
    protected static final JsonPointer DEFINITION_JSON_POINTER = JsonPointer.of("definition");
    protected static final JsonPointer POLICY_ID_JSON_POINTER = JsonPointer.of("policyId");

    protected final JsonSchemaVersion version;

    protected abstract boolean alwaysExists();

    protected abstract PutMatcher createSubResourceMatcher(final CharSequence thingId);

    protected abstract GetMatcher getSubResourceMatcher(final CharSequence thingId);

    protected abstract PutMatcher overwriteSubResourceMatcher(final CharSequence thingId);

    protected abstract PatchMatcher patchSubResourceMatcher(final CharSequence thingId);

    protected abstract DeleteMatcher deleteSubResourceMatcher(final CharSequence thingId);

    AbstractConditionalHeadersOnThingSubResourceITBase(final JsonSchemaVersion version) {
        this.version = version;
    }

    @Test
    public void successfulRequestReturnsExpectedETag() {
        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        assertGetProvidesExpectedETag(thingId, eTag1);

        final String eTag2 = overwriteSubResourceMatcher(thingId)
                .expectingBody(isEmptyOrNullString())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag2).isNotNull().isNotEqualTo(eTag1);

        assertGetProvidesExpectedETag(thingId, eTag2);
    }

    @Test
    public void getWithIfMatchReturnsSubEntityIfETagMatches() {
        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        getSubResourceMatcher(thingId)
                .withHeader(IF_MATCH_HEADER_KEY, eTag1)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(not(isEmptyOrNullString()))
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .fire();
    }

    @Test
    public void getWithIfMatchReturnsPreconditionFailedIfETagDoesNotMatch() {
        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        getSubResourceMatcher(thingId)
                .withHeader(IF_MATCH_HEADER_KEY, NON_MATCH_E_TAG)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingBody(not(isEmptyOrNullString()))
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .fire();
    }

    @Test
    public void getWithIfMatchAsteriskReturns404IfSubEntityDoesNotYetExist() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final ThingId thingId = ThingId.of(idGenerator().withPrefixedRandomName(getClass().getName()));
        createEntity(thingId);

        getSubResourceMatcher(thingId)
                .withHeader(IF_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void getWithIfMatchAsteriskReturnsSubEntityIfEntityAlreadyExists() {
        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        getSubResourceMatcher(thingId)
                .withHeader(IF_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(not(isEmptyOrNullString()))
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchReturnsSubEntityIfETagDoesNotMatch() {
        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        getSubResourceMatcher(thingId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, NON_MATCH_E_TAG)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(not(isEmptyOrNullString()))
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchReturnsNotModifiedIfETagMatches() {
        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        getSubResourceMatcher(thingId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, eTag1)
                .expectingHttpStatus(HttpStatus.NOT_MODIFIED)
                .expectingBody(isEmptyString())
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchAsteriskReturns404IfEntityDoesNotYetExist() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final ThingId thingId = ThingId.of(idGenerator().withPrefixedRandomName(getClass().getName()));
        createEntity(thingId);

        getSubResourceMatcher(thingId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchAsteriskReturnsNotModifiedIfEntityAlreadyExists() {
        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        getSubResourceMatcher(thingId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.NOT_MODIFIED)
                .expectingBody(isEmptyString())
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .fire();
    }

    @Test
    public void putWithIfMatchSucceedsIfETagMatches() {
        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        final String eTag2 = overwriteSubResourceMatcher(thingId)
                .withHeader(IF_MATCH_HEADER_KEY, eTag1)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag2).isNotNull().isNotEqualTo(eTag1);

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag2).fire();
    }

    @Test
    public void patchWithIfMatchSucceedsIfETagMatches() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        final String eTag2 = patchSubResourceMatcher(thingId)
                .withHeader(IF_MATCH_HEADER_KEY, eTag1)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag2).isNotNull().isNotEqualTo(eTag1);

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag2).fire();
    }

    @Test
    public void putWithIfMatchFailsIfETagDoesNotMatch() {
        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        overwriteSubResourceMatcher(thingId)
                .withHeader(IF_MATCH_HEADER_KEY, NON_MATCH_E_TAG)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .expectingBody(containsString(IF_MATCH_HEADER_KEY))
                .fire();

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag1).fire();
    }

    @Test
    public void patchWithIfMatchFailsIfETagDoesNotMatch() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        patchSubResourceMatcher(thingId)
                .withHeader(IF_MATCH_HEADER_KEY, NON_MATCH_E_TAG)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .expectingBody(containsString(IF_MATCH_HEADER_KEY))
                .fire();

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag1).fire();
    }

    @Test
    public void putWithIfMatchAsteriskFailsIfSubEntityDoesNotYetExist() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final ThingId thingId = ThingId.of(generateRandomEntityId());
        createEntity(thingId);

        overwriteSubResourceMatcher(thingId)
                .withHeader(IF_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, (String) null)
                .expectingBody(containsString(IF_MATCH_HEADER_KEY))
                .fire();

        getSubResourceMatcher(thingId).expectingHttpStatus(HttpStatus.NOT_FOUND).fire();
    }

    @Test
    public void patchWithIfMatchAsteriskFailsIfSubEntityDoesNotYetExist() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final ThingId thingId = ThingId.of(generateRandomEntityId());
        createEntity(thingId);

        patchSubResourceMatcher(thingId)
                .withHeader(IF_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, (String) null)
                .expectingBody(containsString(IF_MATCH_HEADER_KEY))
                .fire();

        getSubResourceMatcher(thingId).expectingHttpStatus(HttpStatus.NOT_FOUND).fire();
    }

    @Test
    public void putWithIfMatchAsteriskSucceedsIfSubEntityAlreadyExists() {
        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        final String eTag2 = overwriteSubResourceMatcher(thingId)
                .withHeader(IF_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag2).isNotNull().isNotEqualTo(eTag1);

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag2).fire();
    }

    @Test
    public void patchWithIfMatchAsteriskSucceedsIfSubEntityAlreadyExists() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        final String eTag2 = patchSubResourceMatcher(thingId)
                .withHeader(IF_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag2).isNotNull().isNotEqualTo(eTag1);

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag2).fire();
    }

    @Test
    public void putWithIfNoneMatchSucceedsIfETagDoesNotMatch() {
        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        final String eTag2 = overwriteSubResourceMatcher(thingId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, NON_MATCH_E_TAG)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag2).isNotNull().isNotEqualTo(eTag1);

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag2).fire();
    }

    @Test
    public void patchWithIfNoneMatchSucceedsIfETagDoesNotMatch() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        final String eTag2 = patchSubResourceMatcher(thingId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, NON_MATCH_E_TAG)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag2).isNotNull().isNotEqualTo(eTag1);

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag2).fire();
    }

    @Test
    public void putWithIfNoneMatchFailsIfETagMatches() {
        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        overwriteSubResourceMatcher(thingId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, eTag1)
                .expectingBody(containsString(IF_NONE_MATCH_HEADER_KEY))
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .fire();

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag1).fire();
    }

    @Test
    public void patchWithIfNoneMatchFailsIfETagMatches() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        patchSubResourceMatcher(thingId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, eTag1)
                .expectingBody(containsString(IF_NONE_MATCH_HEADER_KEY))
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .fire();

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag1).fire();
    }

    @Test
    public void putWithIfNoneMatchAsteriskSucceedsIfSubEntityDoesNotYetExist() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final ThingId thingId = ThingId.of(generateRandomEntityId());
        createEntity(thingId);

        final String eTag1 = createSubResourceMatcher(thingId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag1).isNotNull();

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag1).fire();
    }

    @Test
    public void patchWithIfNoneMatchAsteriskSucceedsIfSubEntityDoesNotYetExist() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final ThingId thingId = ThingId.of(generateRandomEntityId());
        createEntity(thingId);

        final String eTag1 = patchSubResourceMatcher(thingId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag1).isNotNull();

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag1).fire();
    }

    @Test
    public void putWithIfNoneMatchAsteriskFailsIfSubEntityAlreadyExists() {
        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        overwriteSubResourceMatcher(thingId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .expectingBody(containsString(IF_NONE_MATCH_HEADER_KEY))
                .fire();

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag1).fire();
    }

    @Test
    public void patchWithIfNoneMatchAsteriskFailsIfSubEntityAlreadyExists() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        patchSubResourceMatcher(thingId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .expectingBody(containsString(IF_NONE_MATCH_HEADER_KEY))
                .fire();

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag1).fire();
    }

    @Test
    public void deleteWithIfMatchSucceedsIfETagMatches() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        deleteSubResourceMatcher(thingId)
                .withHeader(IF_MATCH_HEADER_KEY, eTag1)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .expectingHeader(E_TAG_HEADER_KEY, (String) null)
                .fire();

        getSubResourceMatcher(thingId).expectingHttpStatus(HttpStatus.NOT_FOUND).fire();
    }

    @Test
    public void deleteWithIfMatchFailsIfETagDoesNotMatch() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final ThingId thingId = ThingId.of(generateRandomEntityId());
        final String eTag1 = createEntityWithSubResource(thingId);

        deleteSubResourceMatcher(thingId)
                .withHeader(IF_MATCH_HEADER_KEY, NON_MATCH_E_TAG)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .expectingBody(containsString(IF_MATCH_HEADER_KEY))
                .fire();

        getSubResourceMatcher(thingId).expectingHeader(E_TAG_HEADER_KEY, eTag1).fire();
    }

    private String generateRandomEntityId() {
        return idGenerator().withPrefixedRandomName(getClass().getName());
    }

    private String createOrGetSubResource(final CharSequence thingId) {
        final HttpVerbMatcher eTagMatcher = alwaysExists() ?
                getSubResourceMatcher(thingId).expectingHttpStatus(HttpStatus.OK) :
                createSubResourceMatcher(thingId).expectingHttpStatus(HttpStatus.CREATED);

        final String eTag = eTagMatcher
                .fire()
                .header(E_TAG_HEADER_KEY);

        assertThat(eTag).isNotNull();
        return eTag;
    }

    private void assertGetProvidesExpectedETag(final CharSequence thingId, final String expectedETagValue) {
        getSubResourceMatcher(thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingHeader(E_TAG_HEADER_KEY, expectedETagValue)
                .fire();
    }

    private String createEntityWithSubResource(final ThingId thingId) {
        createEntity(thingId);

        return createOrGetSubResource(thingId);
    }

    private void createEntity(final ThingId thingId) {
        putThingMatcher(thingId)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
    }

    private PutMatcher putThingMatcher(final ThingId thingId) {
        final Thing thing = thingBuilder()
                .setId(thingId)
                .build();

        return putThing(version.toInt(), thing, version);
    }

    private static ThingBuilder.FromScratch thingBuilder() {
        return ThingsModelFactory.newThingBuilder();
    }
}
