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

import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
import static org.hamcrest.core.IsEqual.equalTo;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.HttpParameter;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ResourcePathBuilder;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.UriEncoding;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration test for the Attributes REST API.
 */
public final class AttributesIT extends IntegrationTest {

    /**
     * Attribute key with lots of special chars. NOTE: it does not contain slash, because finding such attributes is not
     * supported currently.
     */
    private static final String SPECIAL_ATTR_KEY = "!\\\"#$%&'()*+,:;=?@[]{|} aZ0";
    private static final String INVALID_POINTER = "foo/ä-is-invalid/bar";

    @Test
    public void putGetAndDeleteSingleAttribute() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String attributeJson = "{\"bar\":\"baz\"}";

        putAttribute(TestConstants.API_V_2, thingId, "foo", attributeJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getAttribute(TestConstants.API_V_2, thingId, "foo")
                .expectingBody(containsOnlyJsonKey("bar"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteAttribute(TestConstants.API_V_2, thingId, "foo")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    @Category(Acceptance.class)
    public void putGetAndDeleteSingleNestedAttribute() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String attributeJson = "{\"alice\":\"bob\"}";

        putAttribute(TestConstants.API_V_2, thingId, "foo/bar/baz", attributeJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getAttribute(TestConstants.API_V_2, thingId, "foo/bar/baz")
                .expectingBody(containsOnlyJsonKey("alice"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteAttribute(TestConstants.API_V_2, thingId, "foo/bar/baz")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getAttribute(TestConstants.API_V_2, thingId, "foo/bar")
                .expectingBody(containsEmptyJson())
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void crudAttributeWithSpecialChars() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonObject attributesJson = JsonFactory.newObjectBuilder()
                .set(SPECIAL_ATTR_KEY, "baz")
                .build();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setAttributes(attributesJson)
                .build();

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final String encodedAttrKey = UriEncoding.encodePathSegment(SPECIAL_ATTR_KEY);

        getAttribute(TestConstants.API_V_2, thingId, encodedAttrKey)
                .expectingBody(equalTo("\"baz\""))
                .expectingHttpStatus(HttpStatus.OK)
                .disableUrlEncoding()
                .fire();

        putAttribute(TestConstants.API_V_2, thingId, encodedAttrKey, "\"bar\"")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .disableUrlEncoding()
                .fire();

        getAttribute(TestConstants.API_V_2, thingId, encodedAttrKey)
                .expectingBody(equalTo("\"bar\""))
                .expectingHttpStatus(HttpStatus.OK)
                .disableUrlEncoding()
                .fire();

        deleteAttribute(TestConstants.API_V_2, thingId, encodedAttrKey)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .disableUrlEncoding()
                .fire();
    }

    @Test
    public void crudNestedAttributeWithSpecialChars() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonObject attributesJson = JsonFactory.newObjectBuilder().
                set(SPECIAL_ATTR_KEY, JsonFactory.newObjectBuilder()
                        .set(SPECIAL_ATTR_KEY, "baz")
                        .build())
                .build();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setAttributes(attributesJson)
                .build();

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final String encodedAttrPath =
                UriEncoding.encodePathSegment(SPECIAL_ATTR_KEY) + "/" + UriEncoding.encodePathSegment(SPECIAL_ATTR_KEY);

        getAttribute(TestConstants.API_V_2, thingId, encodedAttrPath)
                .expectingBody(equalTo("\"baz\""))
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        putAttribute(TestConstants.API_V_2, thingId, encodedAttrPath, "\"bar\"")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .disableUrlEncoding()
                .fire();

        getAttribute(TestConstants.API_V_2, thingId, encodedAttrPath)
                .expectingBody(equalTo("\"bar\""))
                .expectingHttpStatus(HttpStatus.OK)
                .disableUrlEncoding()
                .fire();

        deleteAttribute(TestConstants.API_V_2, thingId, encodedAttrPath)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .disableUrlEncoding()
                .fire();
    }

    @Test
    public void putAndGetSingleAttributeTwice() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String attributeJson = "\"bar\"";

        putAttribute(TestConstants.API_V_2, thingId, "foo", attributeJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getAttribute(TestConstants.API_V_2, thingId, "foo")
                .expectingBody(equalTo("\"bar\""))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final String attributeJsonUpdated = "\"baz\"";

        putAttribute(TestConstants.API_V_2, thingId, "foo", attributeJsonUpdated)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getAttribute(TestConstants.API_V_2, thingId, "foo")
                .expectingBody(equalTo("\"baz\""))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putAndGetSingleNestedAttributeTwice() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String attributeJson = "{\"bar\":{\"baz\":42}}";

        putAttribute(TestConstants.API_V_2, thingId, "foo", attributeJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getAttribute(TestConstants.API_V_2, thingId, "foo")
                .expectingBody(containsOnlyJsonKey("bar", "baz"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final String attributeJsonToUpdate = "{\"baz\":23}";

        putAttribute(TestConstants.API_V_2, thingId, "foo/bar", attributeJsonToUpdate)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getAttribute(TestConstants.API_V_2, thingId, "foo")
                .expectingBody(containsOnlyJsonKey("bar", "baz"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void overrideAnExistingAttributePropertyWithNull() {

        final JsonObject jsonThing = Thing.newBuilder()
                .setAttributes(Attributes.newBuilder()
                        .set("foo", "bar")
                        .build())
                .build()
                .toJson();

        final String location = postThing(TestConstants.API_V_2, jsonThing)
                .expectingHttpStatus(CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        putAttribute(TestConstants.API_V_2, thingId, "foo", "null")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getAttribute(TestConstants.API_V_2, thingId, "foo")
                .expectingBody(contains(JsonFactory.nullLiteral()))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void getAttributesOfUnknownThing() {
        getAttributes(TestConstants.API_V_2, idGenerator().withName("unknown"))
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void getSingleAttributeOfUnknownThing() {
        getAttribute(TestConstants.API_V_2, idGenerator().withName("unknown"), "foo")
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void getUnknownSingleAttribute() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        getAttribute(TestConstants.API_V_2, thingId, "foo")
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void putSingleAttributeWithInvalidJson() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String invalidJson = "\"bar\":\"baz\"";

        putAttribute(TestConstants.API_V_2, thingId, "foo", invalidJson)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    @Test
    public void crudAttributeWithInvalidPath() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        putAttribute(TestConstants.API_V_2, thingId, INVALID_POINTER, "42")
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();

        putAttributes(TestConstants.API_V_2, thingId,
                JsonFactory.newObjectBuilder().set("valid", JsonObject.newBuilder().set("not/valid",
                        JsonValue.of(true)).build()).build().toString())
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();

        getAttribute(TestConstants.API_V_2, thingId, INVALID_POINTER)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();

        deleteAttribute(TestConstants.API_V_2, thingId, INVALID_POINTER)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    @Test
    public void putGetAndDeleteAllAttributesAtOnce() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String attributesJsonString = "{\"foo\":\"bar\",\"baz\":42}";

        deleteAttributes(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getAttributes(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        putAttributes(TestConstants.API_V_2, thingId, attributesJsonString)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getAttributes(TestConstants.API_V_2, thingId)
                .expectingBody(equalTo(attributesJsonString))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        putAttributes(TestConstants.API_V_2, thingId, attributesJsonString)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void getAttributesWithFieldSelectors() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String attributeJsonSimple = "42";
        final String attributeJsonComplex = "{\"foo\":{\"bar\":\"baz\"}}";

        putAttribute(TestConstants.API_V_2, thingId, "simple", attributeJsonSimple)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        putAttribute(TestConstants.API_V_2, thingId, "complex", attributeJsonComplex)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .withParam(HttpParameter.FIELDS, "attributes(simple)")
                .expectingBody(containsOnlyJsonKey("attributes", "simple"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .withParam(HttpParameter.FIELDS, "attributes(complex)")
                .expectingBody(containsOnlyJsonKey("attributes", "complex", "foo", "bar"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .withParam(HttpParameter.FIELDS, "attributes(simple,complex)")
                .expectingBody(containsOnlyJsonKey("attributes", "simple", "complex", "foo", "bar"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void getAttributesWithSpecialCharFieldSelectors() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        /*
         * Note that the attribute key does not contain the following chars due
         * to the missing escaping functionality of field selector syntax: ()/,
         */
        final String specialAttributeKey = "!\\\"#$%&'*+:;=?@[]{|} aZ0";
        final String attributeValue = "42";

        final String pathEncodedAttributeKey = UriEncoding.encodePathSegment(specialAttributeKey);
        putAttribute(TestConstants.API_V_2, thingId, pathEncodedAttributeKey, attributeValue)
                .expectingHttpStatus(HttpStatus.CREATED)
                .disableUrlEncoding()
                .fire();

        // we have to support the "correct" way of path encoding according to RFC 3986
        final String rfc3986QueryEncodedAttributeKey = UriEncoding.encodeQueryParam(specialAttributeKey,
                UriEncoding.EncodingType.RFC3986);
        getThing(TestConstants.API_V_2, thingId)
                .withParam(HttpParameter.FIELDS, "attributes(" + rfc3986QueryEncodedAttributeKey + ")")
                .expectingBody(containsOnlyJsonKey("attributes", specialAttributeKey))
                .expectingHttpStatus(HttpStatus.OK)
                .disableUrlEncoding()
                .fire();

        // we also have to support the "wrong" way of form url encoding
        final String formUrlEncodedAttributeKey = UriEncoding.encodeQueryParam(specialAttributeKey,
                UriEncoding.EncodingType.FORM_URL_ENCODED);
        getThing(TestConstants.API_V_2, thingId)
                .withParam(HttpParameter.FIELDS, "attributes(" + formUrlEncodedAttributeKey + ")")
                .expectingBody(containsOnlyJsonKey("attributes", specialAttributeKey))
                .expectingHttpStatus(HttpStatus.OK)
                .disableUrlEncoding()
                .fire();
    }

    @Test
    public void tryToAccessApiWithDoubleSlashes() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String path = ResourcePathBuilder.forThing(thingId).attributes().toString();
        final String jsonString = "{}";
        final String attributesUrlWithDoubleSlash = dittoUrl(TestConstants.API_V_2, path) + "//";
        final String attributesUrlWithDoubleSlashAndId =
                attributesUrlWithDoubleSlash + "bar";

        // no double slash merging -> tries to post against empty attribute id
        post(attributesUrlWithDoubleSlash, jsonString)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.METHOD_NOT_ALLOWED)
                .fire();
        // no double slash merging -> tries to get double slashed attribute
        get(attributesUrlWithDoubleSlash)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(containsCharSequence("Consecutive slashes in JSON pointers are not supported"))
                .fire();

        // no double slash merging -> tries to put double slashed attribute
        put(attributesUrlWithDoubleSlashAndId, jsonString)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(containsCharSequence("Consecutive slashes in JSON pointers are not supported"))
                .fire();
        // no double slash merging -> tries to get double slashed attribute
        get(attributesUrlWithDoubleSlashAndId)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(containsCharSequence("Consecutive slashes in JSON pointers are not supported"))
                .fire();
        // no double slash merging -> tries to delete double slashed attribute
        delete(attributesUrlWithDoubleSlashAndId)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(containsCharSequence("Consecutive slashes in JSON pointers are not supported"))
                .fire();
    }

    @Test
    public void tryToUseDoubleSlashesInAttributeKey() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String propertyKey = "sla//shes";
        final String path = ResourcePathBuilder.forThing(thingId).attribute(propertyKey).toString();
        final String jsonString = "13";

        put(dittoUrl(TestConstants.API_V_2, path), jsonString)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(containsCharSequence("Consecutive slashes in JSON pointers are not supported"))
                .fire();
    }
}
