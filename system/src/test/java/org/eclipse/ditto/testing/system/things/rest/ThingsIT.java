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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.FieldType;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonFieldDefinition;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.api.Permission;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.HttpParameter;
import org.eclipse.ditto.testing.common.HttpResource;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.client.BasicAuth;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests against things.
 */
public final class ThingsIT extends IntegrationTest {

    private static final int RETRY_TIMEOUT = 500;

    private static String[] getKeyNamesOf(final Collection<JsonFieldDefinition<?>> jsonKeys) {
        return jsonKeys.stream().map(fd -> fd.getPointer().toString()).toArray(String[]::new);
    }

    /**
     * Posts a Thing to the service, gets it afterwards and finally deletes it.
     */
    @Test
    @Category(Acceptance.class)
    public void postGetAndDeleteThing() {
        final ThingId thingId = ThingId.of(parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location")));

        getThing(TestConstants.API_V_2, thingId)
                .expectingBody(containsThingId(thingId))
                .fire();

        // also test with trailing slash
        getThing(TestConstants.API_V_2, thingId + "/")
                .expectingBody(containsThingId(thingId))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    @Category(Acceptance.class)
    public void postDeleteAndModifyThing() {
        final ThingId thingId = ThingId.of(parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location")));

        getThing(TestConstants.API_V_2, thingId)
                .expectingBody(containsThingId(thingId))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        deletePolicy(thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        putAttribute(TestConstants.API_V_2, thingId, "foo", "\"test\"")
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .expectingErrorCode("things:thing.notfound")
                .fire();
    }

    /**
     * Retrieves an unknown Thing.
     */
    @Test
    public void getUnknownThing() {
        getThing(TestConstants.API_V_2, serviceEnv.getDefaultNamespaceName() + ":unknownThingId")
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    /**
     * Retrieves unknown Things as a list.
     */
    @Test
    public void getUnknownThingsAsList() {
        getThings(TestConstants.API_V_2)
                .withParam(HttpParameter.IDS, "unknown:ThingId1", "unknown:ThingId2")
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsEmptyCollection())
                .fire();
    }

    @Test
    public void getThingsAsListWithEmptyIdsParameter() {
        getThings(TestConstants.API_V_2)
                .withParam(HttpParameter.IDS, "")
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    @Test
    public void getThingsAsListWithoutIdsParameter() {
        getThings(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    /**
     * Retrieves a persisted Thing with missing authorization to do so.
     */
    @Test
    public void getThingWithMissingAuthorization() {
        final String thingId = parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    /**
     * Posts a Thing to the service and retrieves it by applying field selectors.
     */
    @Test
    public void getThingSpecialFields() {
        final List<JsonFieldDefinition<?>> expectedJsonFieldDefinitions = List.of(Thing.JsonFields.ID,
                Thing.JsonFields.MODIFIED,
                Thing.JsonFields.LIFECYCLE,
                Thing.JsonFields.NAMESPACE,
                Thing.JsonFields.REVISION);

        final String thingId = parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        getThing(TestConstants.API_V_2, thingId)
                .withParam(HttpParameter.FIELDS, getKeyNamesOf(expectedJsonFieldDefinitions))
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsOnly(expectedJsonFieldDefinitions))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    /**
     * Posts a Thing to the service and retrieves it by applying field selectors.
     */
    @Test
    public void getThingWithFieldSelectors() {
        final var expectedJsonFieldDefinitions = List.of(Thing.JsonFields.ID, Thing.JsonFields.ATTRIBUTES);

        final var thingId = parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        getThing(TestConstants.API_V_2, thingId)
                .withParam(HttpParameter.FIELDS, getKeyNamesOf(expectedJsonFieldDefinitions))
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsOnly(expectedJsonFieldDefinitions))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    /**
     * Posts a Thing to the service and retrieves it by applying an empty list as selectors.
     */
    @Test
    public void getThingWithEmptyFieldSelectors() {
        final var thingId = parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        final List<JsonFieldDefinition<?>> expectedJsonFieldDefinitions = Collections.emptyList();

        getThing(TestConstants.API_V_2, thingId)
                .withParam(HttpParameter.FIELDS, getKeyNamesOf(expectedJsonFieldDefinitions))
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsOnly(expectedJsonFieldDefinitions))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void getThingsByIdWithFieldSelectors() {
        final List<JsonFieldDefinition<?>> expectedJsonFieldDefinitions = List.of(Thing.JsonFields.ID);

        final var thingId1 = ThingId.of(parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location")));

        final var thingId2 = ThingId.of(parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location")));

        final var thingId3 = ThingId.of(parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location")));

        getThings(TestConstants.API_V_2)
                .withParam(HttpParameter.IDS, thingId1, thingId2)
                .withParam(HttpParameter.FIELDS, getKeyNamesOf(expectedJsonFieldDefinitions))
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsThingIds(thingId1, thingId2), containsOnly(expectedJsonFieldDefinitions))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId1)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId2)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId3)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    /**
     * Posts a Thing to the service and retrieves it <em>without</em> applying field selectors. This test checks if the
     * fields of the JSON string are the expected default fields.
     */
    @Test
    public void getThingWithoutFieldSelectors() {
        final var expectedJsonKeys = List.of(Thing.JsonFields.ID,
                Thing.JsonFields.POLICY_ID,
                Thing.JsonFields.ATTRIBUTES,
                Thing.JsonFields.FEATURES);

        final var thingId = parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        getThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsOnly(expectedJsonKeys))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    /**
     * Posts a Thing in V2 to the service and retrieves it <em>without</em> applying field selectors. This test checks
     * if the fields of the JSON string are the expected default fields.
     */
    @Test
    public void getThingV2WithoutFieldSelectors() {
        final var expectedJsonKeys = List.of(Thing.JsonFields.ID,
                Thing.JsonFields.POLICY_ID,
                Thing.JsonFields.ATTRIBUTES,
                Thing.JsonFields.FEATURES);

        final var thingId = parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        getThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsOnly(expectedJsonKeys))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    /**
     * Posts a Thing in V2 to the service and retrieves it by applying field selectors.
     */
    @Test
    public void getThingV2WithFieldSelectors() {
        final var expectedJsonFieldDefinitions = List.of(Thing.JsonFields.ID, Thing.JsonFields.ATTRIBUTES);

        final var thingId = parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        getThing(TestConstants.API_V_2, thingId)
                .withParam(HttpParameter.FIELDS, getKeyNamesOf(expectedJsonFieldDefinitions))
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsOnly(expectedJsonFieldDefinitions))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    /**
     * Posts a Thing in V2 to the service and retrieves its policy by applying the according field selector.
     */
    @Test
    public void getThingV2WithPolicyFieldSelector() {
        final List<JsonFieldDefinition<?>> expectedJsonFieldDefinitions =
                List.of(JsonFactory.newJsonObjectFieldDefinition("_policy",
                        FieldType.SPECIAL,
                        FieldType.HIDDEN,
                        JsonSchemaVersion.V_2));

        final var thingId = parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        getThing(TestConstants.API_V_2, thingId)
                .withParam(HttpParameter.FIELDS, getKeyNamesOf(expectedJsonFieldDefinitions))
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsOnly(expectedJsonFieldDefinitions))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void postAndPutSameThing() {
        final ThingId thingId = ThingId.of(parseIdFromLocation(postThing(TestConstants.API_V_2, JsonFactory.newObject())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location")));

        final Thing thing = Thing.newBuilder().setId(thingId).build();

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void putThingWithInvalidId() {
        final String thingId = "invalidThingId";

        final JsonObject thing = JsonObject.newBuilder()
                .set(Thing.JsonFields.ID, thingId)
                .build();

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    @Test
    public void postThingAndReceiveLocation() {
        final String thingId = parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        Assertions.assertThat(thingId).isNotNull();
    }

    /**
     * Posts a Thing with an invalid JSON format.
     */
    @Test
    public void postThingWithInvalidJson() {
        final String invalidJsonString = "{ \"some\" invalid \"json\" }";

        postThing(TestConstants.API_V_2, invalidJsonString)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    /**
     * Posts two things to the service and retrieves them as a list.
     */
    @Test
    public void postTwoThingsAndRetrieveThemAsList() {
        final ThingId thingId1 = ThingId.of(parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location")));

        final ThingId thingId2 = ThingId.of(parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location")));

        getThings(TestConstants.API_V_2)
                .withParam(HttpParameter.IDS, thingId1, thingId2)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsThingIds(thingId1, thingId2))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId1)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId2)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    /**
     * Posts two hundred things to the service and retrieves them as a list.
     */
    @Test
    public void postTwoHundredThingsAndRetrieveThemAsList() {
        final List<ThingId> ids = new ArrayList<>();

        try {
            final int amountOfThings = 200;
            for (int i = 0; i < amountOfThings; i++) {
                final ThingId thingId = ThingId.of(idGenerator().withName("the_thing_" + i));
                final Thing thing = Thing.newBuilder().setId(thingId).build();
                putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                        .expectingHttpStatus(HttpStatus.CREATED)
                        .fire();
                ids.add(thingId);
            }

            final ThingId[] idsArray = ids.toArray(new ThingId[amountOfThings]);

            getThings(TestConstants.API_V_2)
                    .withParam(HttpParameter.IDS, idsArray)
                    .expectingHttpStatus(HttpStatus.OK)
                    .expectingBody(containsThingIds(ids.toArray(new ThingId[amountOfThings])))
                    .fire();
        } finally {
            ids.forEach(id -> deleteThing(TestConstants.API_V_2, id)
                    .expectingHttpStatus(HttpStatus.NO_CONTENT)
                    .fire());
        }
    }

    /**
     * Creates a thing with put, updates it with put and deletes it finally.
     */
    @Test
    public void putThing() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = Thing.newBuilder().setId(thingId).build();
        final String thingIdFromLocation =
                parseIdFromLocation(putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                        .expectingHttpStatus(HttpStatus.CREATED)
                        .fire()
                        .header("Location"));

        Assertions.assertThat(thingId.toString()).isEqualTo(thingIdFromLocation);

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsThingId(thingId))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    /**
     * Creates a Thing, deletes it and creates it again (before deleting it finally).
     */
    @Test
    public void recreateDeletedThing() {
        final ThingId thingId = ThingId.of(parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location")));

        getThing(TestConstants.API_V_2, thingId)
                .expectingBody(containsThingId(thingId))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        deletePolicy(thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        retryAfterTimeout(
                () -> putThing(TestConstants.API_V_2, ThingsModelFactory.newThingBuilder().setId(thingId).build(),
                        JsonSchemaVersion.V_2)
                        .fire(), HttpStatus.CREATED, RETRY_TIMEOUT);

        getThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsThingId(thingId))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void tryToDeleteUnknownThing() {
        deleteThing(TestConstants.API_V_2, serviceEnv.getDefaultNamespaceName() + ":unknownThingId")
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void putThingIsAllowedWhenThingAlreadyExists() {
        final BasicAuth basicAuth = serviceEnv.getDefaultTestingContext().getBasicAuth();
        final Subjects subjects;
        if (basicAuth.isEnabled()) {
            subjects = Subjects.newInstance(Subject.newInstance(
                    SubjectIssuer.newInstance("nginx"), basicAuth.getUsername()));
        } else {
            subjects = Subjects.newInstance(
                    serviceEnv.getDefaultTestingContext().getOAuthClient().getSubject(),
                    serviceEnv.getTestingContext2().getOAuthClient().getSubject());
        }
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = Thing.newBuilder().setId(thingId).build();

        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubjects(subjects)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), Permission.READ, Permission.WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), Permission.READ, Permission.WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), Permission.READ, Permission.WRITE)
                .build();

        putThingWithPolicy(TestConstants.API_V_2, thing, policy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        if (null == basicAuth || !basicAuth.isEnabled()) {
            // update the Thing with a different solution
            putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                    .expectingHttpStatus(HttpStatus.NO_CONTENT)
                    .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                    .fire();
        }
    }

    @Test
    public void postThingWithDefaultNamespace() {

        final ThingId thingId = ThingId.of(parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location")));

        getThing(TestConstants.API_V_2, thingId)
                .expectingBody(containsThingId(thingId))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void putTooLargeThing() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < DEFAULT_MAX_THING_SIZE; i++) {
            sb.append('a');
        }
        final JsonObject largeAttributes = JsonObject.newBuilder()
                .set("a", sb.toString())
                .build();

        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId)
                .setAttributes(largeAttributes)
                .build();

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.REQUEST_ENTITY_TOO_LARGE)
                .fire();

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.REQUEST_ENTITY_TOO_LARGE)
                .fire();
    }

    @Test
    public void createThingAndMakeItTooLargeViaFeature() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final StringBuilder sb = new StringBuilder();
        final int overhead = 150;
        for (int i = 0; i < DEFAULT_MAX_THING_SIZE - overhead; i++) {
            sb.append('a');
        }
        final JsonObject largeAttributes = JsonObject.newBuilder()
                .set("a", sb.toString())
                .build();

        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId)
                .setAttributes(largeAttributes)
                .build();

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final StringBuilder sb2 = new StringBuilder();
        for (int i = 0; i < overhead; i++) {
            sb2.append('b');
        }
        final JsonObject largeSubObj = JsonObject.newBuilder()
                .set("b", sb2.toString())
                .build();

        final JsonObject featureProperties = JsonObject.newBuilder()
                .set("properties", JsonObject.newBuilder()
                        .set("foo", 42)
                        .set("bar", "this is a test")
                        .set("b", largeSubObj)
                        .build()
                ).build();

        putFeature(TestConstants.API_V_2, thingId, "newFeature", featureProperties)
                .expectingHttpStatus(HttpStatus.REQUEST_ENTITY_TOO_LARGE)
                .fire();
    }

    /**
     * Post a Thing with too large header.
     */
    @Test
    public void postThingWithTooLargeHeader() {
        // Keep header size below 8k, default size of large_client_header_buffers
        // If a single header exceeds 8k in size, Nginx ingress controller rejects the request with status 502.
        final char[] chars = new char[6000];
        Arrays.fill(chars, 'x');
        final String largeString = new String(chars);

        postThing(TestConstants.API_V_2)
                .withHeader(HttpHeader.X_CORRELATION_ID, largeString)
                .expectingHttpStatus(HttpStatus.REQUEST_HEADER_FIELDS_TOO_LARGE)
                .fire();
    }

    @Test
    public void postThingWithSearchPersistedAndTwinPersistedWithDefaultNamespace() {
        postThing(TestConstants.API_V_2)
                .withHeader("requested-acks", "twin-persisted,search-persisted")
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void tryToAccessApiWithDoubleSlashes() {
        final String path = HttpResource.THINGS.getPath();
        final String jsonString = "{}";
        final String thingsServiceUrlWithDoubleSlash = dittoUrl(TestConstants.API_V_2, path) + "//";
        final String thingsServiceUrlWithDoubleSlashAndId =
                thingsServiceUrlWithDoubleSlash + ThingId.of(idGenerator().withRandomName());

        // no double slash merging -> tries to post against empty thing id
        post(thingsServiceUrlWithDoubleSlash, jsonString)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
        // no double slash merging -> tries to get empty thing id
        get(thingsServiceUrlWithDoubleSlash)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        // no double slash merging -> tries to put empty thing id
        put(thingsServiceUrlWithDoubleSlashAndId, jsonString)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
        // no double slash merging -> tries to get empty thing id
        get(thingsServiceUrlWithDoubleSlashAndId)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
        // no double slash merging -> tries to delete empty thing id
        delete(thingsServiceUrlWithDoubleSlashAndId)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void postThingAndVerifyResponseHeaderFiltering() {

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final String correlationId = "fixed-correlation-id";
        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId)
                .build();

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .withCorrelationId(correlationId)
                .withHeader("Referrer", "bumlux123")
                .withHeader("Set-Cookie", "example-cookie=test")
                .expectingHttpStatus(HttpStatus.CREATED)
                .expectingHeaderIsNotPresent("Referrer")
                .expectingHeaderIsNotPresent("Set-Cookie")
                .expectingHeader("correlation-id", correlationId)
                .expectingHeader("etag", "\"rev:1\"")
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

}
