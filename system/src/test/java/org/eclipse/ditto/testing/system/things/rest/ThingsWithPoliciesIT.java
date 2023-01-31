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

import static org.eclipse.ditto.base.model.common.HttpStatus.BAD_REQUEST;
import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
import static org.eclipse.ditto.base.model.common.HttpStatus.FORBIDDEN;
import static org.eclipse.ditto.base.model.common.HttpStatus.NOT_FOUND;
import static org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT;
import static org.eclipse.ditto.base.model.common.HttpStatus.OK;
import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;
import static org.eclipse.ditto.testing.common.TestConstants.Policy.ARBITRARY_SUBJECT_TYPE;

import java.time.Duration;

import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.FieldType;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonCollectors;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonKey;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonObjectBuilder;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.api.Permission;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.Permissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyBuilder;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.testing.common.SearchIntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.ThingsSubjectIssuer;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.restassured.response.Response;

/**
 * Integration Tests for implicit Policy management via /things (e.g. creation).
 * <p>
 * This extends SearchIntegrationTests as at least one test case requires to do a search for things.
 */
public final class ThingsWithPoliciesIT extends SearchIntegrationTest {

    /**
     * When creating a Thing without specifying a policy-id or inline-policy, for the first
     * authorized subject an Policy entry with the minimum required permissions is created.
     */
    @Test
    @Category(Acceptance.class)
    public void createThingWithImplicitPolicyWithMinimumRequiredPermissions() {
        final Response response = postThing(TestConstants.API_V_2, JsonFactory.newObject())
                .expectingHttpStatus(CREATED)
                .fire();

        final String thingId = parseIdFromLocation(response.header("Location"));
        final PolicyId policyId = PolicyId.of(thingId);

        final JsonObject expectedPolicyJson = createPolicyJson(policyId);

        getPolicy(policyId)
                .expectingHttpStatus(OK)
                .expectingBody(contains(expectedPolicyJson))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        deletePolicy(policyId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    /**
     * Posting an incomplete Inline-Policy is a bad request(400).
     */
    @Test
    public void postThingWhereNoPolicyEntryHasMinimumRequiredPermissions() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName(
                "postThingWhereNoPolicyEntryHasMinimumRequiredPermissions"));
        final Policy policyToPost =
                PoliciesModelFactory.newPolicyBuilder(policyId)
                        .forLabel("READ")
                        .setSubject(ThingsSubjectIssuer.DITTO, "sid_read", ARBITRARY_SUBJECT_TYPE)
                        .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ)
                        .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                        .forLabel("WRITE")
                        .setSubject(ThingsSubjectIssuer.DITTO, "sid_write", ARBITRARY_SUBJECT_TYPE)
                        .setGrantedPermissions(PoliciesResourceType.thingResource("/"), WRITE)
                        .build();

        final JsonObjectBuilder jsonObjectBuilder = JsonFactory.newObjectBuilder();
        jsonObjectBuilder.setAll(policyToPost.toInlinedJson(JsonSchemaVersion.V_2, FieldType.notHidden()));
        final JsonObject thingJsonToPost = jsonObjectBuilder.build();

        postThing(TestConstants.API_V_2, thingJsonToPost)
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    @Test
    public void postThingWhereOnePolicyEntryHasMinimumRequiredPermissions() {
        final PolicyId policyId = PolicyId.of(idGenerator()
                .withPrefixedRandomName("postThingWhereOnePolicyEntryHasMinimumRequiredPermissions"));
        final Policy policyToPost =
                PoliciesModelFactory.newPolicyBuilder(policyId)
                        .forLabel("READ")
                        .setSubject(ThingsSubjectIssuer.DITTO, "sid_read", ARBITRARY_SUBJECT_TYPE)
                        .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ)
                        .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                        .forLabel("WRITE")
                        .setSubject(ThingsSubjectIssuer.DITTO, "sid_write", ARBITRARY_SUBJECT_TYPE)
                        .setGrantedPermissions(PoliciesResourceType.policyResource("/"), WRITE)
                        .setGrantedPermissions(PoliciesResourceType.thingResource("/"), WRITE)
                        .forLabel("ALL")
                        .setSubject(ThingsSubjectIssuer.DITTO,
                                serviceEnv.getDefaultTestingContext().getOAuthClient().getClientId(),
                                ARBITRARY_SUBJECT_TYPE)
                        .setGrantedPermissions(PoliciesResourceType.policyResource("/"), WRITE, READ)
                        .setGrantedPermissions(PoliciesResourceType.thingResource("/"), WRITE, READ)
                        .build();

        final JsonObjectBuilder jsonObjectBuilder = JsonFactory.newObjectBuilder();
        jsonObjectBuilder.set(Thing.JsonFields.POLICY_ID, policyId.toString());
        jsonObjectBuilder.setAll(
                policyToPost.toInlinedJson(JsonSchemaVersion.V_2,
                        JsonFactory.newFieldSelector(Policy.JsonFields.ID, Policy.JsonFields.ENTRIES)));
        final JsonObject thingJsonToPost = jsonObjectBuilder.build();

        postThing(TestConstants.API_V_2, thingJsonToPost)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
    }

    @Test
    public void putGetAndDeleteCompletePolicyImplicitlyViaThing() {
        final Response response = postThing(TestConstants.API_V_2, JsonFactory.newObject())
                .expectingHttpStatus(CREATED)
                .fire();

        final String thingId = parseIdFromLocation(response.header("Location"));
        final PolicyId policyId = PolicyId.of(thingId);

        final Policy policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ALL")
                .setSubject(ThingsSubjectIssuer.DITTO, "sid_all", ARBITRARY_SUBJECT_TYPE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), WRITE, READ)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), WRITE, READ)
                .build();

        putPolicy(policyId, policy).expectingHttpStatus(NO_CONTENT).fire();

        // The Thing now should not be visible to the user anymore.
        awaitUntil(() -> getPolicy(policyId)
                .expectingHttpStatus(NOT_FOUND)
                .fire());
    }

    @Test
    public void putCompletePolicyWithInsufficientPermissionsImplicitlyViaThing() {
        final Response response = postThing(TestConstants.API_V_2, JsonFactory.newObject())
                .expectingHttpStatus(CREATED)
                .fire();

        final String thingId = parseIdFromLocation(response.header("Location"));
        final PolicyId policyId = PolicyId.of(thingId);

        final Policy policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("READ")
                .setSubject(ThingsSubjectIssuer.DITTO, "sid_read", ARBITRARY_SUBJECT_TYPE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                .build();

        putPolicy(policyId, policy)
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void createPolicyAndTryToCreateThingWithSameIdUsingImplicitPolicy() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName(
                "createPolicyAndTryToCreateThingWithSameIdUsingImplicitPolicy"));
        final Policy policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("FOO_AND_BAR")
                .setSubject(ThingsSubjectIssuer.DITTO,
                        serviceEnv.getDefaultTestingContext().getOAuthClient().getClientId(),
                        ARBITRARY_SUBJECT_TYPE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();

        putPolicy(policyId, policy)
                .expectingHttpStatus(CREATED)
                .fire();

        getPolicy(policyId)
                .expectingHttpStatus(OK)
                .fire();

        final ThingId thingId = ThingId.of(policyId);
        // creating the Thing with the same ID as the Policy ID shall not be allowed
        final Thing thing = Thing.newBuilder().setId(thingId).build();
        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(BAD_REQUEST)
                .fire();

        // now delete Policy with that ID again
        deletePolicy(policyId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getPolicy(policyId)
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // Creating the Thing should now be allowed - an implicit Policy with that ID is created
        // allow a delay before creation for policy deletion to propagate through the distributed cache
        final Response thingResponse = putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .useAwaitility(Awaitility.await())
                .expectingHttpStatus(CREATED)
                .fire();

        final String thingIdFromResponse = parseIdFromLocation(thingResponse.header("Location"));

        final JsonObject expectedPolicyJson = createPolicyJson(PolicyId.of(thingIdFromResponse));

        // now the policy is found again
        getPolicy(thingIdFromResponse)
                .expectingHttpStatus(OK)
                .expectingBody(contains(expectedPolicyJson))
                .fire();

        deleteThing(TestConstants.API_V_2, thingIdFromResponse)
                .useAwaitility(Awaitility.await())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        deletePolicy(thingIdFromResponse)
                .useAwaitility(Awaitility.await())
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }
    
    @Test
    public void createPolicyAndThingWithSameInlinedPolicyId() {
        final PolicyId policyId = PolicyId.of(
                idGenerator().withPrefixedRandomName("createPolicyAndThingWithSameInlinedPolicyId_policy-"));
        final Policy policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("FOO_AND_BAR")
                .setSubject(ThingsSubjectIssuer.DITTO,
                        serviceEnv.getDefaultTestingContext().getOAuthClient().getClientId(),
                        ARBITRARY_SUBJECT_TYPE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();

        putPolicy(policyId, policy)
                .expectingHttpStatus(CREATED)
                .fire();

        getPolicy(policyId)
                .expectingHttpStatus(OK)
                .fire();

        // creating another Thing with an inline Policy with the same ID should not work
        final ThingId thingId =
                ThingId.of(idGenerator().withPrefixedRandomName("createPolicyAndThingWithSameInlinedPolicyId_thing"));
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .build();
        putThingWithPolicy(TestConstants.API_V_2, thing, policy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(BAD_REQUEST)
                .fire();

        // now delete Policy with that ID again
        deletePolicy(policyId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getPolicy(policyId)
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // Creating the Thing should now be allowed - the inlined Policy with that ID is created
        putThingWithPolicy(TestConstants.API_V_2, thing, policy, JsonSchemaVersion.V_2)
                .useAwaitility(AWAITILITY_DEFAULT_CONFIG)
                .expectingHttpStatus(CREATED)
                .fire();

        // now the policy is found again
        getPolicy(policyId)
                .useAwaitility(AWAITILITY_DEFAULT_CONFIG)
                .expectingHttpStatus(OK)
                .expectingBody(contains(policy.toJson()))
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .useAwaitility(AWAITILITY_DEFAULT_CONFIG)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        deletePolicy(policyId)
                .useAwaitility(AWAITILITY_DEFAULT_CONFIG)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }
    
    @Test
    @Category(Acceptance.class)
    public void createPolicyAndThingWithReferenceToThatPolicy() {
        final PolicyId policyId =
                PolicyId.of(
                        idGenerator().withPrefixedRandomName("createPolicyAndThingWithReferenceToThatPolicy_policy"));
        final Policy policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("FOO_AND_BAR")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();

        final Response policyResponse = putPolicy(policyId, policy)
                .expectingHttpStatus(CREATED)
                .fire();

        final String policyIdFromResponse = parseIdFromLocation(policyResponse.header("Location"));

        getPolicy(policyIdFromResponse)
                .expectingHttpStatus(OK)
                .fire();

        // creating the Thing with a reference to the just created Policy ID
        final ThingId thingId =
                ThingId.of(idGenerator().withPrefixedRandomName("createPolicyAndThingWithReferenceToThatPolicy_thing"));
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(PolicyId.of(policyIdFromResponse))
                .build();
        final Response thingResponse = putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        final String thingIdFromResponse = parseIdFromLocation(thingResponse.header("Location"));

        final JsonObject expectedPolicyJson = JsonObject.newBuilder().set("_policy", policy.toJson()).build();

        // get the policy via the Thing (field selector "_policy") to see if that is the same as the created one
        getThingPolicy(thingIdFromResponse)
                .expectingHttpStatus(OK)
                .expectingBody(contains(expectedPolicyJson))
                .fire();

        // now delete the Policy
        deletePolicy(policyIdFromResponse)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // ensure that it was deleted
        getPolicy(policyIdFromResponse)
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // ensure that accessing the Thing also no longer works
        getThing(TestConstants.API_V_2, policyIdFromResponse)
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // create the Policy again with the same ID
        putPolicy(policyId, policy)
                .expectingHttpStatus(CREATED)
                .fire();

        // now accessing the Thing must also work again
        getThing(TestConstants.API_V_2, thingIdFromResponse)
                .expectingHttpStatus(OK)
                .fire();

        // clean up
        awaitUntil(() -> deleteThing(TestConstants.API_V_2, thingIdFromResponse)
                .expectingHttpStatus(NO_CONTENT)
                .fire());

        deletePolicy(policyIdFromResponse)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void putThingWithInvalidPolicyId() {
        final String invalidPolicyId = idGenerator().withName("");

        final JsonObject thingWithInvalidPolicyId = Thing.newBuilder()
                .setGeneratedId()
                .build()
                .toJson()
                .set(Thing.JsonFields.POLICY_ID, invalidPolicyId);

        putThing(TestConstants.API_V_2, thingWithInvalidPolicyId, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("things:policy.id.invalid");
    }

    @Test
    public void postThingWithInvalidPolicyId() {
        final String invalidPolicyId = idGenerator().withName("");

        final JsonObject thingWithInvalidPolicyId = Thing.newBuilder()
                .setGeneratedId()
                .build()
                .toJson()
                .set(Thing.JsonFields.POLICY_ID, invalidPolicyId);

        postThing(TestConstants.API_V_2, thingWithInvalidPolicyId)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("things:policy.id.invalid");
    }

    @Test
    public void updatePolicyIdOfThingWithInvalidPolicyId() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName(getClass().getName()));

        final Thing validThing = Thing.newBuilder()
                .setGeneratedId()
                .setPolicyId(policyId)
                .build();

        putThing(TestConstants.API_V_2, validThing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED);

        final String invalidPolicyId = idGenerator().withName("");

        final JsonObject thingWithInvalidPolicyId = Thing.newBuilder()
                .setId(validThing.getEntityId().get())
                .build()
                .toJson()
                .set(Thing.JsonFields.POLICY_ID, invalidPolicyId);

        putThing(TestConstants.API_V_2, thingWithInvalidPolicyId, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("things:policy.id.invalid");
    }

    @Test
    public void putLargeThingWithLargeInlinePolicy() {
        final ThingId thingId = ThingId.of(idGenerator().withName("putLargeThingWithLargeInlinePolicy"));

        final PolicyBuilder policyBuilder = Policy.newBuilder(PolicyId.of(thingId));
        int i = 0;
        Policy policy;
        do {
            policyBuilder.forLabel("ENTRY-NO" + i)
                    .setSubject(ThingsSubjectIssuer.DITTO,
                            serviceEnv.getDefaultTestingContext().getOAuthClient().getClientId(),
                            ARBITRARY_SUBJECT_TYPE)
                    .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                    .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE);
            policy = policyBuilder.build();
            i++;
        } while (policy.toJsonString().length() < DEFAULT_MAX_POLICY_SIZE);

        // just stay above the limit for policy sizes by removing the last entry:
        policy = policy.removeEntry("ENTRY-NO" + (i - 1));

        final ThingBuilder.FromScratch thingBuilder = ThingsModelFactory.newThingBuilder().setId(thingId);
        Thing thing;
        do {
            thingBuilder.setAttribute(JsonPointer.of("attr" + i), JsonValue.of(i));
            thing = thingBuilder.build();
            i++;
        } while (thing.toJsonString().length() < DEFAULT_MAX_THING_SIZE - 100); // subtract 100 bytes for JSON overhead

        thing = thing.removeAttribute("attr" + (i - 1));

        final JsonObjectBuilder builder = thing.toJson(JsonSchemaVersion.V_2).toBuilder();
        builder.set(CreateThing.JSON_INLINE_POLICY, policy.toJson(JsonSchemaVersion.V_2));
        final JsonObject thingJsonObjectWithInlinePolicy = builder.build();

        putThing(TestConstants.API_V_2, thingJsonObjectWithInlinePolicy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        // clean up
        awaitUntil(() -> deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire());

        deletePolicy(thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void putThingWithTooLargeInlinePolicy() {
        final ThingId thingId = ThingId.of(idGenerator().withName("putThingWithTooLargeInlinePolicy"));

        final PolicyBuilder policyBuilder = Policy.newBuilder(PolicyId.of(thingId));
        int i = 0;
        Policy policy;
        do {
            policyBuilder.forLabel("ENTRY-NO" + i)
                    .setSubject(ThingsSubjectIssuer.DITTO,
                            serviceEnv.getDefaultTestingContext().getOAuthClient().getClientId(),
                            ARBITRARY_SUBJECT_TYPE)
                    .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                    .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE);
            policy = policyBuilder.build();
            i++;
        } while (policy.toJsonString().length() < DEFAULT_MAX_POLICY_SIZE);

        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId)
                .build();

        final JsonObjectBuilder builder = thing.toJson(JsonSchemaVersion.V_2).toBuilder();
        builder.set(CreateThing.JSON_INLINE_POLICY, policy.toJson(JsonSchemaVersion.V_2));
        final JsonObject thingJsonObjectWithInlinePolicy = builder.build();

        putThing(TestConstants.API_V_2, thingJsonObjectWithInlinePolicy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.REQUEST_ENTITY_TOO_LARGE)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        deletePolicy(thingId)
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    private static JsonObject createPolicyJson(final PolicyId policyId) {
        final JsonObjectBuilder policyJsonObjectBuilder = JsonObject.newBuilder();
        policyJsonObjectBuilder.set(Policy.JsonFields.ID, policyId.toString());

        final var defaultSubject = serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject();
        final var policyEntryJson = createPolicyEntryJson(Permission.MIN_REQUIRED_POLICY_PERMISSIONS, defaultSubject);

        final JsonObjectBuilder policyEntriesJsonObjectBuilder = JsonObject.newBuilder();
        policyEntriesJsonObjectBuilder.set("DEFAULT", policyEntryJson);

        policyJsonObjectBuilder.set(Policy.JsonFields.ENTRIES, policyEntriesJsonObjectBuilder.build());
        return policyJsonObjectBuilder.build();
    }

    private static JsonObject createPolicyEntryJson(final Permissions granted, final Subject subject) {
        final JsonObject subjectsWithoutType = JsonObject.newBuilder()
                .set(subject.getId(), subject.toJson().remove(Subject.JsonFields.TYPE.getPointer()))
                .build();

        return JsonObject.newBuilder()
                .set(PolicyEntry.JsonFields.SUBJECTS, subjectsWithoutType)
                .set(PolicyEntry.JsonFields.RESOURCES, JsonObject.newBuilder()
                        .set(JsonKey.of("thing:/"), JsonObject.newBuilder()
                                .set(EffectedPermissions.JsonFields.GRANT, granted.stream()
                                        .map(JsonValue::of)
                                        .collect(JsonCollectors.valuesToArray()))
                                .set(EffectedPermissions.JsonFields.REVOKE, JsonArray.empty())
                                .build())
                        .build())
                .build();
    }

    private static void awaitUntil(final ThrowingRunnable assertion) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(assertion);
    }

}
