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
package org.eclipse.ditto.testing.system.things.authorization;

import static org.eclipse.ditto.base.model.common.HttpStatus.BAD_REQUEST;
import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
import static org.eclipse.ditto.base.model.common.HttpStatus.FORBIDDEN;
import static org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT;
import static org.eclipse.ditto.json.assertions.DittoJsonAssertions.assertThat;
import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;
import static org.eclipse.ditto.testing.common.TestConstants.API_V_2;
import static org.eclipse.ditto.testing.common.TestConstants.Policy.ARBITRARY_SUBJECT_TYPE;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.FieldType;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonField;
import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.json.JsonKey;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.messages.model.MessageDirection;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.Permissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.ResourceKey;
import org.eclipse.ditto.policies.model.Resources;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectId;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.policies.model.SubjectType;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.matcher.PostMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.Features;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.exceptions.ThingNotCreatableException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import io.restassured.http.ContentType;
import io.restassured.response.Response;

/**
 * Integration test for Policy based authorization of Things REST API.
 */
public final class PolicyEnforcementIT extends IntegrationTest {

    private static final String ATTRIBUTE_A = "attributeA";
    private static final boolean ATTRIBUTE_A_VALUE = true;

    private static SubjectId defaultClientSubjectId;
    private static SubjectId client2SubjectId;
    private static SubjectId client3SubjectId;

    @Rule
    public TestName name = new TestName();

    private static Subject getSubject(final TestingContext context) {
        if (context.getBasicAuth().isEnabled()) {
            return Subject.newInstance(
                    SubjectIssuer.newInstance("nginx"), context.getBasicAuth().getUsername());
        } else {
            return context.getOAuthClient().getDefaultSubject();
        }
    }

    @BeforeClass
    public static void initTestFixture() {
        defaultClientSubjectId = getSubject(serviceEnv.getDefaultTestingContext()).getId();
        client2SubjectId = getSubject(serviceEnv.getTestingContext2()).getId();
        client3SubjectId = getSubject(serviceEnv.getTestingContext3()).getId();
    }

    /**
     * Posts a Thing to the service with an implicit Policy and gets the policy afterwards.
     */
    @Test
    public void postThingWithImplicitPolicy() {
        final String location = postThing(API_V_2)
                .expectingHttpStatus(CREATED)
                .fire()
                .header("Location");

        final ThingId thingId = ThingId.of(parseIdFromLocation(location));
        final PolicyId policyId = PolicyId.of(thingId);

        final Policy expectedPolicy = createDefaultPolicy(defaultClientSubjectId, policyId);

        getPolicy(policyId)
                .expectingBody(contains(expectedPolicy.toJson()))
                .fire();
    }

    @Test
    public void getThingWithGrantedReadOnAttribute() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        putThingWithPolicyAdditionalAccess(thingId, defaultClientSubjectId, client2SubjectId, ATTRIBUTE_A, "READ")
                .expectingHttpStatus(CREATED)
                .fire()
                .header("Location");

        final JsonValue expectedBody = createOnlyAttributeBody(thingId);

        // There may only be returned the partial thing containing the attribute on which the passed in user has access
        getThing(API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(containsOnly(expectedBody))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void getThingWithoutGrantedRead() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        putThingWithPolicyAdditionalAccess(thingId, defaultClientSubjectId, client2SubjectId, ATTRIBUTE_A, "READ")
                .expectingHttpStatus(CREATED)
                .fire();

        // not found expected as user 3 has o rights on the thing
        getThing(API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext3())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void postThingAlterPolicyForReadAccess() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);

        putThingWithPolicyAdditionalAccess(thingId, defaultClientSubjectId, client2SubjectId, ATTRIBUTE_A, "READ")
                .expectingHttpStatus(CREATED)
                .fire()
                .header("Location");

        // not accessible with user 3
        getThing(API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext3())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        // alter policy
        final Policy alteredPolicy =
                createPolicyRootAndAdditionalAccess(policyId, defaultClientSubjectId, client3SubjectId, "READ",
                        ATTRIBUTE_A);
        putPolicy(policyId, alteredPolicy)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        final JsonValue expectedBody = createOnlyAttributeBody(thingId);
        getThing(API_V_2, thingId)
                .useAwaitility(AWAITILITY_DEFAULT_CONFIG)
                .withConfiguredAuth(serviceEnv.getTestingContext3())
                .expectingBody(containsOnly(expectedBody))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void postThingAlterPolicyForWriteAccessOnAttribute() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final String location =
                putThingWithPolicyAdditionalAccess(thingId, defaultClientSubjectId, client2SubjectId, ATTRIBUTE_A,
                        "READ")
                        .expectingHttpStatus(CREATED)
                        .fire()
                        .header("Location");

        final String testThingId = parseIdFromLocation(location);
        final PolicyId policyId = PolicyId.of(testThingId);

        final Policy alteredPolicy =
                createPolicyRootAndAdditionalAccess(policyId, defaultClientSubjectId, client3SubjectId, "WRITE",
                        ATTRIBUTE_A);

        putAttribute(API_V_2, testThingId, ATTRIBUTE_A, "false")
                .withConfiguredAuth(serviceEnv.getTestingContext3())
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // alter policy
        putPolicy(policyId, alteredPolicy)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        Awaitility.await().atMost(5000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> putAttribute(API_V_2, testThingId, ATTRIBUTE_A, "false")
                        .withConfiguredAuth(serviceEnv.getTestingContext3())
                        .expectingHttpStatus(NO_CONTENT)
                        .fire());
    }

    @Test
    public void putThingShouldNotDeletePolicyWithSameId() {
        final ThingId thingId =
                ThingId.of(idGenerator().withPrefixedRandomName("putThingShouldNotDeletePolicyWithSameId"));
        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId).build();

        final ResourceKey thingPolicyId = ResourceKey.newInstance(PoliciesResourceType.THING, "policyId");
        final Policy defaultPolicy = createDefaultPolicy(defaultClientSubjectId, PolicyId.of(thingId));
        final Policy policy = defaultPolicy.toBuilder()
                .forLabel("revoke-thing-policyId")
                .setSubject(Subject.newInstance(defaultClientSubjectId, ARBITRARY_SUBJECT_TYPE))
                .setRevokedPermissions(thingPolicyId, WRITE)
                .build();

        // GIVEN: thing created with implicit policy
        putThing(API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // GIVEN: implicit policy modified with revoke on thing:/policyId
        putPolicy(thingId, policy)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // WHEN: an update is made on the thing with explicit policy ID
        final PolicyId newPolicyId = PolicyId.of(idGenerator().withRandomName());
        final Thing newThing = thing.toBuilder().setPolicyId(newPolicyId).build();
        putThing(API_V_2, newThing, JsonSchemaVersion.V_2)
                .useAwaitility(Awaitility.await())
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // THEN: the thing should remain accessible
        final JsonField expectedThingId =
                JsonFactory.newField(JsonKey.of("thingId"), JsonFactory.newValue(thingId.toString()));
        final JsonField expectedPolicyId =
                JsonFactory.newField(JsonKey.of("policyId"), JsonFactory.newValue(thingId.toString()));

        getThing(API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(satisfies(body -> {
                    final JsonObject thingObject = JsonFactory.newObject(body);

                    assertThat(thingObject).contains(expectedThingId, expectedPolicyId);
                }));


        // THEN: the policy should remain accessible
        getPolicy(thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(satisfies(body -> {
                    final JsonObject policyObject = JsonFactory.newObject(body);
                    assertThat(policyObject).contains(expectedPolicyId);
                }));

        // CLEANUP
        putPolicy(thingId, defaultPolicy)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        deleteThing(API_V_2, thingId)
                .useAwaitility(AWAITILITY_DEFAULT_CONFIG)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        deletePolicy(thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void putThingWithPolicyId() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName("putThingWithPolicyId"));
        final Policy policy = createDefaultPolicy(defaultClientSubjectId, policyId);
        final ThingId thingId = ThingId.of(idGenerator().withPrefixedRandomName("putThingWithPolicyId"));
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .build();

        // GIVEN: policy exists
        putPolicy(policyId, policy)
                .expectingHttpStatus(CREATED)
                .fire();

        // WHEN: a thing with that policy is successfully created
        putThing(API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // THEN: identity updates to the thing should succeed
        putThing(API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void putThingWithImplicitPolicy() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        putThingWithPolicyAdditionalAccess(thingId, defaultClientSubjectId, client2SubjectId, ATTRIBUTE_A, "READ")
                .expectingHttpStatus(CREATED)
                .fire();

        final PolicyId policyId = PolicyId.of(thingId);
        final Policy expectedPolicy = createDefaultPolicy(defaultClientSubjectId, policyId, Label.of("rootUser"));

        getPolicy(policyId)
                .expectingBody(contains(expectedPolicy.toJson()))
                .fire();
    }

    @Test
    public void postThingWithExplicitInvalidPolicy() {
        postThingWithInvalidPolicy(ThingId.of(idGenerator().withRandomName()))
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    @Test
    public void putAttributesWithRevokedWriteOnSingleAttribute() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        putThingWithPolicyRevokedAccess(thingId, defaultClientSubjectId, client2SubjectId, ATTRIBUTE_A, "WRITE")
                .expectingHttpStatus(CREATED)
                .fire();

        putAttributes(API_V_2, thingId, createAttributes())
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(FORBIDDEN)
                .fire();
    }

    @Test
    public void deleteAndRecreateThing() {
        final String location = postThing(API_V_2)
                .expectingHttpStatus(CREATED)
                .fire()
                .header("Location");

        final ThingId thingId = ThingId.of(parseIdFromLocation(location));

        final Response getThingResponse = getThing(API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final Thing thingAsPosted = ThingsModelFactory.newThingBuilder(getThingResponse.getBody().asString()).build();
        final PolicyId policyId = thingAsPosted.getPolicyId()
                .orElseThrow(() -> new IllegalStateException("Expected a Policy ID"));

        // delete thing - policy is not deleted
        deleteThing(API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        try {
            TimeUnit.MILLISECONDS.sleep(100);
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }

        final Thing thing = Thing.newBuilder().setId(thingId).build();

        // recreate the thing without reusing the policy - should NOT work
        // PUT provokes ThingNotCreatableException because policy already exists - 403
        putThing(API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(BAD_REQUEST)
                .expectingErrorCode(ThingNotCreatableException.ERROR_CODE)
                .fire();

        // recreate the thing with reusing the policy - should work
        putThing(API_V_2, thing.toBuilder().setPolicyId(policyId).build(), JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // try to access the Thing with the "another use" - should not be accessible (as policy was implicitly created for other user)
        getThing(API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        // try to access the Thing with the other user who created the policy - should work
        getThing(API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void createThingWithInlinePolicyContainingNoPermissions() {
        // GIVEN
        final PolicyId policyId =
                PolicyId.of(idGenerator().withPrefixedRandomName("createThingWithInlinePolicyContainingNoPermissions"));
        final Policy policyWithoutThingPermissions = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("POLICY")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getSubject())
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .build();

        final JsonObject policyWithoutThingPermissionsJson =
                policyWithoutThingPermissions.toJson(JsonSchemaVersion.LATEST,
                        JsonFieldSelector.newInstance(Policy.JsonFields.ENTRIES.getPointer().toString()));

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();

        final JsonObject thingJson = thing.toJson()
                .toBuilder()
                .set(Policy.INLINED_FIELD_NAME, policyWithoutThingPermissionsJson)
                .build();

        // WHEN/THEN
        putThing(API_V_2, thingJson, JsonSchemaVersion.V_2)
                .expectingHttpStatus(FORBIDDEN) // must not be allowed as the policy does not contain WRITE on "/"
                .fire();

        // GIVEN
        final Policy policyWithThingPermissions = policyWithoutThingPermissions.toBuilder()
                .forLabel("POLICY")
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();
        final JsonObject policyWithThingPermissionsJson = policyWithThingPermissions.toJson(JsonSchemaVersion.LATEST,
                JsonFieldSelector.newInstance(Policy.JsonFields.ENTRIES.getPointer().toString()));

        final JsonObject otherThingJson = thing.toBuilder().setId(ThingId.of(idGenerator().withRandomName())).build()
                .toJson().toBuilder()
                .set(Policy.INLINED_FIELD_NAME, policyWithThingPermissionsJson)
                .build();

        // WHEN/THEN
        putThing(API_V_2, otherThingJson, JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getDefaultTestingContext())
                .expectingHttpStatus(CREATED) // now that the thing permissions are included it must be allowed
                .fire();
    }

    @Test
    public void createThingWithRevokeForChangingPolicyId() {
        final String location = postThing(API_V_2)
                .expectingHttpStatus(CREATED)
                .fire()
                .header("Location");

        final ThingId thingId = ThingId.of(parseIdFromLocation(location));
        final PolicyId policyId = PolicyId.of(thingId);

        final PolicyId newPolicyId = PolicyId.of(idGenerator().withRandomName());
        putPolicy(newPolicyId, createDefaultPolicy(defaultClientSubjectId, newPolicyId))
                .expectingHttpStatus(CREATED)
                .fire();

        // create Policy with revoke on "policyId" for user1
        final Policy permittingPolicy = createDefaultPolicy(defaultClientSubjectId, policyId)
                .setEffectedPermissionsFor("DEFAULT", PoliciesResourceType.thingResource("policyId"),
                        EffectedPermissions.newInstance(Permissions.none(), collect("WRITE")));

        // alter policy adding the revoke
        putPolicy(policyId, permittingPolicy)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        final Thing updatedThingWithOtherPolicyId = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(newPolicyId)
                .build();

        // alter the Thing trying to set another policyId which must be forbidden
        putThing(API_V_2, updatedThingWithOtherPolicyId, JsonSchemaVersion.V_2)
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // now change the policy again and remove the unwanted "revoke":
        final Policy allowingPolicy =
                permittingPolicy.setEffectedPermissionsFor("DEFAULT", PoliciesResourceType.thingResource("policyId"),
                        EffectedPermissions.newInstance(collect("READ", "WRITE"), Permissions.none()));

        // alter policy effectively removing the WRITE revoke on "policyId" again
        putPolicy(thingId, allowingPolicy)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // alter the Thing trying to set another policyId should now be allowed again
        putThing(API_V_2, updatedThingWithOtherPolicyId, JsonSchemaVersion.V_2)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void createThingRemovePolicyEnsureThatThingIsNoLongerAccessible() {
        final String location = postThing(API_V_2)
                .expectingHttpStatus(CREATED)
                .fire()
                .header("Location");

        final ThingId thingId = ThingId.of(parseIdFromLocation(location));
        final PolicyId policyId = PolicyId.of(thingId);

        // delete the implicitly created policy
        deletePolicy(policyId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // getting the Thing should no longer work
        getThing(API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        final Thing theThing = Thing.newBuilder().setId(thingId).build();

        // modifying the Thing should no longer work
        putThing(API_V_2, theThing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // adding the Policy again
        putPolicy(policyId, createDefaultPolicy(defaultClientSubjectId, policyId))
                .expectingHttpStatus(CREATED)
                .fire();

        // now getting the Thing should work again
        getThing(API_V_2, thingId)
                .useAwaitility(Awaitility.await())
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void enforcePolicyForThingMessages() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName("enforcePolicyForThingMessages"));
        final Policy policyWithoutMessagesPermissions = createDefaultPolicy(defaultClientSubjectId, policyId);
        putPolicy(policyId, policyWithoutMessagesPermissions)
                .expectingHttpStatus(CREATED)
                .fire();

        // create a Thing to use for message sending
        final ThingId thingId = ThingId.of(idGenerator().withPrefixedRandomName("enforcePolicyForThingMessages"));
        final Thing thing = Thing.newBuilder().setId(thingId).setPolicyId(policyId).build();
        putThing(API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // POSTing a message with subject "test" in the inbox of the Thing should not be permitted
        postMessage(API_V_2, thingId, MessageDirection.TO, "test", ContentType.TEXT, "foo!", "5")
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        final Policy policyWithMessagesPermissions = policyWithoutMessagesPermissions
                .setSubjectFor("MESSAGE_SENDER",
                        Subject.newInstance(defaultClientSubjectId, ARBITRARY_SUBJECT_TYPE))
                .setEffectedPermissionsFor(
                        "MESSAGE_SENDER",
                        PoliciesResourceType.messageResource("/"), // grant WRITE for all subjects
                        EffectedPermissions.newInstance(collect("WRITE"), Permissions.none()))
                .setEffectedPermissionsFor(
                        "MESSAGE_SENDER",
                        PoliciesResourceType.messageResource("/inbox/messages/test"), // revoke WRITE for subject "test"
                        EffectedPermissions.newInstance(Permissions.none(), collect("WRITE")));

        // modify the Policy so that the MESSAGE_SENDER entry is added
        putPolicy(policyId, policyWithMessagesPermissions)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // POSTing a message with subject "foo" in the inbox of the Thing should now BE permitted
        postMessage(API_V_2, thingId, MessageDirection.TO, "foo", ContentType.TEXT, "foo!", "0")
                .useAwaitility(AWAITILITY_DEFAULT_CONFIG)
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                .fire();

        // POSTing a message with subject "test" in the inbox of the Thing should NOT be permitted
        postMessage(API_V_2, thingId, MessageDirection.TO, "test", ContentType.TEXT, "test!", "5")
                .expectingHttpStatus(FORBIDDEN)
                .fire();
    }

    @Test
    @Category(Acceptance.class)
    public void enforcePolicyForThingFeatureMessages() {
        final PolicyId policyId =
                PolicyId.of(idGenerator().withPrefixedRandomName("enforcePolicyForThingFeatureMessages"));
        final Policy policyWithoutMessagesPermissions = createDefaultPolicy(defaultClientSubjectId, policyId);
        putPolicy(policyId, policyWithoutMessagesPermissions)
                .expectingHttpStatus(CREATED)
                .fire();

        // create a Thing to use for message sending
        final ThingId thingId =
                ThingId.of(idGenerator().withPrefixedRandomName("enforcePolicyForThingFeatureMessages"));
        final Thing thing = Thing.newBuilder().setId(thingId).setPolicyId(policyId).build();
        putThing(API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // POSTing a message with subject "foo" in the outbox of the Thing feature "granted" should not be permitted
        postMessage(API_V_2, thingId, "granted", MessageDirection.FROM, "foo", ContentType.TEXT, "foo!", "5")
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        final Policy policyWithFeatureMessagesPermissions = policyWithoutMessagesPermissions
                .setSubjectFor("FEATURE_MESSAGE_SENDER",
                        Subject.newInstance(defaultClientSubjectId, ARBITRARY_SUBJECT_TYPE))
                .setEffectedPermissionsFor(
                        "FEATURE_MESSAGE_SENDER",
                        PoliciesResourceType.messageResource("/features/granted"),
                        // grant WRITE for all feature messages subjects on feature "granted"
                        EffectedPermissions.newInstance(collect("WRITE"), Permissions.none()))
                .setEffectedPermissionsFor(
                        "FEATURE_MESSAGE_SENDER",
                        PoliciesResourceType.messageResource("/features/other/outbox"),
                        // grant WRITE for all feature messages subjects in outbox on feature "other"
                        EffectedPermissions.newInstance(collect("WRITE"), Permissions.none()))
                .setEffectedPermissionsFor(
                        "FEATURE_MESSAGE_SENDER",
                        PoliciesResourceType.messageResource("/features/other/outbox/messages/test"),
                        // revoke WRITE for subject "test" in outbox on feature "other"
                        EffectedPermissions.newInstance(Permissions.none(), collect("WRITE")));

        // modify the Policy so that the FEATURE_MESSAGE_SENDER entry is added
        putPolicy(policyId, policyWithFeatureMessagesPermissions)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // POSTing a message with subject "foo" in the outbox of the Thing feature "granted" should now BE permitted
        postMessage(API_V_2, thingId, "granted", MessageDirection.FROM, "foo", ContentType.TEXT, "foo!", "0")
                .useAwaitility(AWAITILITY_DEFAULT_CONFIG)
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                .fire();

        // POSTing a message with subject "test" in the inbox of the Thing feature "revoked" should NOT be permitted
        postMessage(API_V_2, thingId, "revoked", MessageDirection.TO, "foo", ContentType.TEXT, "foo!", "5")
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // POSTing a message with subject "test" in the inbox of the Thing feature "revoked" should NOT be permitted
        postMessage(API_V_2, thingId, "other", MessageDirection.TO, "test", ContentType.TEXT, "foo!", "5")
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // POSTing a message with subject "foo" in the outbox of the Thing feature "revoked" should NOT be permitted
        postMessage(API_V_2, thingId, "other", MessageDirection.FROM, "foo", ContentType.TEXT, "foo!", "0")
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                .fire();

        // POSTing a message with subject "test" in the outbox of the Thing feature "revoked" should NOT be permitted
        postMessage(API_V_2, thingId, "other", MessageDirection.FROM, "test", ContentType.TEXT, "foo!", "5")
                .expectingHttpStatus(FORBIDDEN)
                .fire();
    }

    @Test
    public void enforcePolicyForPolicy() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName("enforcePolicyForPolicy"));
        final Policy defaultPolicy = createDefaultPolicy(defaultClientSubjectId, policyId);
        putPolicy(policyId, defaultPolicy)
                .expectingHttpStatus(CREATED)
                .fire();

        // create Policy with "READER" label containing read permissions for all entries for user2
        final Subject user2Subject = Subject.newInstance(client2SubjectId, ARBITRARY_SUBJECT_TYPE);
        final Policy readerPolicy = defaultPolicy
                .setSubjectFor("READER", user2Subject)
                .setEffectedPermissionsFor("READER", PoliciesResourceType.policyResource("entries/READER"),
                        EffectedPermissions.newInstance(collect("READ"), Permissions.none()));

        // alter policy adding the revoke
        putPolicy(policyId, readerPolicy)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // reading the DEFAULT entry should not be permitted for user2
        getPolicyEntry(policyId, "DEFAULT")
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        // reading the READER entry should be permitted for user2
        getPolicyEntry(policyId, "READER")
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        putPolicyEntry(policyId, PolicyEntry.newInstance("READER", Subjects.newInstance(user2Subject),
                Resources.newInstance(Resource.newInstance(PoliciesResourceType.policyResource("entries/READER"),
                        EffectedPermissions.newInstance(collect("READ", "WRITE"),
                                Permissions.none())))))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(
                        FORBIDDEN) // writing the "READER" policyEntry should be permitted for user2
                .fire();
    }

    @Test
    public void jsonViewOfFeatureOfThingWithRestrictedPermissionsIsExpected() {
        final Feature gyroscopeFeature = ThingsModelFactory.newFeatureBuilder()
                .properties(ThingsModelFactory.newFeaturePropertiesBuilder()
                        .set("status", JsonFactory.newObjectBuilder()
                                .set("minRangeValue", -2000)
                                .set("xValue", -0.05071427300572395)
                                .set("units", "Deg/s")
                                .set("yValue", -0.4192921817302704)
                                .set("zValue", 0.20766231417655945)
                                .set("maxRangeValue", 2000)
                                .build())
                        .build())
                .withId("Gyroscope.0")
                .build();

        final Feature nonsenseFeature = ThingsModelFactory.newFeatureBuilder()
                .properties(ThingsModelFactory.newFeaturePropertiesBuilder()
                        .set("foo", "bar")
                        .set("variousValues", JsonFactory.newArrayBuilder()
                                .add(23)
                                .add("boogle")
                                .add(true)
                                .build())
                        .build())
                .withId("loco")
                .build();

        final PolicyId policyId = PolicyId.of(
                idGenerator().withPrefixedRandomName("jsonViewOfFeatureOfThingWithRestrictedPermissionsIsExpected-"));
        final Policy policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("owner")
                .setSubject(Subject.newInstance(defaultClientSubjectId, SubjectType.GENERATED))
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"),
                        collect("READ", "WRITE"))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"),
                        collect("READ", "WRITE"))
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"),
                        collect("READ", "WRITE"))
                .forLabel("client")
                .setSubject(Subject.newInstance(client2SubjectId, SubjectType.GENERATED))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/features/" + gyroscopeFeature.getId()),
                        collect("READ", "WRITE"))
                .build();

        final Features features = ThingsModelFactory.newFeatures(gyroscopeFeature, nonsenseFeature);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setAttributes(ThingsModelFactory.newAttributesBuilder()
                        .set("isOnline", false)
                        .set("lastUpdate", "Thu Sep 28 15:01:43 CEST 2017")
                        .build())
                .setFeatures(features)
                .build();

        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        putThing(API_V_2, thing, JsonSchemaVersion.V_2)
                .useAwaitility(AWAITILITY_DEFAULT_CONFIG)
                .withConfiguredAuth(serviceEnv.getDefaultTestingContext())
                .expectingHttpStatus(CREATED)
                .fire();

        getFeature(API_V_2, thingId, gyroscopeFeature.getId())
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsOnly(gyroscopeFeature.toJson()))
                .fire();

        getFeatures(API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsOnly(JsonFactory.newObjectBuilder()
                        .set(gyroscopeFeature.getId(), gyroscopeFeature.toJson())
                        .build()))
                .fire();

        getFeatures(API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getDefaultTestingContext())
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsOnly(features.toJson()))
                .fire();
    }

    private static Permissions collect(final String permission, final String... furtherPermissions) {
        final Collection<String> allPermissions = new HashSet<>(1 + furtherPermissions.length);
        allPermissions.add(permission);
        allPermissions.addAll(Arrays.asList(furtherPermissions));
        return PoliciesModelFactory.newPermissions(allPermissions);
    }

    private static String createAttributes() {
        return ThingsModelFactory.newAttributesBuilder().set("newAttr", "newVal").build().toJsonString();
    }

    private static JsonObject createOnlyAttributeBody(final ThingId thingId) {
        return ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonFactory.newPointer(ATTRIBUTE_A), JsonFactory.newValue(ATTRIBUTE_A_VALUE))
                .build()
                .toJson();
    }

    private static PutMatcher putThingWithPolicyAdditionalAccess(final ThingId thingId,
            final SubjectId rootUser,
            final SubjectId readAccessUser,
            final String attribute,
            final String permission) {

        final Policy policy =
                createPolicyRootAndAdditionalAccess(PolicyId.of(thingId), rootUser, readAccessUser, permission,
                        attribute);

        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonFactory.newPointer(attribute), JsonFactory.newValue(ATTRIBUTE_A_VALUE))
                .setAttribute(JsonFactory.newPointer("attr2"), JsonFactory.newValue(54))
                .build();

        return putThingWithPolicy(API_V_2, thing, policy, JsonSchemaVersion.V_2);
    }

    private static PutMatcher putThingWithPolicyRevokedAccess(final ThingId thingId,
            final SubjectId rootUser,
            final SubjectId revokedAccessUser,
            final String attribute,
            final String permission) {

        final Policy policy =
                createPolicyRootAndAdditionalAccessWithRevoke(PolicyId.of(thingId), rootUser, revokedAccessUser,
                        permission, attribute);

        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonFactory.newPointer(attribute), JsonFactory.newValue(ATTRIBUTE_A_VALUE))
                .setAttribute(JsonFactory.newPointer("attr2"), JsonFactory.newValue(54))
                .build();

        return putThingWithPolicy(API_V_2, thing, policy, JsonSchemaVersion.V_2);
    }

    private static PostMatcher postThingWithInvalidPolicy(final ThingId thingId) {
        final Policy defaultPolicy = createDefaultPolicy(defaultClientSubjectId, PolicyId.of(thingId));
        final Policy invalidPolicy = defaultPolicy.setResourceFor("DEFAULT",
                Resource.newInstance(ResourceKey.newInstance("thing:"),
                        EffectedPermissions.newInstance(new HashSet<>(), new HashSet<>())));

        final JsonObject thingJson = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .build()
                .toJson(FieldType.regularOrSpecial())
                .setAll(invalidPolicy.toInlinedJson(JsonSchemaVersion.V_2, FieldType.notHidden()));

        return postThing(API_V_2, thingJson);
    }

    private static Policy createPolicyRootAndAdditionalAccess(final PolicyId policyId,
            final SubjectId rootUser,
            final SubjectId additionalUser,
            final String additionalGrantedPermission,
            final String attribute) {

        final EffectedPermissions permissionsAll =
                EffectedPermissions.newInstance(collect("READ", "WRITE"), Permissions.none());
        final EffectedPermissions permissionsAdditional = EffectedPermissions.newInstance(
                Permissions.newInstance("READ", additionalGrantedPermission), Permissions.none());
        final PolicyEntry entry1 = PoliciesModelFactory.newPolicyEntry(
                Label.of("rootUser"),
                Subjects.newInstance(PoliciesModelFactory.newSubject(rootUser, SubjectType.GENERATED)),
                PoliciesModelFactory.newResources(
                        Resource.newInstance(ResourceKey.newInstance("policy:"), permissionsAll),
                        Resource.newInstance(ResourceKey.newInstance("thing:"), permissionsAll)));

        final PolicyEntry entry2 = PoliciesModelFactory.newPolicyEntry(
                Label.of("partialReadUser"),
                Subjects.newInstance(
                        PoliciesModelFactory.newSubject(additionalUser, ARBITRARY_SUBJECT_TYPE)),
                PoliciesModelFactory.newResources(Resource.newInstance(
                        PoliciesResourceType.thingResource("/attributes/" + attribute), permissionsAdditional)));

        return PoliciesModelFactory.newPolicy(policyId, entry1, entry2);
    }

    private static Policy createPolicyRootAndAdditionalAccessWithRevoke(final PolicyId policyId,
            final SubjectId rootUser,
            final SubjectId additionalUser,
            final String additionalRevokedPermission,
            final String attribute) {

        final EffectedPermissions permissionsAll =
                EffectedPermissions.newInstance(collect("READ", "WRITE"), Permissions.none());
        final EffectedPermissions permissionsAdditional = EffectedPermissions.newInstance(Permissions.none(),
                Permissions.newInstance(additionalRevokedPermission));
        final PolicyEntry entry1 = PoliciesModelFactory.newPolicyEntry(
                Label.of("rootUser"),
                Subjects.newInstance(PoliciesModelFactory.newSubject(rootUser, ARBITRARY_SUBJECT_TYPE)),
                PoliciesModelFactory.newResources(
                        Resource.newInstance(ResourceKey.newInstance("policy:"), permissionsAll),
                        Resource.newInstance(ResourceKey.newInstance("thing:"), permissionsAll)));

        final PolicyEntry entry2 = PoliciesModelFactory.newPolicyEntry(
                Label.of("partialReadUser"),
                Subjects.newInstance(
                        PoliciesModelFactory.newSubject(additionalUser, ARBITRARY_SUBJECT_TYPE)),
                PoliciesModelFactory.newResources(
                        Resource.newInstance(ResourceKey.newInstance("thing:/attributes/" + attribute),
                                permissionsAdditional),
                        Resource.newInstance(ResourceKey.newInstance("thing:"), permissionsAll)));

        return PoliciesModelFactory.newPolicy(policyId, entry1, entry2);
    }

    private static Policy createDefaultPolicy(final SubjectId user, final PolicyId policyId) {
        return createDefaultPolicy(user, policyId, Label.of("DEFAULT"));
    }

    private static Policy createDefaultPolicy(final SubjectId user, final PolicyId policyId, final Label label) {
        final EffectedPermissions permissions =
                EffectedPermissions.newInstance(collect("READ", "WRITE"), Permissions.none());
        final PolicyEntry entry = PoliciesModelFactory.newPolicyEntry(Label.of(label),
                Subjects.newInstance(Subject.newInstance(user)),
                PoliciesModelFactory.newResources(
                        Resource.newInstance(ResourceKey.newInstance("policy:"), permissions),
                        Resource.newInstance(ResourceKey.newInstance("thing:"), permissions)));
        return PoliciesModelFactory.newPolicy(policyId, entry);
    }

}
