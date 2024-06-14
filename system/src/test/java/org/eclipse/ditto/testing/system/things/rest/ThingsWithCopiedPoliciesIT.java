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

import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;
import static org.eclipse.ditto.testing.common.TestConstants.API_V_2;

import java.util.Arrays;
import java.util.Collections;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonObjectBuilder;
import org.eclipse.ditto.policies.api.Permission;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.ResourceKey;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.restassured.response.Response;

/**
 * Integration tests that verify the behavior when copying a policy using _copyPolicyFrom when creating a new Thing.
 * <p>
 * <p>
 * {@code
 * request   reference  thing accessible  policy accessible  new thing acc.  expected status
 * ------- | -------- | --------------- | ---------------- | ------------- | --------------- |
 * POST      policyId       /                     +               +            201
 * *                        /                     +               -            403
 * *                        /                     -               /            404
 * *         {{thing}}      +                     +               +            201
 * *                        +                     +               -            - (can never happen, since user then also has no access on existing thing)
 * *                        +                     -               /            404
 * *                        -                     /               /            404
 * PUT       policyId       /                     +               +            201
 * *                        /                     +               -            403
 * *                        /                     -               /            404
 * *         {{thing}}      +                     +               +            201
 * *                        +                     +               -            - (can never happen, since user then also has no access on existing thing)
 * *                        +                     -               /            404
 * *                        -                     /               /            404
 * <p>
 * }
 */
public final class ThingsWithCopiedPoliciesIT extends IntegrationTest {

    private AuthClient secondClientForDefaultSolution;

    @Before
    public void setUp() {
        final TestingContext testingContext =
                TestingContext.withGeneratedMockClient(serviceEnv.getTestingContext2().getSolution(), TEST_CONFIG);
        secondClientForDefaultSolution = testingContext.getOAuthClient();
    }
    
    @Test
    public void postThingWithCopiedPolicyFromId() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName("postThingWithCopiedPolicyFromId"));
        final String specialLabel = "postThingWithCopiedPolicyFromId";
        final Policy originalPolicy = policyWithSpecialLabel(policyId, specialLabel);
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, originalPolicy);

        postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        // create thing with copied policy
        final Thing thingWithCopiedPolicy = postThingWithCopiedPolicyFromId(originalPolicy);

        final PolicyId copiedPolicyId = thingWithCopiedPolicy.getPolicyId()
                .orElseThrow(() -> new IllegalStateException("Thing should have a policy"));

        final JsonObject expectedPolicy = originalPolicy.toBuilder().setId(copiedPolicyId).build().toJson();
        getPolicy(copiedPolicyId)
                .expectingBody(containsOnly(expectedPolicy))
                .fire();
    }
    
    @Test
    public void postThingWithCopiedPolicyFromIdFailsIfCreatedThingWouldNotBeAccessible() {
        final PolicyId policyId = PolicyId.of(idGenerator()
                .withPrefixedRandomName("postThingWithCopiedPolicyFromIdFailsIfCreatedThingWouldNotBeAccessible"));
        final String specialLabel = "postThingWithCopiedPolicyFromIdFailsIfCreatedThingWouldNotBeAccessible";
        final Policy policyWithoutUser2Permissions = policyWithSpecialLabel(policyId, specialLabel);
        final Policy policyWithUser2PermissionOnPolicy = addPermissionForPolicyResource(policyWithoutUser2Permissions,
                specialLabel + "-user2",
                secondClientForDefaultSolution.getSubject());
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, policyWithUser2PermissionOnPolicy);

        postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        // create thing with copied policy
        final JsonObject thingWithCopiedPolicyJson =
                thingWithCopiedPolicyFromId(policyWithUser2PermissionOnPolicy.getEntityId()
                        .orElseThrow(() -> new IllegalStateException("Policy should have an ID")));

        postThing(API_V_2, thingWithCopiedPolicyJson)
                .withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingHttpStatus(HttpStatus.FORBIDDEN)
                .expectingErrorCode("things:thing.notmodifiable")
                .fire();
    }
    
    @Test
    public void postThingWithCopiedPolicyFromIdFailsIfPolicyNotAccessible() {
        final PolicyId policyId = PolicyId.of(
                idGenerator().withPrefixedRandomName("postThingWithCopiedPolicyFromIdFailsIfPolicyNotAccessible"));
        final String specialLabel = "postThingWithCopiedPolicyFromIdFailsIfPolicyNotAccessible";
        final Policy originalPolicy = policyWithSpecialLabel(policyId, specialLabel);
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, originalPolicy);

        postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        // create thing with copied policy
        final JsonObject thingWithCopiedPolicyJson = thingWithCopiedPolicyFromId(originalPolicy.getEntityId()
                .orElseThrow(() -> new IllegalStateException("Policy should have an ID")));

        postThing(API_V_2, thingWithCopiedPolicyJson)
                .withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .expectingErrorCode("policies:policy.notfound")
                .fire();
    }
    
    @Test
    @Category(Acceptance.class)
    public void postThingWithCopiedPolicyFromThingRef() {
        final PolicyId policyId =
                PolicyId.of(idGenerator().withPrefixedRandomName("postThingWithCopiedPolicyFromThingRef"));
        final String specialLabel = "postThingWithCopiedPolicyFromThingRef";
        final Policy originalPolicy = policyWithSpecialLabel(policyId, specialLabel);
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, originalPolicy);

        final Thing originalThing = parseThingFromResponse(postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        // create thing with copied policy
        final Thing thingWithCopiedPolicy = postThingWithCopiedPolicyFromThingRef(originalThing);

        final PolicyId copiedPolicyId = thingWithCopiedPolicy.getPolicyId()
                .orElseThrow(() -> new IllegalStateException("Thing should have a policy"));

        final JsonObject expectedPolicy = originalPolicy.toBuilder().setId(copiedPolicyId).build().toJson();
        getPolicy(copiedPolicyId)
                .expectingBody(containsOnly(expectedPolicy))
                .fire();
    }
    
    @Test
    public void postThingWithCopiedPolicyFromThingRefFailsIfPolicyNotAccessible() {
        final PolicyId policyId = PolicyId.of(idGenerator()
                .withPrefixedRandomName("postThingWithCopiedPolicyFromThingRefFailsIfPolicyNotAccessible"));
        final String specialLabel = "postThingWithCopiedPolicyFromThingRefFailsIfPolicyNotAccessible";
        final Policy originalPolicy = policyWithSpecialLabel(policyId, specialLabel);
        final Policy policyWithUser2AccessToThing =
                addPermissionForThingResource(originalPolicy, "user2Label",
                        secondClientForDefaultSolution.getSubject());
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, policyWithUser2AccessToThing);

        final String originalThingId = parseIdFromResponse(postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        // create thing with copied policy
        final JsonObject thingWithCopiedPolicyJson = thingWithCopiedPolicyFromThingRef(originalThingId);

        postThing(API_V_2, thingWithCopiedPolicyJson)
                .withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .expectingErrorCode("policies:policy.notfound")
                .fire();
    }
    
    @Test
    public void postThingWithCopiedPolicyFromRefFailsIfThingNotAccessible() {
        final PolicyId policyId = PolicyId.of(
                idGenerator().withPrefixedRandomName("postThingWithCopiedPolicyFromRefFailsIfThingNotAccessible"));
        final String specialLabel = "postThingWithCopiedPolicyFromRefFailsIfThingNotAccessible";
        final Policy originalPolicy = policyWithSpecialLabel(policyId, specialLabel);
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, originalPolicy);

        final Thing originalThing = parseThingFromResponse(postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        // create thing with copied policy
        final JsonObject thingWithCopiedPolicyJson = thingWithCopiedPolicyFromThingRef(originalThing.getEntityId()
                .orElseThrow(() -> new IllegalStateException("Thing should have an ID")));

        postThing(API_V_2, thingWithCopiedPolicyJson)
                .withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .expectingErrorCode("things:thing.notfound")
                .fire();
    }

    
    @Test
    public void putNewThingWithCopiedPolicyFromId() {
        final PolicyId policyId = PolicyId.of(
                idGenerator().withPrefixedRandomName("putNewThingWithCopiedPolicyFromId"));
        final String specialLabel = "putNewThingWithCopiedPolicyFromId";
        final Policy originalPolicy = policyWithSpecialLabel(policyId, specialLabel);
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, originalPolicy);

        final String originalThingId = parseIdFromResponse(postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        // create thing with copied policy
        final String newThingId = originalThingId.concat("_cp");
        final Thing thingWithCopiedPolicy = putThingWithCopiedPolicyFromId(newThingId, originalPolicy);

        final PolicyId copiedPolicyId = thingWithCopiedPolicy.getPolicyId()
                .orElseThrow(() -> new IllegalStateException("Thing should have a policy"));

        final JsonObject expectedPolicy = originalPolicy.toBuilder().setId(copiedPolicyId).build().toJson();
        getPolicy(copiedPolicyId)
                .expectingBody(containsOnly(expectedPolicy))
                .fire();
    }
    
    @Test
    public void putThingWithCopiedPolicyFromIdFailsIfCreatedThingWouldNotBeAccessible() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName(
                "putThingWithCopiedPolicyFromIdFailsIfCreatedThingWouldNotBeAccessible"));
        final String specialLabel = "putThingWithCopiedPolicyFromIdFailsIfCreatedThingWouldNotBeAccessible";
        final Policy policyWithoutUser2Permissions = policyWithSpecialLabel(policyId, specialLabel);
        final Policy policyWithUser2PermissionOnPolicy = addPermissionForPolicyResource(policyWithoutUser2Permissions,
                specialLabel + "-user2",
                secondClientForDefaultSolution.getSubject());
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, policyWithUser2PermissionOnPolicy);

        final String originalThingId = parseIdFromResponse(postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        // create thing with copied policy
        final String newThingId = originalThingId.concat("_cp");
        final JsonObject thingWithCopiedPolicyJson =
                thingWithCopiedPolicyFromId(policyWithUser2PermissionOnPolicy.getEntityId()
                        .orElseThrow(() -> new IllegalStateException("Policy should have an ID")))
                        .set(Thing.JsonFields.ID, newThingId);

        putThing(API_V_2, thingWithCopiedPolicyJson, JsonSchemaVersion.V_2)
                .withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingHttpStatus(HttpStatus.FORBIDDEN)
                .expectingErrorCode("things:thing.notmodifiable")
                .fire();
    }
    
    @Test
    public void putThingWithCopiedPolicyFromIdFailsIfPolicyNotAccessible() {
        final PolicyId policyId = PolicyId.of(
                idGenerator().withPrefixedRandomName("putThingWithCopiedPolicyFromIdFailsIfPolicyNotAccessible"));
        final String specialLabel = "putThingWithCopiedPolicyFromIdFailsIfPolicyNotAccessible";
        final Policy originalPolicy = policyWithSpecialLabel(policyId, specialLabel);
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, originalPolicy);

        final String originalThingId = parseIdFromResponse(postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        // create thing with copied policy
        final String newThingId = originalThingId.concat("_cp");
        final JsonObject thingWithCopiedPolicyJson = thingWithCopiedPolicyFromId(originalPolicy.getEntityId()
                .orElseThrow(() -> new IllegalStateException("Policy should have an ID")))
                .set(Thing.JsonFields.ID, newThingId);

        putThing(API_V_2, thingWithCopiedPolicyJson, JsonSchemaVersion.V_2)
                .withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .expectingErrorCode("policies:policy.notfound")
                .fire();
    }
    
    @Test
    public void putNewThingWithCopiedPolicyFromThingRef() {
        final PolicyId policyId = PolicyId.of(
                idGenerator().withPrefixedRandomName("putNewThingWithCopiedPolicyFromThingRef"));
        final String specialLabel = "putNewThingWithCopiedPolicyFromThingRef";
        final Policy originalPolicy = policyWithSpecialLabel(policyId, specialLabel);
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, originalPolicy);

        final Thing originalThing = parseThingFromResponse(postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        // create thing with copied policy
        final String newThingId = originalThing.getEntityId()
                .map(id -> id + "_cp")
                .orElseThrow(() -> new IllegalStateException("Thing should have an ID."));
        final Thing thingWithCopiedPolicy = putThingWithCopiedPolicyFromThingRef(newThingId, originalThing);

        final PolicyId copiedPolicyId = thingWithCopiedPolicy.getPolicyId()
                .orElseThrow(() -> new IllegalStateException("Thing should have a policy"));

        final JsonObject expectedPolicy = originalPolicy.toBuilder().setId(copiedPolicyId).build().toJson();
        getPolicy(copiedPolicyId)
                .expectingBody(containsOnly(expectedPolicy))
                .fire();
    }
    
    @Test
    public void putThingWithCopiedPolicyFromThingRefFailsIfPolicyNotAccessible() {
        final PolicyId policyId = PolicyId.of(
                idGenerator().withPrefixedRandomName("putThingWithCopiedPolicyFromThingRefFailsIfPolicyNotAccessible"));
        final String specialLabel = "putThingWithCopiedPolicyFromThingRefFailsIfPolicyNotAccessible";
        final Policy originalPolicy = policyWithSpecialLabel(policyId, specialLabel);
        final Policy policyWithUser2AccessToThing =
                addPermissionForThingResource(originalPolicy, "user2Label",
                        secondClientForDefaultSolution.getSubject());
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, policyWithUser2AccessToThing);

        final String originalThingId = parseIdFromResponse(postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        // create thing with copied policy
        final JsonObject thingWithCopiedPolicyJson = thingWithCopiedPolicyFromThingRef(originalThingId)
                .set(Thing.JsonFields.ID, originalThingId + "_cp");

        putThing(API_V_2, thingWithCopiedPolicyJson, JsonSchemaVersion.V_2)
                .withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .expectingErrorCode("policies:policy.notfound")
                .fire();
    }
    
    @Test
    public void putThingWithCopiedPolicyFromRefFailsIfThingNotAccessible() {
        final PolicyId policyId = PolicyId.of(
                idGenerator().withPrefixedRandomName("putThingWithCopiedPolicyFromRefFailsIfThingNotAccessible"));
        final String specialLabel = "putThingWithCopiedPolicyFromRefFailsIfThingNotAccessible";
        final Policy originalPolicy = policyWithSpecialLabel(policyId, specialLabel);
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, originalPolicy);

        final String originalThingId = parseIdFromResponse(postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        // create thing with copied policy
        final JsonObject thingWithCopiedPolicyJson = thingWithCopiedPolicyFromThingRef(originalThingId)
                .set(Thing.JsonFields.ID, originalThingId + "_cp");

        putThing(API_V_2, thingWithCopiedPolicyJson, JsonSchemaVersion.V_2)
                .withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .expectingErrorCode("things:thing.notfound")
                .fire();
    }

    @Test
    public void putNewThingWithCopiedPolicyAndPolicyIdSetsPolicyId() {
        final PolicyId policyId = PolicyId.of(
                idGenerator().withPrefixedRandomName("putNewThingWithCopiedPolicyAndPolicyIdSetsPolicyId"));
        final String specialLabel = "putNewThingWithCopiedPolicyAndPolicyIdSetsPolicyId";
        final Policy originalPolicy = policyWithSpecialLabel(policyId, specialLabel);
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, originalPolicy);

        final Thing originalThing = parseThingFromResponse(postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        // create thing with copied policy
        final String newThingId = originalThing.getEntityId()
                .map(id -> id + "_cp")
                .orElseThrow(() -> new IllegalStateException("Thing should have an ID."));
        final PolicyId newPolicyId = PolicyId.of(
                idGenerator().withPrefixedRandomName("putNewThingWithCopiedPolicyAndPolicyIdSetsPolicyId"));

        final JsonObject thingToCreate = thingWithCopiedPolicyFromThingRef(originalThing.getEntityId()
                .orElseThrow(() -> new IllegalStateException("Thing should have an ID")))
                .set(Thing.JsonFields.ID, newThingId)
                .set(Thing.JsonFields.POLICY_ID, newPolicyId.toString());

        putThingAndExpectCreated(thingToCreate);

        final JsonObject expectedPolicy = originalPolicy.toBuilder().setId(newPolicyId).build().toJson();
        getPolicy(newPolicyId)
                .expectingBody(containsOnly(expectedPolicy))
                .fire();
    }

    @Test
    public void postNewThingWithCopiedPolicyAndPolicyIdSetsPolicyId() {
        final PolicyId policyId = PolicyId.of(
                idGenerator().withPrefixedRandomName("putNewThingWithCopiedPolicyAndPolicyIdSetsPolicyId"));
        final String specialLabel = "putNewThingWithCopiedPolicyAndPolicyIdSetsPolicyId";
        final Policy originalPolicy = policyWithSpecialLabel(policyId, specialLabel);
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, originalPolicy);

        final Thing originalThing = parseThingFromResponse(postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        // create thing with copied policy

        final PolicyId newPolicyId = PolicyId.of(
                idGenerator().withPrefixedRandomName("putNewThingWithCopiedPolicyAndPolicyIdSetsPolicyId"));
        final JsonObject thingToCreate = thingWithCopiedPolicyFromThingRef(originalThing.getEntityId()
                .orElseThrow(() -> new IllegalStateException("Thing should have an ID")))
                .set(Thing.JsonFields.POLICY_ID, newPolicyId.toString());

        final PolicyId actualPolicyId = postThingAndExpectCreated(thingToCreate).getPolicyId()
                .orElseThrow(() -> new IllegalStateException(""));

        final JsonObject expectedPolicy = originalPolicy.toBuilder().setId(newPolicyId).build().toJson();
        getPolicy(actualPolicyId)
                .expectingBody(containsOnly(expectedPolicy))
                .fire();
    }

    @Test
    public void createThingWithCopiedPolicyAndInlinePolicyReturnsError() {
        final JsonObject thingWithCopiedPolicyAndInlinePolicy = thingWithCopiedPolicyFromId("any:policy")
                .set(CreateThing.JSON_INLINE_POLICY, JsonObject.newBuilder().build());

        postThing(API_V_2, thingWithCopiedPolicyAndInlinePolicy)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("things:policy.conflicting")
                .fire();

        final JsonObject thingWithCopiedPolicyAndInlinePolicyAndThingId = thingWithCopiedPolicyAndInlinePolicy
                .set(Thing.JsonFields.ID, idGenerator().withPrefixedRandomName("otherThing"));

        putThing(API_V_2, thingWithCopiedPolicyAndInlinePolicyAndThingId, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("things:policy.conflicting")
                .fire();
    }

    @Test
    public void createThingWithInvalidCopiedPolicyReferenceReturnsError() {
        final String invalidRef = "{{ ref:topologies/some:id/policyId }}";
        final JsonObject thingJson = JsonFactory.newObjectBuilder()
                .set(CreateThing.JSON_COPY_POLICY_FROM, invalidRef)
                .build();

        postThing(API_V_2, thingJson)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("placeholder:placeholder.reference.notsupported")
                .fire();

        final JsonObject thingJsonWithId = thingJson
                .set(Thing.JsonFields.ID, idGenerator().withPrefixedRandomName("otherThing"));

        putThing(API_V_2, thingJsonWithId, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("placeholder:placeholder.reference.notsupported")
                .fire();
    }
    
    @Test
    public void postThingWithCopiedPolicyFromIdFailsWithOnlyReadAccessOnPolicy() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName("postThingWithCopiedPolicyFromId"));
        final String specialLabel = "postThingWithCopiedPolicyFromId";
        final ResourceKey policyResourceKey = PoliciesResourceType.policyResource("/");
        final Policy policyWithoutUser2Permissions = policyWithSpecialLabel(policyId, specialLabel);
        final Policy policyWithUser2PermissionOnPolicy = addPermissionForPolicyResource(policyWithoutUser2Permissions,
                specialLabel + "-user2",
                serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject());
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, policyWithUser2PermissionOnPolicy);

        final String originalThingId = parseIdFromResponse(postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        putPolicyEntryResource(policyId, specialLabel, policyResourceKey.toString(),
                Resource.newInstance(policyResourceKey,
                        EffectedPermissions.newInstance(Collections.singletonList(Permission.READ),
                                Collections.emptySet())))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // create thing with copied policy with only read permission and special header
        final JsonObject thingWithCopiedPolicyJson = thingWithCopiedPolicyFromThingRef(originalThingId)
                .set(Thing.JsonFields.ID, originalThingId + "_cp");

        putThing(API_V_2, thingWithCopiedPolicyJson, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.FORBIDDEN)
                .expectingErrorCode("things:thing.notcreatable")
                .fire();
    }
    
    @Test
    public void postThingWithCopiedPolicyFromIdAndOnlyReadAccess() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName("postThingWithCopiedPolicyFromId"));
        final String specialLabel = "postThingWithCopiedPolicyFromId";
        final ResourceKey policyResourceKey = PoliciesResourceType.policyResource("/");
        final Resource readResourceToBeUpdated = Resource.newInstance(policyResourceKey,
                EffectedPermissions.newInstance(Collections.singletonList(Permission.READ), Collections.emptySet()));
        final Policy policyWithoutUser2Permissions = policyWithSpecialLabel(policyId, specialLabel);
        final Policy policyWithUser2PermissionOnPolicy = addPermissionForPolicyResource(policyWithoutUser2Permissions,
                specialLabel + "-user2",
                serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject());
        final JsonObject originalThingJson = thingWithInlinePolicy(policyId, policyWithUser2PermissionOnPolicy);

        postThing(API_V_2, originalThingJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        putPolicyEntryResource(policyId, specialLabel, policyResourceKey.toString(), readResourceToBeUpdated)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // create thing with copied policy with only read permission and special header
        final Thing thingWithCopiedPolicy =
                postThingWithCopiedPolicyFromIdAndAllowPolicyLockoutHeader(policyWithUser2PermissionOnPolicy);

        final PolicyId copiedPolicyId = thingWithCopiedPolicy.getPolicyId()
                .orElseThrow(() -> new IllegalStateException("Thing should have a policy"));

        final JsonObject expectedPolicy = policyWithUser2PermissionOnPolicy.toBuilder()
                .setId(copiedPolicyId)
                .setResourceFor(specialLabel, readResourceToBeUpdated)
                .build().toJson();
        getPolicy(copiedPolicyId)
                .expectingBody(containsOnly(expectedPolicy))
                .fire();
    }

    private static Thing parseThingFromResponse(final Response response) {
        return ThingsModelFactory.newThing(response.getBody().asString());
    }

    private static Thing postThingWithCopiedPolicyFromIdAndAllowPolicyLockoutHeader(final Policy originalPolicy) {
        final JsonObject thingWithCopiedPolicyJson = thingWithCopiedPolicyFromId(originalPolicy.getEntityId()
                .orElseThrow(() -> new IllegalStateException("Policy should have an ID")));

        return parseThingFromResponse(postThing(API_V_2, thingWithCopiedPolicyJson)
                .withHeader(HttpHeader.ALLOW_POLICY_LOCKOUT, true)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());
    }

    private static JsonObject thingWithInlinePolicy(final PolicyId policyId, final Policy policy) {
        final JsonObjectBuilder jsonObjectBuilder = JsonFactory.newObjectBuilder();
        jsonObjectBuilder.set(Thing.JsonFields.POLICY_ID, policyId.toString());
        jsonObjectBuilder.setAll(
                policy.toInlinedJson(JsonSchemaVersion.V_2,
                        JsonFactory.newFieldSelector(Policy.JsonFields.ID, Policy.JsonFields.ENTRIES)));
        return jsonObjectBuilder.build();
    }

    private static Thing postThingWithCopiedPolicyFromId(final Policy originalPolicy) {
        final JsonObject thingWithCopiedPolicyJson = thingWithCopiedPolicyFromId(originalPolicy.getEntityId()
                .orElseThrow(() -> new IllegalStateException("Policy should have an ID")));

        return parseThingFromResponse(postThing(API_V_2, thingWithCopiedPolicyJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());
    }

    private static Thing postThingWithCopiedPolicyFromThingRef(final Thing originalThing) {
        final JsonObject thingWithCopiedPolicyJson = thingWithCopiedPolicyFromThingRef(originalThing.getEntityId()
                .orElseThrow(() -> new IllegalStateException("Thing should have an ID")));

        return parseThingFromResponse(postThing(API_V_2, thingWithCopiedPolicyJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());
    }

    private static Thing postThingAndExpectCreated(final JsonObject thingToCreate) {
        return parseThingFromResponse(postThing(API_V_2, thingToCreate)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());
    }

    private static Thing putThingWithCopiedPolicyFromId(final String thingId, final Policy originalPolicy) {
        final JsonObject thingWithCopiedPolicyJson = thingWithCopiedPolicyFromId(originalPolicy.getEntityId()
                .orElseThrow(() -> new IllegalStateException("Policy should have an ID")))
                .set(Thing.JsonFields.ID, thingId);

        return parseThingFromResponse(putThing(API_V_2, thingWithCopiedPolicyJson, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());
    }

    private static Thing putThingWithCopiedPolicyFromThingRef(final CharSequence thingId, final Thing originalThing) {
        final JsonObject thingWithCopiedPolicyJson = thingWithCopiedPolicyFromThingRef(originalThing.getEntityId()
                .orElseThrow(() -> new IllegalStateException("Thing should have an ID")))
                .set(Thing.JsonFields.ID, thingId.toString());
        return putThingAndExpectCreated(thingWithCopiedPolicyJson);
    }

    private static Thing putThingAndExpectCreated(final JsonObject thingWithCopiedPolicyJson) {
        return parseThingFromResponse(putThing(API_V_2, thingWithCopiedPolicyJson, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());
    }

    private static JsonObject thingWithCopiedPolicyFromId(final CharSequence originalPolicyId) {
        final JsonObjectBuilder jsonObjectBuilder = JsonFactory.newObjectBuilder();
        jsonObjectBuilder.set(CreateThing.JSON_COPY_POLICY_FROM, originalPolicyId.toString());
        return jsonObjectBuilder.build();
    }

    private static JsonObject thingWithCopiedPolicyFromThingRef(final CharSequence originalThingId) {
        final String ref = "{{ ref:things/" + originalThingId + "/policyId }}";
        final JsonObjectBuilder jsonObjectBuilder = JsonFactory.newObjectBuilder();
        jsonObjectBuilder.set(CreateThing.JSON_COPY_POLICY_FROM, ref);
        return jsonObjectBuilder.build();
    }

    private static Policy policyWithSpecialLabel(final PolicyId policyId, final String specialLabel) {
        final TestingContext context = serviceEnv.getDefaultTestingContext();
        final Subjects subjects;
        if (context.getBasicAuth().isEnabled()) {
            subjects = Subjects.newInstance(Subject.newInstance(
                    SubjectIssuer.newInstance("nginx"), context.getBasicAuth().getUsername()));
        } else {
            subjects = Subjects.newInstance(context.getOAuthClient().getDefaultSubject());
        }
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel(specialLabel)
                .setSubjects(subjects)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();
    }


    private static Policy addPermissionForPolicyResource(final Policy policy, final String label,
            final Subject subject) {
        final EffectedPermissions permissions =
                EffectedPermissions.newInstance(Arrays.asList(READ, WRITE), Collections.emptyList());
        final Resource policyResource = Resource.newInstance(PoliciesResourceType.policyResource("/"), permissions);
        return policy.setEntry(
                PolicyEntry.newInstance(label,
                        Collections.singletonList(subject),
                        Collections.singletonList(policyResource)
                ));
    }

    private static Policy addPermissionForThingResource(final Policy policy, final String label,
            final Subject subject) {
        final EffectedPermissions permissions =
                EffectedPermissions.newInstance(Arrays.asList(READ, WRITE), Collections.emptyList());
        final Resource policyResource = Resource.newInstance(PoliciesResourceType.thingResource("/"), permissions);
        return policy.setEntry(
                PolicyEntry.newInstance(label,
                        Collections.singletonList(subject),
                        Collections.singletonList(policyResource)
                ));
    }

}
