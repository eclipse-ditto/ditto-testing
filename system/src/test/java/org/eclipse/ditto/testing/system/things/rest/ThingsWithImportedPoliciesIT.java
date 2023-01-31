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

import static org.eclipse.ditto.things.api.Permission.READ;
import static org.eclipse.ditto.things.api.Permission.WRITE;

import java.util.List;
import java.util.UUID;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.model.EffectedImports;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.junit.Test;

public final class ThingsWithImportedPoliciesIT extends IntegrationTest {

    /**
     * Retrieves a persisted Thing with missing authorization to do so.
     */
    @Test
    public void getThingWithMissingAuthorizationButExistingAuthorizationInImportedPolicy() {
        final String thingId = parseIdFromLocation(postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        final PolicyId policyIdToImport =
                PolicyId.of(serviceEnv.getDefaultNamespaceName(), UUID.randomUUID().toString());
        final Policy policy = Policy.newBuilder(policyIdToImport)
                .forLabel("default")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setImportable(ImportableType.NEVER)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .forLabel("toBeImported")
                .setSubject(serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                .build();

        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        putPolicyImport(thingId, PoliciesModelFactory.newPolicyImport(policyIdToImport))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void createThingWithEmptyButImportingPolicy() {
        final PolicyId policyIdToImport =
                PolicyId.of(serviceEnv.getDefaultNamespaceName(), UUID.randomUUID().toString());

        final Policy policy = Policy.newBuilder(policyIdToImport)
                .forLabel("default")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setImportable(ImportableType.IMPLICIT)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();

        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final JsonObject thingJson = Thing.newBuilder().build().toJson();
        final Policy emptyImportingPolicy =
                Policy.newBuilder().setPolicyImport(PoliciesModelFactory.newPolicyImport(policyIdToImport)).build();
        final JsonObject thingWithInlinePolicy =
                thingJson.set(CreateThing.JSON_INLINE_POLICY, emptyImportingPolicy.toJson());

        final String thingId = parseIdFromLocation(postThing(TestConstants.API_V_2, thingWithInlinePolicy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        getThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void createThingWithEmptyButImportingPolicyWhenImportedPolicyHasDeclaredEntryAsNeverImportable() {
        final PolicyId policyIdToImport =
                PolicyId.of(serviceEnv.getDefaultNamespaceName(), UUID.randomUUID().toString());

        final Policy policy = Policy.newBuilder(policyIdToImport)
                .forLabel("default")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setImportable(ImportableType.NEVER)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();

        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final JsonObject thingJson = Thing.newBuilder().build().toJson();
        final Policy emptyImportingPolicy =
                Policy.newBuilder().setPolicyImport(PoliciesModelFactory.newPolicyImport(policyIdToImport)).build();
        final JsonObject thingWithInlinePolicy =
                thingJson.set(CreateThing.JSON_INLINE_POLICY, emptyImportingPolicy.toJson());

        postThing(TestConstants.API_V_2, thingWithInlinePolicy)
                .expectingHttpStatus(HttpStatus.FORBIDDEN)
                .expectingErrorCode("policies:policy.notcreatable")
                .fire();
    }

    @Test
    public void createThingWithEmptyButImportingPolicyWhenImportedPolicyHasDeclaredEntryAsExplicitImportable() {
        final PolicyId policyIdToImport =
                PolicyId.of(serviceEnv.getDefaultNamespaceName(), UUID.randomUUID().toString());

        final Policy policy = Policy.newBuilder(policyIdToImport)
                .forLabel("default")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setImportable(ImportableType.EXPLICIT)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();

        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final JsonObject thingJson = Thing.newBuilder().build().toJson();
        final Policy emptyImportingPolicy =
                Policy.newBuilder().setPolicyImport(PoliciesModelFactory.newPolicyImport(policyIdToImport)).build();
        final JsonObject thingWithInlinePolicy =
                thingJson.set(CreateThing.JSON_INLINE_POLICY, emptyImportingPolicy.toJson());

        postThing(TestConstants.API_V_2, thingWithInlinePolicy)
                .expectingHttpStatus(HttpStatus.FORBIDDEN)
                .expectingErrorCode("policies:policy.notcreatable")
                .fire();
    }

    @Test
    public void createThingWithEmptyButExplicitlyImportingPolicyWhenImportedPolicyHasDeclaredEntryAsExplicitImportable() {
        final PolicyId policyIdToImport =
                PolicyId.of(serviceEnv.getDefaultNamespaceName(), UUID.randomUUID().toString());

        final Policy policy = Policy.newBuilder(policyIdToImport)
                .forLabel("default")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setImportable(ImportableType.EXPLICIT)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();

        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final JsonObject thingJson = Thing.newBuilder().build().toJson();
        final Policy emptyImportingPolicy =
                Policy.newBuilder()
                        .setPolicyImport(PoliciesModelFactory.newPolicyImport(policyIdToImport,
                                EffectedImports.newInstance(List.of(Label.of("default")))))
                        .build();
        final JsonObject thingWithInlinePolicy =
                thingJson.set(CreateThing.JSON_INLINE_POLICY, emptyImportingPolicy.toJson());

        final String thingId = parseIdFromLocation(postThing(TestConstants.API_V_2, thingWithInlinePolicy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        getThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void getThingWithEmptyButImportingPolicyWhenImportedPolicyHasDeclaredEntryAsNeverImportable() {
        final PolicyId policyIdToImport =
                PolicyId.of(serviceEnv.getDefaultNamespaceName(), UUID.randomUUID().toString());

        final Policy policy = Policy.newBuilder(policyIdToImport)
                .forLabel("default")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setImportable(ImportableType.IMPLICIT)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();

        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final JsonObject thingJson = Thing.newBuilder().build().toJson();
        final Policy emptyImportingPolicy =
                Policy.newBuilder().setPolicyImport(PoliciesModelFactory.newPolicyImport(policyIdToImport)).build();
        final JsonObject thingWithInlinePolicy =
                thingJson.set(CreateThing.JSON_INLINE_POLICY, emptyImportingPolicy.toJson());

        final String thingId = parseIdFromLocation(postThing(TestConstants.API_V_2, thingWithInlinePolicy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        getThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final Policy policyWithExplicitImportable = Policy.newBuilder(policyIdToImport)
                .forLabel("default")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setImportable(ImportableType.NEVER)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();

        putPolicy(policyWithExplicitImportable)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void getThingWithEmptyButImportingPolicyWhenImportedPolicyHasDeclaredEntryAsExplicitImportable() {
        final PolicyId policyIdToImport =
                PolicyId.of(serviceEnv.getDefaultNamespaceName(), UUID.randomUUID().toString());

        final Policy policy = Policy.newBuilder(policyIdToImport)
                .forLabel("default")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setImportable(ImportableType.IMPLICIT)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();

        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final JsonObject thingJson = Thing.newBuilder().build().toJson();
        final Policy emptyImportingPolicy =
                Policy.newBuilder().setPolicyImport(PoliciesModelFactory.newPolicyImport(policyIdToImport)).build();
        final JsonObject thingWithInlinePolicy =
                thingJson.set(CreateThing.JSON_INLINE_POLICY, emptyImportingPolicy.toJson());

        final String thingId = parseIdFromLocation(postThing(TestConstants.API_V_2, thingWithInlinePolicy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        getThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final Policy policyWithExplicitImportable = Policy.newBuilder(policyIdToImport)
                .forLabel("default")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setImportable(ImportableType.EXPLICIT)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();

        putPolicy(policyWithExplicitImportable)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void getThingWithEmptyButExplicitlyImportingPolicyWhenImportedPolicyHasDeclaredEntryAsExplicitImportable() {
        final PolicyId policyIdToImport =
                PolicyId.of(serviceEnv.getDefaultNamespaceName(), UUID.randomUUID().toString());

        final Policy policy = Policy.newBuilder(policyIdToImport)
                .forLabel("default")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setImportable(ImportableType.IMPLICIT)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();

        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final JsonObject thingJson = Thing.newBuilder().build().toJson();
        final Policy emptyImportingPolicy =
                Policy.newBuilder()
                        .setPolicyImport(PoliciesModelFactory.newPolicyImport(policyIdToImport,
                                EffectedImports.newInstance(List.of(Label.of("default")))))
                        .build();
        final JsonObject thingWithInlinePolicy =
                thingJson.set(CreateThing.JSON_INLINE_POLICY, emptyImportingPolicy.toJson());

        final String thingId = parseIdFromLocation(postThing(TestConstants.API_V_2, thingWithInlinePolicy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location"));

        getThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final Policy policyWithExplicitImportable = Policy.newBuilder(policyIdToImport)
                .forLabel("default")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setImportable(ImportableType.EXPLICIT)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .build();

        putPolicy(policyWithExplicitImportable)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

}
