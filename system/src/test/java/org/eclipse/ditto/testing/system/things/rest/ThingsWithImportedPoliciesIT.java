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

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.model.EffectedImports;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.Permissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.ResourceKey;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.junit.Test;

public final class ThingsWithImportedPoliciesIT extends IntegrationTest {

    @Test
    public void getThingWithNamespaceScopedAuthorizationInImportedPolicy() {
        final PolicyId importedPolicyId =
                PolicyId.of(serviceEnv.getDefaultNamespaceName(), UUID.randomUUID().toString());
        final PolicyId importingPolicyId =
                PolicyId.of(serviceEnv.getDefaultNamespaceName(), UUID.randomUUID().toString());
        final ThingId allowedThingId = ThingId.of("com.acme", UUID.randomUUID().toString());
        final ThingId deniedThingId = ThingId.of("other.ns", UUID.randomUUID().toString());

        final EffectedPermissions ownerPermissions = EffectedPermissions.newInstance(
                PoliciesModelFactory.newPermissions(READ, WRITE), Permissions.none());
        final PolicyEntry ownerEntry = PoliciesModelFactory.newPolicyEntry(
                Label.of("OWNER"),
                PoliciesModelFactory.newSubjects(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject()),
                PoliciesModelFactory.newResources(
                        Resource.newInstance(ResourceKey.newInstance("policy:"), ownerPermissions),
                        Resource.newInstance(ResourceKey.newInstance("thing:"), ownerPermissions)));
        final PolicyEntry scopedReaderEntry = PoliciesModelFactory.newPolicyEntry(
                Label.of("SCOPED_READER"),
                PoliciesModelFactory.newSubjects(serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject()),
                PoliciesModelFactory.newResources(
                        Resource.newInstance(ResourceKey.newInstance("thing:"),
                                EffectedPermissions.newInstance(PoliciesModelFactory.newPermissions(READ),
                                        Permissions.none()))),
                Arrays.asList("com.acme", "com.acme.*"),
                ImportableType.EXPLICIT,
                java.util.Collections.emptySet());
        final Policy importedPolicy = PoliciesModelFactory.newPolicy(importedPolicyId, ownerEntry, scopedReaderEntry);

        putPolicy(importedPolicy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final Policy importingPolicy = Policy.newBuilder(importingPolicyId)
                .forLabel("OWNER")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setPolicyImport(PoliciesModelFactory.newPolicyImport(importedPolicyId,
                        EffectedImports.newInstance(List.of(Label.of("SCOPED_READER")))))
                .build();

        putPolicy(importingPolicy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        putThing(TestConstants.API_V_2,
                Thing.newBuilder().setId(allowedThingId).setPolicyId(importingPolicyId).build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        putThing(TestConstants.API_V_2,
                Thing.newBuilder().setId(deniedThingId).setPolicyId(importingPolicyId).build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getThing(TestConstants.API_V_2, allowedThingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        getThing(TestConstants.API_V_2, deniedThingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

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
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        putPolicyImport(thingId, PoliciesModelFactory.newPolicyImport(policyIdToImport))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
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
                .expectingErrorCode("things:thing.notcreatable")
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
                .expectingErrorCode("things:thing.notcreatable")
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
