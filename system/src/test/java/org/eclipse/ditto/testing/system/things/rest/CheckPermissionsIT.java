/*
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
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

import java.util.List;
import java.util.Set;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.api.Permission;
import org.eclipse.ditto.policies.model.EffectedImports;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.client.BasicAuth;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.BeforeClass;
import org.junit.Test;


public final class CheckPermissionsIT extends IntegrationTest {

    private static ThingId testThingId;
    private static Policy testPolicy;

    private static ThingId importOnlyThingId;
    private static PolicyId importedPolicyId;

    @BeforeClass
    public static void setup() {
        testThingId = ThingId.of(idGenerator().withRandomName());
        testPolicy = createPolicyForCheckPermissions(testThingId);
        putThingWithPolicy(TestConstants.API_V_2, newThing(testThingId), testPolicy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        setupImportedPolicyScenario();
    }

    /**
     * Creates a pair of policies: one with explicit importable entries, and a second that imports from it.
     * The importing policy has no local thing/message grants — all access comes via the import.
     */
    private static void setupImportedPolicyScenario() {
        final Subjects subjects = defaultSubjects();

        importedPolicyId = PolicyId.of(idGenerator().withRandomName());
        final Policy importedPolicy = Policy.newBuilder(importedPolicyId)
                .forLabel("OWNER")
                .setSubjects(subjects)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), Permission.READ, Permission.WRITE)
                .forLabel("OPERATOR")
                .setSubjects(subjects)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), Permission.READ, Permission.WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), Permission.WRITE)
                .setImportable(ImportableType.EXPLICIT)
                .build();

        putPolicy(importedPolicy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        importOnlyThingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId importingPolicyId = PolicyId.of(importOnlyThingId);
        final Policy importingPolicy = Policy.newBuilder(importingPolicyId)
                .forLabel("SELF")
                .setSubjects(subjects)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), Permission.READ, Permission.WRITE)
                .setPolicyImport(PoliciesModelFactory.newPolicyImport(importedPolicyId,
                        EffectedImports.newInstance(List.of(Label.of("OPERATOR")))))
                .build();

        putPolicy(importingPolicy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        putThing(TestConstants.API_V_2,
                Thing.newBuilder().setId(importOnlyThingId).setPolicyId(importingPolicyId).build(),
                JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
    }

    // --- Existing tests (local-only policies) ---

    @Test
    public void checkPermissionsFullAccessCase() {
        final JsonObject requestBody = buildPermissionsRequest(
                buildResourceEntry("thing_writer", "thing:/features/lamp/properties/on", testThingId.toString(), Set.of("WRITE")),
                buildResourceEntry("message_writer", "message:/features/lamp/inbox/messages/toggle", testThingId.toString(), Set.of("WRITE")),
                buildResourceEntry("policy_reader", "policy:/", testThingId.toString(), Set.of("READ"))
        );

        postCheckPermissions(requestBody.toString())
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(JsonObject.newBuilder()
                        .set("thing_writer", true)
                        .set("message_writer", true)
                        .set("policy_reader", true)
                        .build()))
                .fire();
    }

    @Test
    public void checkPermissionsRestrictedAccessCase() {
        final JsonObject requestBody = buildPermissionsRequest(
                buildResourceEntry("thing_reader", "thing:/features/fan/properties/on", testThingId.toString(), Set.of("READ")),
                buildResourceEntry("message_reader", "message:/features/lamp/inbox/messages/toggle", testThingId.toString(), Set.of("READ")),
                buildResourceEntry("policy_writer", "policy:/", testThingId.toString(), Set.of("WRITE"))
        );

        postCheckPermissions(requestBody.toString())
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(JsonObject.newBuilder()
                        .set("thing_reader", false)
                        .set("message_reader", false)
                        .set("policy_writer", true)
                        .build()))
                .fire();
    }


    @Test
    public void checkPermissionsWithNonexistentPolicyOrThing() {
        final JsonObject nonexistentRequestBody = buildPermissionsRequest(
                buildResourceEntry("nonexistent_policy", "policy:/", "namespace.default:nonexistent-policy-id",
                        Set.of("READ")),
                buildResourceEntry("nonexistent_thing", "thing:/features/lamp/properties/on",
                        "namespace.default:nonexistent-thing-id", Set.of("READ"))
        );

        postCheckPermissions(nonexistentRequestBody.toString())
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(JsonObject.newBuilder()
                        .set("nonexistent_policy", false)
                        .set("nonexistent_thing", false)
                        .build()))
                .fire();
    }

    @Test
    public void checkPermissionsWithInvalidPolicyIdReturnsBadRequest() {
        final JsonObject invalidRequestBody = buildPermissionsRequest(
                buildResourceEntry("invalid_policy", "policy:/", "nonexistent-policy-id", Set.of("READ"))
        );

        postCheckPermissions(invalidRequestBody.toString())
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    @Test
    public void checkPermissionsWithInvalidThingIdReturnsBadRequest() {
        final JsonObject invalidRequestBody = buildPermissionsRequest(
                buildResourceEntry("invalid_thing", "thing:/", "nonexistent-thing-id", Set.of("READ"))
        );

        postCheckPermissions(invalidRequestBody.toString())
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    @Test
    public void checkPermissionsWithUnsupportedResourceTypeReturnsBadRequest() {
        final JsonObject invalidRequestBody = buildPermissionsRequest(
                buildResourceEntry("invalid_resource", "foo:/bar", "namespace.default:thing", Set.of("READ"))
        );

        postCheckPermissions(invalidRequestBody.toString())
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    @Test
    public void checkPermissionsHonorsImportedThingGrant() {
        final JsonObject requestBody = buildPermissionsRequest(
                buildResourceEntry("thing_read", "thing:/", importOnlyThingId.toString(), Set.of("READ")),
                buildResourceEntry("thing_write", "thing:/", importOnlyThingId.toString(), Set.of("WRITE"))
        );

        postCheckPermissions(requestBody.toString())
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(JsonObject.newBuilder()
                        .set("thing_read", true)
                        .set("thing_write", true)
                        .build()))
                .fire();
    }

    @Test
    public void checkPermissionsHonorsImportedMessageGrant() {
        final JsonObject requestBody = buildPermissionsRequest(
                buildResourceEntry("toggle_write", "message:/features/lamp/inbox/messages/toggle",
                        importOnlyThingId.toString(), Set.of("WRITE"))
        );

        postCheckPermissions(requestBody.toString())
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(JsonObject.newBuilder()
                        .set("toggle_write", true)
                        .build()))
                .fire();
    }

    @Test
    public void checkPermissionsRejectsUngrantedPermissionOnImportOnlyPolicy() {
        final JsonObject requestBody = buildPermissionsRequest(
                buildResourceEntry("message_read", "message:/features/lamp/inbox/messages/toggle",
                        importOnlyThingId.toString(), Set.of("READ"))
        );

        postCheckPermissions(requestBody.toString())
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(JsonObject.newBuilder()
                        .set("message_read", false)
                        .build()))
                .fire();
    }

    @Test
    public void checkPermissionsMixedLocalAndImportedGrants() {
        final JsonObject requestBody = buildPermissionsRequest(
                // policy:/ is a local grant in the importing policy
                buildResourceEntry("policy_read", "policy:/", importOnlyThingId.toString(), Set.of("READ")),
                // thing:/ comes from the imported OPERATOR entry
                buildResourceEntry("thing_read", "thing:/", importOnlyThingId.toString(), Set.of("READ")),
                // message WRITE comes from the imported OPERATOR entry
                buildResourceEntry("message_write", "message:/", importOnlyThingId.toString(), Set.of("WRITE")),
                // message READ was never granted anywhere
                buildResourceEntry("message_read", "message:/", importOnlyThingId.toString(), Set.of("READ"))
        );

        postCheckPermissions(requestBody.toString())
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(JsonObject.newBuilder()
                        .set("policy_read", true)
                        .set("thing_read", true)
                        .set("message_write", true)
                        .set("message_read", false)
                        .build()))
                .fire();
    }


    private static Thing newThing(final ThingId thingId) {
        return Thing.newBuilder().setId(thingId).build();
    }

    private static Subjects defaultSubjects() {
        final BasicAuth basicAuth = serviceEnv.getDefaultTestingContext().getBasicAuth();
        if (basicAuth.isEnabled()) {
            return Subjects.newInstance(Subject.newInstance(
                    SubjectIssuer.newInstance("nginx"), basicAuth.getUsername()));
        } else {
            return Subjects.newInstance(
                    serviceEnv.getDefaultTestingContext().getOAuthClient().getSubject(),
                    serviceEnv.getTestingContext2().getOAuthClient().getSubject());
        }
    }

    private static Policy createPolicyForCheckPermissions(final ThingId thingId) {
        return Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubjects(defaultSubjects())
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), Permission.WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), Permission.WRITE, Permission.READ)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), Permission.WRITE)
                .build();
    }

    private static JsonObject buildPermissionsRequest(final JsonObject... entries) {
        final var builder = JsonObject.newBuilder();
        for (var entry : entries) {
            entry.forEach(builder::set);
        }
        return builder.build();
    }

    private static JsonObject buildResourceEntry(final String entryName, final String resource, final String entityId, final Set<String> permissions) {
        return JsonObject.newBuilder()
                .set(entryName, JsonObject.newBuilder()
                        .set("resource", resource)
                        .set("entityId", entityId)
                        .set("hasPermissions", JsonFactory.newArrayBuilder()
                                .addAll(permissions.stream().map(JsonFactory::newValue).toList())
                                .build())
                        .build())
                .build();
    }
}
