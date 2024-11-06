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

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.api.Permission;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.client.BasicAuth;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.BeforeClass;
import org.junit.Test;
import org.eclipse.ditto.policies.model.Subjects;

import java.util.Set;


public final class CheckPermissionsIT extends IntegrationTest {

    private static ThingId testThingId;
    private static Policy testPolicy;

    @BeforeClass
    public static void setup() {
        testThingId = ThingId.of(idGenerator().withRandomName());
        testPolicy = createPolicyForCheckPermissions(testThingId);
        putThingWithPolicy(TestConstants.API_V_2, newThing(testThingId), testPolicy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
    }

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
                buildResourceEntry("nonexistent_policy", "policy:/", "nonexistent-policy-id", Set.of("READ")),
                buildResourceEntry("nonexistent_thing", "thing:/features/lamp/properties/on", "nonexistent-thing-id", Set.of("READ"))
        );

        postCheckPermissions(nonexistentRequestBody.toString())
                .expectingHttpStatus(HttpStatus.INTERNAL_SERVER_ERROR)
                .fire();
    }

    private static Thing newThing(final ThingId thingId) {
        return Thing.newBuilder().setId(thingId).build();
    }

    private static Policy createPolicyForCheckPermissions(final ThingId thingId) {
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
        return Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubjects(subjects)
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
