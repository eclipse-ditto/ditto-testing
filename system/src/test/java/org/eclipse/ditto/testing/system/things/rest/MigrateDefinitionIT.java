/*
 * Copyright (c) 2025 Contributors to the Eclipse Foundation
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
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
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
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.eclipse.ditto.policies.model.Subjects;

public final class MigrateDefinitionIT extends IntegrationTest {

    private static ThingId testThingId;
    private static Policy testPolicy;
    private static final String THING_DEFINITION_URL = "https://eclipse-ditto.github.io/ditto-examples/wot/models/dimmable-colored-lamp-1.0.0.tm.jsonld";

    @BeforeClass
    public static void setup() {
        testThingId = ThingId.of(idGenerator().withRandomName());
        testPolicy = createPolicyForMigration();

        putThingWithPolicy(TestConstants.API_V_2, newThing(testThingId), testPolicy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
    }

    @Test
    public void testMigrateDefinitionDryRun() {
        final JsonObject migrationPayload = buildMigrationPayload();

        postMigrateDefinition(testThingId, migrationPayload.toString(), true)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(buildExpectedResponse(testThingId, true)))
                .fire();
    }

    @Test
    public void testMigrateDefinitionSuccess() {
        final JsonObject migrationPayload = buildMigrationPayload();

        postMigrateDefinition(testThingId, migrationPayload.toString(), false)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        getDefinition(TestConstants.API_V_2, testThingId.toString())
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(buildExpectedResponse(testThingId, false)))
                .fire();
    }

    @Test
    public void testMigrateDefinitionWithInvalidThingId() {
        final JsonObject migrationPayload = buildMigrationPayload();

        postMigrateDefinition(serviceEnv.getDefaultNamespaceName() + ":unknownThingId", migrationPayload.toString(), false)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void testMigrateDefinitionWithInvalidPayload() {
        final JsonObject invalidPayload = JsonFactory.newObjectBuilder()
                .set("invalid_field", "some_value")
                .build();

        postMigrateDefinition(testThingId, invalidPayload.toString(), false)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    private static Thing newThing(final ThingId thingId) {
        return ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonPointer.of("manufacturer"), JsonValue.of("Old Corp"))
                .build();
    }

    private static JsonObject buildMigrationPayload() {
        return JsonFactory.newObjectBuilder()
                .set("thingDefinitionUrl", THING_DEFINITION_URL)
                .set("migrationPayload", JsonFactory.newObjectBuilder()
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("manufacturer", JsonFactory.nullLiteral())
                                .set("color", JsonFactory.newObjectBuilder()
                                        .set("g", 5)
                                        .build())
                                .set("dimmer-level", 1.0)
                                .build())
                        .build())
                .set("initializeMissingPropertiesFromDefaults", true)
                .build();
    }

    private static JsonObject buildExpectedResponse(ThingId thingId, boolean dryRun) {
        return JsonFactory.newObjectBuilder()
                .set("thingId", thingId.toString())
                .set("patch", JsonFactory.newObjectBuilder()
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("on", false)
                                .set("color", JsonFactory.newObjectBuilder()
                                        .set("r", 0)
                                        .set("g", 5)
                                        .set("b", 0)
                                        .build())
                                .set("dimmer-level", 1.0)
                                .build())
                        .build())
                .set("mergeStatus", dryRun ? "DRY_RUN" : "APPLIED")
                .build();
    }

    private static Policy createPolicyForMigration() {
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
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), Permission.WRITE, Permission.READ)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), Permission.WRITE, Permission.READ)
                .build();
    }
}
