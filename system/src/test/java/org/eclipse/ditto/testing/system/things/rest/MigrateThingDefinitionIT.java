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
import org.eclipse.ditto.things.model.signals.commands.ThingCommand;
import org.eclipse.ditto.things.model.signals.commands.modify.MigrateThingDefinition;
import org.eclipse.ditto.things.model.signals.commands.modify.MigrateThingDefinitionResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import org.eclipse.ditto.policies.model.Subjects;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class MigrateThingDefinitionIT extends IntegrationTest {

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
    public void test1_MigrateDefinitionDryRun() {
        final JsonObject migrationPayload = buildMigrationPayload();

        postMigrateDefinition(testThingId, migrationPayload.toString(), true)
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                .expectingBody(contains(buildExpectedResponse(testThingId, true)))
                .fire();
    }

    @Test
    public void test2_MigrateDefinitionSuccess() {
        final JsonObject migrationPayload = buildMigrationPayload();

        postMigrateDefinition(testThingId, migrationPayload.toString(), false)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(buildExpectedResponse(testThingId, false)))
                .fire();
    }

    @Test
    public void test3_MigrateDefinitionResolvesThingJsonPlaceholdersDryRun() {
        final ThingId placeholderThingId = ThingId.of(idGenerator().withRandomName());
        putThingWithPolicy(TestConstants.API_V_2, newThingWithPlaceholders(placeholderThingId), testPolicy,
                JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final JsonObject migrationPayload = JsonObject.newBuilder()
                .set(MigrateThingDefinition.JsonFields.JSON_THING_DEFINITION_URL, THING_DEFINITION_URL)
                .set(MigrateThingDefinition.JsonFields.JSON_MIGRATION_PAYLOAD, JsonFactory.newObjectBuilder()
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("on", "{{ thing-json:attributes/on }}")
                                .set("color", JsonFactory.newObjectBuilder()
                                        .set("r", "{{ thing-json:attributes/color/g }}")
                                        .set("g", "{{ thing-json:attributes/color/b }}")
                                        .set("b", "{{ thing-json:attributes/color/r }}")
                                        .build())
                                .set("dimmer-level", "{{ thing-json:attributes/dimmer-level }}")
                                .build())
                        .build())
                .set(MigrateThingDefinition.JsonFields.JSON_INITIALIZE_MISSING_PROPERTIES_FROM_DEFAULTS, true)
                .build();

        final JsonObject expectedResponse = JsonObject.newBuilder()
                .set(ThingCommand.JsonFields.JSON_THING_ID, placeholderThingId.toString())
                .set(MigrateThingDefinitionResponse.JsonFields.JSON_PATCH, JsonFactory.newObjectBuilder()
                        .set("definition", THING_DEFINITION_URL)
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("on", true)
                                .set("color", JsonFactory.newObjectBuilder()
                                        .set("r", 2)
                                        .set("g", 3)
                                        .set("b", 1)
                                        .build())
                                .set("dimmer-level", 0.5)
                                .build())
                        .build())
                .set(MigrateThingDefinitionResponse.JsonFields.JSON_MERGE_STATUS, "DRY_RUN")
                .build();

        postMigrateDefinition(placeholderThingId, migrationPayload.toString(), true)
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                .expectingBody(contains(expectedResponse))
                .fire();
    }

    @Test
    public void test3a_MigrateDefinitionFailsOnUnresolvedThingJsonPlaceholder() {
        final ThingId placeholderThingId = ThingId.of(idGenerator().withRandomName());
        putThingWithPolicy(TestConstants.API_V_2, newThingWithPlaceholders(placeholderThingId), testPolicy,
                JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final JsonObject migrationPayload = JsonObject.newBuilder()
                .set(MigrateThingDefinition.JsonFields.JSON_THING_DEFINITION_URL, THING_DEFINITION_URL)
                .set(MigrateThingDefinition.JsonFields.JSON_MIGRATION_PAYLOAD, JsonFactory.newObjectBuilder()
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("on", "{{ thing-json:attributes/does-not-exist }}")
                                .build())
                        .build())
                .build();

        postMigrateDefinition(placeholderThingId, migrationPayload.toString(), true)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(contains(JsonObject.newBuilder()
                        .set("error", "placeholder:unresolved")
                        .build()))
                .fire();
    }

    @Test
    public void test3b_MigrateDefinitionCopiesThenDeletesSourceFieldDryRun() {
        final ThingId placeholderThingId = ThingId.of(idGenerator().withRandomName());
        putThingWithPolicy(TestConstants.API_V_2, newThingWithOptionalWhite(placeholderThingId), testPolicy,
                JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final JsonObject migrationPayload = JsonObject.newBuilder()
                .set(MigrateThingDefinition.JsonFields.JSON_THING_DEFINITION_URL, THING_DEFINITION_URL)
                .set(MigrateThingDefinition.JsonFields.JSON_MIGRATION_PAYLOAD, JsonFactory.newObjectBuilder()
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("on", true)
                                .set("color", JsonFactory.newObjectBuilder()
                                        .set("r", "{{ thing-json:attributes/color/w }}")
                                        .set("w", JsonFactory.nullLiteral())
                                        .build())
                                .set("dimmer-level", "{{ thing-json:attributes/dimmer-level }}")
                                .build())
                        .build())
                .set(MigrateThingDefinition.JsonFields.JSON_INITIALIZE_MISSING_PROPERTIES_FROM_DEFAULTS, true)
                .build();

        postMigrateDefinition(placeholderThingId, migrationPayload.toString(), true)
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                .expectingBody(contains(JsonObject.newBuilder()
                        .set(ThingCommand.JsonFields.JSON_THING_ID, placeholderThingId.toString())
                        .set(MigrateThingDefinitionResponse.JsonFields.JSON_PATCH, JsonFactory.newObjectBuilder()
                                .set("definition", THING_DEFINITION_URL)
                                .set("attributes", JsonFactory.newObjectBuilder()
                                        .set("on", true)
                                        .set("color", JsonFactory.newObjectBuilder()
                                                .set("r", 7)
                                                .set("w", JsonFactory.nullLiteral())
                                                .build())
                                        .set("dimmer-level", 0.7)
                                        .build())
                                .build())
                        .set(MigrateThingDefinitionResponse.JsonFields.JSON_MERGE_STATUS, "DRY_RUN")
                        .build()))
                .fire();
    }

    @Test
    public void test3c_MigrateDefinitionConsolidatesColorChannelsDryRun() {
        final ThingId placeholderThingId = ThingId.of(idGenerator().withRandomName());
        putThingWithPolicy(TestConstants.API_V_2, newThingWithPlaceholders(placeholderThingId), testPolicy,
                JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final JsonObject migrationPayload = JsonObject.newBuilder()
                .set(MigrateThingDefinition.JsonFields.JSON_THING_DEFINITION_URL, THING_DEFINITION_URL)
                .set(MigrateThingDefinition.JsonFields.JSON_MIGRATION_PAYLOAD, JsonFactory.newObjectBuilder()
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("color", JsonFactory.newObjectBuilder()
                                        .set("r", "{{ thing-json:attributes/color/r }}")
                                        .set("g", "{{ thing-json:attributes/color/g }}")
                                        .set("b", "{{ thing-json:attributes/color/b }}")
                                        .build())
                                .build())
                        .build())
                .set(MigrateThingDefinition.JsonFields.JSON_INITIALIZE_MISSING_PROPERTIES_FROM_DEFAULTS, true)
                .build();

        postMigrateDefinition(placeholderThingId, migrationPayload.toString(), true)
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                .expectingBody(contains(JsonObject.newBuilder()
                        .set(ThingCommand.JsonFields.JSON_THING_ID, placeholderThingId.toString())
                        .set(MigrateThingDefinitionResponse.JsonFields.JSON_PATCH, JsonFactory.newObjectBuilder()
                                .set("definition", THING_DEFINITION_URL)
                                .set("attributes", JsonFactory.newObjectBuilder()
                                        .set("color", JsonFactory.newObjectBuilder()
                                                .set("r", 1)
                                                .set("g", 2)
                                                .set("b", 3)
                                                .build())
                                        .build())
                                .build())
                        .set(MigrateThingDefinitionResponse.JsonFields.JSON_MERGE_STATUS, "DRY_RUN")
                        .build()))
                .fire();
    }

    @Test
    public void test3d_MigrateDefinitionConditionalPlaceholderDryRun() {
        final ThingId placeholderThingId = ThingId.of(idGenerator().withRandomName());
        putThingWithPolicy(TestConstants.API_V_2, newThingWithOptionalWhite(placeholderThingId), testPolicy,
                JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final JsonObject migrationPayload = JsonObject.newBuilder()
                .set(MigrateThingDefinition.JsonFields.JSON_THING_DEFINITION_URL, THING_DEFINITION_URL)
                .set(MigrateThingDefinition.JsonFields.JSON_MIGRATION_PAYLOAD, JsonFactory.newObjectBuilder()
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("dimmer-level", "{{ thing-json:attributes/dimmer-level }}")
                                .build())
                        .build())
                .set(MigrateThingDefinition.JsonFields.JSON_PATCH_CONDITIONS, JsonObject.newBuilder()
                        .set("thing:/attributes/dimmer-level", "eq(attributes/dimmer-level,0.7)")
                        .build())
                .set(MigrateThingDefinition.JsonFields.JSON_INITIALIZE_MISSING_PROPERTIES_FROM_DEFAULTS, true)
                .build();

        postMigrateDefinition(placeholderThingId, migrationPayload.toString(), true)
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                .expectingBody(contains(JsonObject.newBuilder()
                        .set(ThingCommand.JsonFields.JSON_THING_ID, placeholderThingId.toString())
                        .set(MigrateThingDefinitionResponse.JsonFields.JSON_PATCH, JsonFactory.newObjectBuilder()
                                .set("definition", THING_DEFINITION_URL)
                                .set("attributes", JsonFactory.newObjectBuilder()
                                        .set("dimmer-level", 0.7)
                                        .build())
                                .build())
                        .set(MigrateThingDefinitionResponse.JsonFields.JSON_MERGE_STATUS, "DRY_RUN")
                        .build()))
                .fire();
    }

    @Test
    public void test3e_MigrateDefinitionLegacyFormatTypePreservationDryRun() {
        final ThingId placeholderThingId = ThingId.of(idGenerator().withRandomName());
        putThingWithPolicy(TestConstants.API_V_2, newThingWithPlaceholders(placeholderThingId), testPolicy,
                JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final JsonObject migrationPayload = JsonObject.newBuilder()
                .set(MigrateThingDefinition.JsonFields.JSON_THING_DEFINITION_URL, THING_DEFINITION_URL)
                .set(MigrateThingDefinition.JsonFields.JSON_MIGRATION_PAYLOAD, JsonFactory.newObjectBuilder()
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("on", "${ thing-json:attributes/on }")
                                .set("dimmer-level", "${ thing-json:attributes/dimmer-level }")
                                .build())
                        .build())
                .set(MigrateThingDefinition.JsonFields.JSON_INITIALIZE_MISSING_PROPERTIES_FROM_DEFAULTS, true)
                .build();

        final JsonObject expectedResponse = JsonObject.newBuilder()
                .set(ThingCommand.JsonFields.JSON_THING_ID, placeholderThingId.toString())
                .set(MigrateThingDefinitionResponse.JsonFields.JSON_PATCH, JsonFactory.newObjectBuilder()
                        .set("definition", THING_DEFINITION_URL)
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("on", true)
                                .set("dimmer-level", 0.5)
                                .build())
                        .build())
                .set(MigrateThingDefinitionResponse.JsonFields.JSON_MERGE_STATUS, "DRY_RUN")
                .build();

        postMigrateDefinition(placeholderThingId, migrationPayload.toString(), true)
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                .expectingBody(contains(expectedResponse))
                .fire();
    }

    @Test
    public void test3f_MigrateDefinitionWithPlaceholdersApplyThenGetThing() {
        final ThingId placeholderThingId = ThingId.of(idGenerator().withRandomName());
        putThingWithPolicy(TestConstants.API_V_2, newThingWithPlaceholders(placeholderThingId), testPolicy,
                JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final JsonObject migrationPayload = JsonObject.newBuilder()
                .set(MigrateThingDefinition.JsonFields.JSON_THING_DEFINITION_URL, THING_DEFINITION_URL)
                .set(MigrateThingDefinition.JsonFields.JSON_MIGRATION_PAYLOAD, JsonFactory.newObjectBuilder()
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("on", "{{ thing-json:attributes/on }}")
                                .set("color", JsonFactory.newObjectBuilder()
                                        .set("r", "{{ thing-json:attributes/color/r }}")
                                        .set("g", "{{ thing-json:attributes/color/g }}")
                                        .set("b", "{{ thing-json:attributes/color/b }}")
                                        .build())
                                .set("dimmer-level", "{{ thing-json:attributes/dimmer-level }}")
                                .build())
                        .build())
                .set(MigrateThingDefinition.JsonFields.JSON_INITIALIZE_MISSING_PROPERTIES_FROM_DEFAULTS, true)
                .build();

        postMigrateDefinition(placeholderThingId, migrationPayload.toString(), false)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(JsonObject.newBuilder()
                        .set(ThingCommand.JsonFields.JSON_THING_ID, placeholderThingId.toString())
                        .set(MigrateThingDefinitionResponse.JsonFields.JSON_MERGE_STATUS, "APPLIED")
                        .build()))
                .fire();

        getThing(TestConstants.API_V_2, placeholderThingId)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(JsonObject.newBuilder()
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("on", true)
                                .set("color", JsonFactory.newObjectBuilder()
                                        .set("r", 1)
                                        .set("g", 2)
                                        .set("b", 3)
                                        .build())
                                .set("dimmer-level", 0.5)
                                .build())
                        .build()))
                .fire();
    }


    @Test
    public void test4_MigrateDefinitionWithWotValidationErrorReturnsBadRequest() {
        final JsonObject migrationPayload = JsonObject.newBuilder()
                .set(MigrateThingDefinition.JsonFields.JSON_THING_DEFINITION_URL, THING_DEFINITION_URL)
                .set(MigrateThingDefinition.JsonFields.JSON_MIGRATION_PAYLOAD, JsonFactory.newObjectBuilder()
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("dimmer-level", JsonFactory.nullLiteral())
                                .build())
                        .build())
                .build();

        postMigrateDefinition(testThingId, migrationPayload.toString(), false)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(contains(JsonObject.newBuilder().set("error", "wot:payload.validation.error").build()))
                .fire();
    }

    @Test
    public void test5_MigrateDefinitionWithPatchConditionsFilteringPayload() {
        final JsonObject migrationPayload = JsonObject.newBuilder()
                .set(MigrateThingDefinition.JsonFields.JSON_THING_DEFINITION_URL, THING_DEFINITION_URL)
                .set(MigrateThingDefinition.JsonFields.JSON_MIGRATION_PAYLOAD, JsonFactory.newObjectBuilder()
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("dimmer-level", 0.5)
                                .build())
                        .build())
                .set(MigrateThingDefinition.JsonFields.JSON_PATCH_CONDITIONS, JsonObject.newBuilder()
                        .set("thing:/features/thermostat", "eq(attributes/dimmer-level,1.0)")
                        .build())
                .build();

        postMigrateDefinition(testThingId, migrationPayload.toString(), false)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(JsonObject.newBuilder()
                        .set(ThingCommand.JsonFields.JSON_THING_ID, testThingId.toString())
                        .set(MigrateThingDefinitionResponse.JsonFields.JSON_PATCH, JsonFactory.newObjectBuilder()
                                .set("definition", THING_DEFINITION_URL)
                                .set("attributes", JsonFactory.newObjectBuilder()
                                        .set("dimmer-level", 0.5)
                                        .build())
                                .build())
                        .set(MigrateThingDefinitionResponse.JsonFields.JSON_MERGE_STATUS, "APPLIED")
                        .build()))
                .fire();
    }

    @Test
    public void test6_MigrateDefinitionWithNotFoundThingId() {
        final JsonObject migrationPayload = buildMigrationPayload();

        postMigrateDefinition(serviceEnv.getDefaultNamespaceName() + ":unknownThingId", migrationPayload.toString(), false)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void test7_MigrateDefinitionWithBadRequestInvalidPayload() {
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
                .setAttribute(JsonPointer.of("on"), JsonValue.of(false))
                .setAttribute(JsonPointer.of("color"), JsonFactory.newObjectBuilder()
                        .set("r", 0)
                        .set("g", 0)
                        .set("b", 0)
                        .build())
                .setAttribute(JsonPointer.of("dimmer-level"), JsonValue.of(0.0))
                .build();
    }

    private static Thing newThingWithPlaceholders(final ThingId thingId) {
        return ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonPointer.of("on"), JsonValue.of(true))
                .setAttribute(JsonPointer.of("color"), JsonFactory.newObjectBuilder()
                        .set("r", 1)
                        .set("g", 2)
                        .set("b", 3)
                        .build())
                .setAttribute(JsonPointer.of("dimmer-level"), JsonValue.of(0.5))
                .build();
    }

    private static Thing newThingWithOptionalWhite(final ThingId thingId) {
        return ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonPointer.of("on"), JsonValue.of(true))
                .setAttribute(JsonPointer.of("color"), JsonFactory.newObjectBuilder()
                        .set("r", 1)
                        .set("g", 2)
                        .set("b", 3)
                        .set("w", 7)
                        .build())
                .setAttribute(JsonPointer.of("dimmer-level"), JsonValue.of(0.7))
                .build();
    }

    private static JsonObject buildMigrationPayload() {
        return JsonObject.newBuilder()
                .set(MigrateThingDefinition.JsonFields.JSON_THING_DEFINITION_URL, THING_DEFINITION_URL)
                .set(MigrateThingDefinition.JsonFields.JSON_MIGRATION_PAYLOAD, JsonFactory.newObjectBuilder()
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("color", JsonFactory.newObjectBuilder()
                                        .set("g", 5)
                                        .build())
                                .set("dimmer-level", 1.0)
                                .build())
                        .build())
                .set(MigrateThingDefinition.JsonFields.JSON_INITIALIZE_MISSING_PROPERTIES_FROM_DEFAULTS, true)
                .build();
    }

    private static JsonObject buildExpectedResponse(ThingId thingId, boolean dryRun) {
        return JsonObject.newBuilder()
                .set(ThingCommand.JsonFields.JSON_THING_ID, thingId.toString())
                .set(MigrateThingDefinitionResponse.JsonFields.JSON_PATCH, JsonFactory.newObjectBuilder()
                        .set("definition", THING_DEFINITION_URL)
                        .set("attributes", JsonFactory.newObjectBuilder()
                                .set("color", JsonFactory.newObjectBuilder()
                                        .set("g", 5)
                                        .build())
                                .set("dimmer-level", 1.0)
                                .build())
                        .build())
                .set(MigrateThingDefinitionResponse.JsonFields.JSON_MERGE_STATUS, dryRun ? "DRY_RUN" : "APPLIED")
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
