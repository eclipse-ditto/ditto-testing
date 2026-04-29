/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
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

import static org.eclipse.ditto.base.model.common.HttpStatus.CONFLICT;
import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
import static org.eclipse.ditto.base.model.common.HttpStatus.NOT_FOUND;
import static org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT;
import static org.eclipse.ditto.base.model.common.HttpStatus.OK;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.policyResource;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.thingResource;
import static org.eclipse.ditto.things.api.Permission.READ;
import static org.eclipse.ditto.things.api.Permission.WRITE;

import java.util.List;

import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ResourcePathBuilder;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for local references ({@code {"entry":"label"}}) on policy entries.
 * Verifies that a local reference inherits subjects, resources, and namespaces from the referenced
 * entry within the same policy. Replaces the subject fan-out use case previously covered by
 * {@code importsAliases}.
 * <p>
 * Migrated from {@code PolicyImportsAliasesIT}.
 * </p>
 *
 * @since 3.9.0
 */
public final class PolicyEntryLocalReferencesIT extends IntegrationTest {

    private PolicyId policyId;
    private Subject defaultSubject;
    private Subject subject2;

    @Before
    public void setUp() {
        policyId = PolicyId.of(idGenerator().withPrefixedRandomName("localref"));
        defaultSubject = serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject();
        subject2 = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
    }

    // ---- Inheritance behavior ----

    @Test
    public void localReferenceInheritsSubjectsFromReferencedEntry() {
        // "shared-subjects" has subject2 + thing:/ READ
        // "consumer" has defaultSubject + thing:/ WRITE + references shared-subjects
        final JsonObject policyJson = buildPolicyJsonWithLocalRef(policyId,
                subjectsJson(subject2),
                resourcesJson("thing:/", List.of("READ"), List.of()),
                subjectsJson(defaultSubject),
                resourcesJson("thing:/", List.of("WRITE"), List.of()),
                "shared-subjects");
        putPolicy(policyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = policyId.toString();
        putThingWithPolicy(thingId, policyId);

        // subject2 can access (inherited subjects from shared-subjects into consumer's resolution)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void localReferenceInheritsResourcesFromReferencedEntry() {
        // "resource-provider" has defaultSubject + thing:/ WRITE
        // "consumer" has subject2 + thing:/ READ + references resource-provider
        final JsonObject policyJson = JsonObject.newBuilder()
                .set("policyId", policyId.toString())
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", buildAdminEntryJson())
                        .set("resource-provider", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(defaultSubject))
                                .set("resources", resourcesJson("thing:/", List.of("WRITE"), List.of()))
                                .build())
                        .set("consumer", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(subject2))
                                .set("resources", resourcesJson("thing:/", List.of("READ"), List.of()))
                                .set("references", JsonArray.of(localRef("resource-provider")))
                                .build())
                        .build())
                .build();
        putPolicy(policyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = policyId.toString();
        putThingWithPolicy(thingId, policyId);

        // subject2 can READ (own) and WRITE (inherited from resource-provider)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        putThing(TestConstants.API_V_2,
                JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", policyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("test", "value").build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void localReferenceFansOutSubjectsToMultipleEntries() {
        // "shared-subjects" has subject2 (no resources)
        // "entry-read" has thing:/ READ + references shared-subjects
        // "entry-write" has thing:/features WRITE + references shared-subjects
        final JsonObject policyJson = JsonObject.newBuilder()
                .set("policyId", policyId.toString())
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", buildAdminEntryJson())
                        .set("shared-subjects", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(subject2))
                                .set("resources", JsonObject.empty())
                                .build())
                        .set("entry-read", JsonObject.newBuilder()
                                .set("subjects", JsonObject.empty())
                                .set("resources", resourcesJson("thing:/", List.of("READ"), List.of()))
                                .set("references", JsonArray.of(localRef("shared-subjects")))
                                .build())
                        .set("entry-write", JsonObject.newBuilder()
                                .set("subjects", JsonObject.empty())
                                .set("resources", resourcesJson("thing:/features", List.of("WRITE"), List.of()))
                                .set("references", JsonArray.of(localRef("shared-subjects")))
                                .build())
                        .build())
                .build();
        putPolicy(policyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = policyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", policyId.toString())
                        .set("features", JsonObject.newBuilder()
                                .set("sensor", JsonObject.newBuilder()
                                        .set("properties", JsonObject.newBuilder()
                                                .set("value", 42).build())
                                        .build())
                                .build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2 can READ (from entry-read) and WRITE features (from entry-write)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        putFeatures(TestConstants.API_V_2, thingId,
                JsonObject.newBuilder()
                        .set("sensor", JsonObject.newBuilder()
                                .set("properties", JsonObject.newBuilder()
                                        .set("value", 99).build())
                                .build())
                        .build().toString())
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    // ---- Dynamic subject management ----

    @Test
    public void subjectAddedToReferencedEntryGrantsAccess() {
        final ThingId thingId = ThingId.of(idGenerator().withPrefixedRandomName("localRefGrant"));
        final PolicyId thingPolicyId = PolicyId.of(thingId);

        // "shared-subjects" starts with defaultSubject only
        // "consumer" has thing:/ READ + references shared-subjects
        final JsonObject policyJson = buildPolicyJsonWithLocalRef(thingPolicyId,
                subjectsJson(defaultSubject),
                resourcesJson("thing:/", List.of("READ"), List.of()),
                JsonObject.empty(),
                resourcesJson("thing:/", List.of("READ"), List.of()),
                "shared-subjects");
        putPolicy(thingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(thingPolicyId)
                .setAttribute(JsonPointer.of("status"), JsonValue.of("active"))
                .build();
        putThing(TestConstants.API_V_2, thing, org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2 cannot access yet
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // Add subject2 to shared-subjects
        putPolicyEntrySubject(thingPolicyId, "shared-subjects",
                subject2.getId().toString(), subject2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2 can now access
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void subjectRemovedFromReferencedEntryRevokesAccess() {
        final ThingId thingId = ThingId.of(idGenerator().withPrefixedRandomName("localRefRevoke"));
        final PolicyId thingPolicyId = PolicyId.of(thingId);

        // "shared-subjects" has both defaultSubject and subject2
        final JsonObject bothSubjects = JsonObject.newBuilder()
                .set(defaultSubject.getId().toString(), defaultSubject.toJson())
                .set(subject2.getId().toString(), subject2.toJson())
                .build();
        final JsonObject policyJson = buildPolicyJsonWithLocalRef(thingPolicyId,
                bothSubjects,
                resourcesJson("thing:/", List.of("READ"), List.of()),
                JsonObject.empty(),
                resourcesJson("thing:/", List.of("READ"), List.of()),
                "shared-subjects");
        putPolicy(thingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(thingPolicyId)
                .setAttribute(JsonPointer.of("status"), JsonValue.of("active"))
                .build();
        putThing(TestConstants.API_V_2, thing, org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2 can access
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Remove subject2 from shared-subjects
        deletePolicyEntrySubject(thingPolicyId, "shared-subjects",
                subject2.getId().toString())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 can no longer access
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    @Test
    public void subjectAddedViaLocalReferenceCanWriteWhenEntryGrantsWrite() {
        final ThingId thingId = ThingId.of(idGenerator().withPrefixedRandomName("localRefWrite"));
        final PolicyId thingPolicyId = PolicyId.of(thingId);

        // "shared-subjects" starts with defaultSubject only
        // "consumer" has thing:/ READ+WRITE + references shared-subjects
        final JsonObject policyJson = buildPolicyJsonWithLocalRef(thingPolicyId,
                subjectsJson(defaultSubject),
                JsonObject.empty(),
                JsonObject.empty(),
                resourcesJson("thing:/", List.of("READ", "WRITE"), List.of()),
                "shared-subjects");
        putPolicy(thingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(thingPolicyId)
                .setAttribute(JsonPointer.of("counter"), JsonValue.of(0))
                .build();
        putThing(TestConstants.API_V_2, thing, org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // Add subject2 to shared-subjects
        putPolicyEntrySubject(thingPolicyId, "shared-subjects",
                subject2.getId().toString(), subject2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2 can write
        final String attributePath = ResourcePathBuilder.forThing(thingId).attribute("counter").toString();
        put(dittoUrl(TestConstants.API_V_2, attributePath), "42")
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Verify write took effect
        get(dittoUrl(TestConstants.API_V_2, attributePath))
                .expectingBody(containsCharSequence("42"))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void multipleLocalReferencesAreMergedAdditively() {
        // "subjects-entry" has subject2, no resources
        // "resources-entry" has no subjects, thing:/ READ
        // "consumer" references both -> gets subject2 + thing:/ READ
        final JsonObject policyJson = JsonObject.newBuilder()
                .set("policyId", policyId.toString())
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", buildAdminEntryJson())
                        .set("subjects-entry", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(subject2))
                                .set("resources", JsonObject.empty())
                                .build())
                        .set("resources-entry", JsonObject.newBuilder()
                                .set("subjects", JsonObject.empty())
                                .set("resources", resourcesJson("thing:/", List.of("READ"), List.of()))
                                .build())
                        .set("consumer", JsonObject.newBuilder()
                                .set("subjects", JsonObject.empty())
                                .set("resources", JsonObject.empty())
                                .set("references", JsonArray.of(
                                        localRef("subjects-entry"),
                                        localRef("resources-entry")))
                                .build())
                        .build())
                .build();
        putPolicy(policyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = policyId.toString();
        putThingWithPolicy(thingId, policyId);

        // subject2 can access (subjects from subjects-entry + resources from resources-entry)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    // ---- Referential integrity ----

    @Test
    public void deleteReferencedEntryIsRejectedWhileReferenced() {
        final JsonObject policyJson = buildPolicyJsonWithLocalRef(policyId,
                subjectsJson(subject2),
                resourcesJson("thing:/", List.of("READ"), List.of()),
                JsonObject.empty(),
                resourcesJson("thing:/", List.of("READ"), List.of()),
                "shared-subjects");
        putPolicy(policyId, policyJson).expectingHttpStatus(CREATED).fire();

        // Try to delete shared-subjects while consumer references it
        deletePolicyEntry(policyId, "shared-subjects")
                .expectingHttpStatus(CONFLICT)
                .fire();
    }

    @Test
    public void deleteReferencedEntrySucceedsAfterRemovingReference() {
        final JsonObject policyJson = buildPolicyJsonWithLocalRef(policyId,
                subjectsJson(subject2),
                resourcesJson("thing:/", List.of("READ"), List.of()),
                JsonObject.empty(),
                resourcesJson("thing:/", List.of("READ"), List.of()),
                "shared-subjects");
        putPolicy(policyId, policyJson).expectingHttpStatus(CREATED).fire();

        // Remove local reference first
        deleteReferences(policyId, "consumer")
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Now deleting the entry succeeds
        deletePolicyEntry(policyId, "shared-subjects")
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    // ---- Helpers ----

    private void putThingWithPolicy(final String thingId, final PolicyId policyId) {
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", policyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();
    }

    private JsonObject buildPolicyJsonWithLocalRef(final PolicyId policyId,
            final JsonObject sharedSubjects, final JsonObject sharedResources,
            final JsonObject consumerSubjects, final JsonObject consumerResources,
            final String referencedEntry) {

        return JsonObject.newBuilder()
                .set("policyId", policyId.toString())
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", buildAdminEntryJson())
                        .set("shared-subjects", JsonObject.newBuilder()
                                .set("subjects", sharedSubjects)
                                .set("resources", sharedResources)
                                .build())
                        .set("consumer", JsonObject.newBuilder()
                                .set("subjects", consumerSubjects)
                                .set("resources", consumerResources)
                                .set("references", JsonArray.of(localRef(referencedEntry)))
                                .build())
                        .build())
                .build();
    }

    private JsonObject buildAdminEntryJson() {
        return JsonObject.newBuilder()
                .set("subjects", subjectsJson(defaultSubject))
                .set("resources", JsonObject.newBuilder()
                        .set("policy:/", JsonObject.newBuilder()
                                .set("grant", JsonFactory.newArrayBuilder()
                                        .add("READ").add("WRITE").build())
                                .set("revoke", JsonArray.empty())
                                .build())
                        .set("thing:/", JsonObject.newBuilder()
                                .set("grant", JsonFactory.newArrayBuilder()
                                        .add("READ").add("WRITE").build())
                                .set("revoke", JsonArray.empty())
                                .build())
                        .build())
                .set("importable", "never")
                .build();
    }

    private static JsonObject subjectsJson(final Subject subject) {
        return JsonObject.newBuilder()
                .set(subject.getId().toString(), subject.toJson())
                .build();
    }

    private static JsonObject resourcesJson(final String path, final List<String> grant,
            final List<String> revoke) {
        return JsonObject.newBuilder()
                .set(path, JsonObject.newBuilder()
                        .set("grant", toJsonArray(grant))
                        .set("revoke", toJsonArray(revoke))
                        .build())
                .build();
    }

    private static JsonArray toJsonArray(final List<String> strings) {
        final var builder = JsonFactory.newArrayBuilder();
        strings.forEach(builder::add);
        return builder.build();
    }

    private static JsonObject localRef(final String entryLabel) {
        return JsonObject.newBuilder()
                .set("entry", entryLabel)
                .build();
    }

    private static DeleteMatcher deleteReferences(final PolicyId policyId, final String label) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyEntryReferences(label).toString();
        return delete(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "References");
    }

}
