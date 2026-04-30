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

import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
import static org.eclipse.ditto.base.model.common.HttpStatus.FORBIDDEN;
import static org.eclipse.ditto.base.model.common.HttpStatus.NOT_FOUND;
import static org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT;
import static org.eclipse.ditto.base.model.common.HttpStatus.OK;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.policyResource;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.thingResource;
import static org.eclipse.ditto.things.api.Permission.READ;
import static org.eclipse.ditto.things.api.Permission.WRITE;

import java.util.List;
import java.util.Set;

import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.AllowedAddition;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.PolicyImport;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ResourcePathBuilder;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for the dedicated policy entry sub-resource HTTP routes:
 * <ul>
 *   <li>GET/PUT {@code /entries/{label}/importable}</li>
 *   <li>GET/PUT {@code /entries/{label}/allowedAdditions}</li>
 * </ul>
 */
public final class PolicyEntryImportableSubResourcesIT extends IntegrationTest {

    private PolicyId importedPolicyId;
    private PolicyId importingPolicyId;
    private Subject defaultSubject;
    private Subject subject2;

    @Before
    public void setUp() {
        importedPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("imported"));
        importingPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("importing"));
        defaultSubject = serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject();
        subject2 = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
    }

    @Test
    public void getAndPutPolicyEntryImportable() {
        // Create policy with DEFAULT entry (IMPLICIT importable)
        final Policy policy = buildImportedPolicy(importedPolicyId, Set.of());
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        // GET importable and verify it is "implicit"
        getPolicyEntryImportable(importedPolicyId, "DEFAULT")
                .expectingBody(containsOnly(JsonValue.of("implicit")))
                .expectingHttpStatus(OK)
                .fire();

        // PUT importable to "never"
        putPolicyEntryImportable(importedPolicyId, "DEFAULT", "never")
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // GET importable again and verify it is "never"
        getPolicyEntryImportable(importedPolicyId, "DEFAULT")
                .expectingBody(containsOnly(JsonValue.of("never")))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void getAndPutPolicyEntryAllowedAdditions() {
        // Create policy with DEFAULT entry that has allowedAdditions=["subjects"]
        final Policy policy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedAddition.SUBJECTS));
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        // GET allowedAdditions and verify it contains "subjects"
        getPolicyEntryAllowedAdditions(importedPolicyId, "DEFAULT")
                .expectingBody(containsOnly(JsonArray.newBuilder().add("subjects").build()))
                .expectingHttpStatus(OK)
                .fire();

        // PUT allowedAdditions to ["subjects","resources"]
        final JsonArray updated = JsonArray.newBuilder().add("subjects").add("resources").build();
        putPolicyEntryAllowedAdditions(importedPolicyId, "DEFAULT", updated)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // GET allowedAdditions again and verify
        getPolicyEntryAllowedAdditions(importedPolicyId, "DEFAULT")
                .expectingBody(containsOnly(updated))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void changingImportableToNeverRevokesThingAccess() {
        // Create imported policy with DEFAULT entry (IMPLICIT, allows subject additions)
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Create importing policy with subject2 + import reference to DEFAULT
        putPolicy(importingPolicyId, buildImportingPolicyJsonWithImportRef(
                importingPolicyId, importedPolicyId, subject2))
                .expectingHttpStatus(CREATED).fire();

        // Create a thing with the importing policy
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // Verify user2 can access the thing
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Change importable of DEFAULT to NEVER on the imported (template) policy
        putPolicyEntryImportable(importedPolicyId, "DEFAULT", "never")
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Verify user2 can no longer access the thing
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void absentAllowedAdditionsImposesNoRestriction() {
        // Imported policy entry with the allowedAdditions field ABSENT (not set at all).
        // Round-2 semantics: absent = no restriction, so the consumer's own subject must NOT
        // be stripped at enforcement time. This is the upgrade-friendly default for templates
        // that pre-date the feature.
        final Policy importedPolicy = buildImportedPolicyWithoutAllowedAdditions(importedPolicyId);
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        final PolicyImport simpleImport = PoliciesModelFactory.newPolicyImport(importedPolicyId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        final Policy importingPolicy = buildImportingPolicy(importingPolicyId)
                .toBuilder().setPolicyImport(simpleImport).build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        final PolicyEntry userEntry = PoliciesModelFactory.newPolicyEntry("user-access",
                List.of(subject2),
                List.of(),
                ImportableType.NEVER, Set.of());
        putPolicyEntry(importingPolicyId, userEntry).expectingHttpStatus(CREATED).fire();

        final JsonArray refs = JsonArray.of(importRef(importedPolicyId, "DEFAULT"));
        putReferences(importingPolicyId, "user-access", refs)
                .expectingHttpStatus(CREATED)
                .fire();

        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2 has runtime access -- own subject is kept (absent = unrestricted) and the
        // referenced template grants thing:/ READ.
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void explicitEmptyAllowedAdditionsStripsOwnSubject() {
        // Imported policy entry with allowedAdditions=[] (explicit empty deny-all).
        // The consumer's own subject must be stripped at enforcement time. Adding "subjects"
        // to the filter via the sub-resource route then unblocks the consumer.
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId, Set.of());
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        final PolicyImport simpleImport = PoliciesModelFactory.newPolicyImport(importedPolicyId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        final Policy importingPolicy = buildImportingPolicy(importingPolicyId)
                .toBuilder().setPolicyImport(simpleImport).build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        final PolicyEntry userEntry = PoliciesModelFactory.newPolicyEntry("user-access",
                List.of(subject2),
                List.of(),
                ImportableType.NEVER, Set.of());
        putPolicyEntry(importingPolicyId, userEntry).expectingHttpStatus(CREATED).fire();

        final JsonArray refs = JsonArray.of(importRef(importedPolicyId, "DEFAULT"));
        putReferences(importingPolicyId, "user-access", refs)
                .expectingHttpStatus(CREATED)
                .fire();

        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2 has no access: own subject is stripped because the explicit empty filter
        // denies all consumer additions.
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // Loosen the filter to allow "subjects" -- subject2 now gains runtime access.
        putPolicyEntryAllowedAdditions(importedPolicyId, "DEFAULT",
                JsonArray.newBuilder().add("subjects").build())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void addingAllowedAdditionsViaSubResourceEnablesThingAccess() {
        // Create imported policy WITHOUT allowedAdditions (but IMPLICIT importable)
        final Policy importedPolicy = buildImportedPolicyWithoutAllowedAdditions(importedPolicyId);
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Create importing policy with a simple import (no references yet)
        final PolicyImport simpleImport = PoliciesModelFactory.newPolicyImport(importedPolicyId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        final Policy importingPolicy = buildImportingPolicy(importingPolicyId)
                .toBuilder().setPolicyImport(simpleImport).build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing with the importing policy
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // Verify user2 cannot access the thing initially
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // Add allowedAdditions=["subjects"] via the sub-resource route on the imported policy
        putPolicyEntryAllowedAdditions(importedPolicyId, "DEFAULT",
                JsonArray.newBuilder().add("subjects").build())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Add a user-access entry with subject2 + import reference
        final PolicyEntry userEntry = PoliciesModelFactory.newPolicyEntry("user-access",
                List.of(subject2),
                List.of(),
                ImportableType.NEVER, Set.of());
        putPolicyEntry(importingPolicyId, userEntry).expectingHttpStatus(CREATED).fire();

        final JsonArray refs = JsonArray.of(importRef(importedPolicyId, "DEFAULT"));
        putReferences(importingPolicyId, "user-access", refs)
                .expectingHttpStatus(CREATED)
                .fire();

        // Verify user2 can now access the thing
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void removingAllowedAdditionsSilentlyStripsOwnSubjectAtRuntime() {
        // Create imported policy WITH allowedAdditions=["subjects"]
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Create importing policy with subject2 + import reference to DEFAULT
        putPolicy(importingPolicyId, buildImportingPolicyJsonWithImportRef(
                importingPolicyId, importedPolicyId, subject2))
                .expectingHttpStatus(CREATED).fire();

        // Create a thing using the importing policy
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2 has access: own subject + inherited resources from the import reference
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Remove allowedAdditions from the imported policy's DEFAULT entry.
        // This succeeds at write time -- no rejection of already-persisted import references.
        putPolicyEntryAllowedAdditions(importedPolicyId, "DEFAULT",
                JsonArray.empty())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Adding another import reference with the now-disallowed own subject also succeeds at
        // write time -- allowedAdditions is a runtime filter, not a write-time contract.
        final JsonArray refs = JsonArray.of(importRef(importedPolicyId, "DEFAULT"));
        putReferences(importingPolicyId, "user-access", refs)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 loses access at runtime: the own subject is silently stripped because
        // the imported entry's allowedAdditions no longer permits subject additions.
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void removingResourcesFromAllowedAdditionsSilentlyStripsOwnResourceAtRuntime() {
        // Create imported policy with allowedAdditions=["subjects","resources"]
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedAddition.SUBJECTS, AllowedAddition.RESOURCES));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Create importing policy with import ref + own WRITE resource on thing:/attributes
        final JsonObject policyJson = buildImportingPolicyJsonWithExtraResource(
                importingPolicyId, importedPolicyId, subject2, "thing:/attributes", List.of("WRITE"));
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("k", "v").build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2 can WRITE attributes via the own resource addition
        putAttributes(TestConstants.API_V_2, thingId,
                JsonObject.newBuilder().set("k", "v1").build().toString())
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Remove "resources" from allowedAdditions, keeping only "subjects".
        // The PUT itself succeeds -- persisted state may still contain the now-disallowed
        // own resource addition, but it must no longer be effective at enforcement time.
        putPolicyEntryAllowedAdditions(importedPolicyId, "DEFAULT",
                JsonArray.newBuilder().add("subjects").build())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 can still READ via the inherited template grant (subject is allowed)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // subject2 can no longer WRITE attributes -- the own resource addition is silently
        // stripped at enforcement time because RESOURCES is no longer in allowedAdditions.
        putAttributes(TestConstants.API_V_2, thingId,
                JsonObject.newBuilder().set("k", "v2").build().toString())
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void getAllowedAdditionsReturnsEmptyArrayForAbsentField() {
        // The DEFAULT entry of buildImportedPolicyWithoutAllowedAdditions has the field absent
        // in the underlying model. The route now returns 200 + [] instead of 404, distinguishing
        // "entry exists but field unset" from "entry not found".
        final Policy importedPolicy = buildImportedPolicyWithoutAllowedAdditions(importedPolicyId);
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        getPolicyEntryAllowedAdditions(importedPolicyId, "DEFAULT")
                .expectingBody(containsOnly(JsonArray.empty()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void deleteAllowedAdditionsClearsFieldAndReinstatesNoRestrictionSemantics() {
        // Start with allowedAdditions=[] (explicit empty deny-all). A consumer's own subject
        // is stripped at runtime. After DELETE, the field becomes absent which means
        // "no restriction" -- the consumer's own subject is no longer stripped.
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId, Set.of());
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        final PolicyImport simpleImport = PoliciesModelFactory.newPolicyImport(importedPolicyId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        final Policy importingPolicy = buildImportingPolicy(importingPolicyId)
                .toBuilder().setPolicyImport(simpleImport).build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        final PolicyEntry userEntry = PoliciesModelFactory.newPolicyEntry("user-access",
                List.of(subject2),
                List.of(),
                ImportableType.NEVER, Set.of());
        putPolicyEntry(importingPolicyId, userEntry).expectingHttpStatus(CREATED).fire();

        final JsonArray refs = JsonArray.of(importRef(importedPolicyId, "DEFAULT"));
        putReferences(importingPolicyId, "user-access", refs)
                .expectingHttpStatus(CREATED)
                .fire();

        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2 has no access: explicit empty filter strips the own subject.
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // DELETE the field on the imported entry -- field becomes absent.
        deletePolicyEntryAllowedAdditions(importedPolicyId, "DEFAULT")
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // GET reflects the absent field as an empty array on the wire.
        getPolicyEntryAllowedAdditions(importedPolicyId, "DEFAULT")
                .expectingBody(containsOnly(JsonArray.empty()))
                .expectingHttpStatus(OK)
                .fire();

        // subject2 now gains runtime access -- absent = no restriction, own subject kept.
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    // --- Helper methods for building policies ---

    private Policy buildImportedPolicy(final PolicyId policyId,
            final Set<AllowedAddition> allowedAdditions) {

        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());

        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, allowedAdditions);

        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .set(adminEntry)
                .set(defaultEntry)
                .build();
    }

    private Policy buildImportedPolicyWithoutAllowedAdditions(final PolicyId policyId) {
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setImportable(ImportableType.NEVER)
                .forLabel("DEFAULT")
                .setSubject(defaultSubject)
                .setGrantedPermissions(thingResource("/"), READ)
                .build();
    }

    private Policy buildImportingPolicy(final PolicyId policyId) {
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .build();
    }

    private JsonObject buildImportingPolicyJsonWithImportRef(final PolicyId policyId,
            final PolicyId templateId, final Subject localSubject) {

        return JsonObject.newBuilder()
                .set("policyId", policyId.toString())
                .set("imports", JsonObject.newBuilder()
                        .set(templateId.toString(), JsonObject.newBuilder()
                                .set("entries", JsonFactory.newArrayBuilder()
                                        .add("DEFAULT").build())
                                .build())
                        .build())
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(defaultSubject))
                                .set("resources", JsonObject.newBuilder()
                                        .set("policy:/", permJson(List.of("READ", "WRITE")))
                                        .set("thing:/", permJson(List.of("READ", "WRITE")))
                                        .build())
                                .set("importable", "never")
                                .build())
                        .set("user-access", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(localSubject))
                                .set("resources", JsonObject.empty())
                                .set("references", JsonArray.of(importRef(templateId, "DEFAULT")))
                                .build())
                        .build())
                .build();
    }

    private JsonObject buildImportingPolicyJsonWithExtraResource(final PolicyId policyId,
            final PolicyId templateId, final Subject localSubject,
            final String resourcePath, final List<String> grantedPerms) {

        return JsonObject.newBuilder()
                .set("policyId", policyId.toString())
                .set("imports", JsonObject.newBuilder()
                        .set(templateId.toString(), JsonObject.newBuilder()
                                .set("entries", JsonFactory.newArrayBuilder()
                                        .add("DEFAULT").build())
                                .build())
                        .build())
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(defaultSubject))
                                .set("resources", JsonObject.newBuilder()
                                        .set("policy:/", permJson(List.of("READ", "WRITE")))
                                        .set("thing:/", permJson(List.of("READ", "WRITE")))
                                        .build())
                                .set("importable", "never")
                                .build())
                        .set("user-access", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(localSubject))
                                .set("resources", JsonObject.newBuilder()
                                        .set(resourcePath, permJson(grantedPerms))
                                        .build())
                                .set("references", JsonArray.of(importRef(templateId, "DEFAULT")))
                                .build())
                        .build())
                .build();
    }

    private static JsonObject subjectsJson(final Subject subject) {
        return JsonObject.newBuilder()
                .set(subject.getId().toString(), subject.toJson())
                .build();
    }

    private static JsonObject permJson(final List<String> grant) {
        return JsonObject.newBuilder()
                .set("grant", toJsonArray(grant))
                .set("revoke", JsonArray.empty())
                .build();
    }

    private static JsonArray toJsonArray(final List<String> strings) {
        final var builder = JsonFactory.newArrayBuilder();
        strings.forEach(builder::add);
        return builder.build();
    }

    private static JsonObject importRef(final PolicyId policyId, final String entryLabel) {
        return JsonObject.newBuilder()
                .set("import", policyId.toString())
                .set("entry", entryLabel)
                .build();
    }

    // --- Helper methods for sub-resource HTTP operations ---

    private static GetMatcher getPolicyEntryImportable(final CharSequence policyId,
            final CharSequence label) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyEntry(label).toString() + "/importable";
        return get(dittoUrl(TestConstants.API_V_2, path))
                .withLogging(LOGGER, "PolicyEntryImportable");
    }

    private static PutMatcher putPolicyEntryImportable(final CharSequence policyId,
            final CharSequence label, final String importableType) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyEntry(label).toString() + "/importable";
        return put(dittoUrl(TestConstants.API_V_2, path), "\"" + importableType + "\"")
                .withLogging(LOGGER, "PolicyEntryImportable");
    }

    private static GetMatcher getPolicyEntryAllowedAdditions(final CharSequence policyId,
            final CharSequence label) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyEntry(label).toString() + "/allowedAdditions";
        return get(dittoUrl(TestConstants.API_V_2, path))
                .withLogging(LOGGER, "PolicyEntryAllowedAdditions");
    }

    private static PutMatcher putPolicyEntryAllowedAdditions(final CharSequence policyId,
            final CharSequence label, final JsonArray allowedAdditions) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyEntry(label).toString() + "/allowedAdditions";
        return put(dittoUrl(TestConstants.API_V_2, path), allowedAdditions.toString())
                .withLogging(LOGGER, "PolicyEntryAllowedAdditions");
    }

    private static DeleteMatcher deletePolicyEntryAllowedAdditions(final CharSequence policyId,
            final CharSequence label) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyEntry(label).toString() + "/allowedAdditions";
        return delete(dittoUrl(TestConstants.API_V_2, path))
                .withLogging(LOGGER, "PolicyEntryAllowedAdditions");
    }

    private static PutMatcher putReferences(final PolicyId policyId, final String label,
            final JsonArray references) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyEntryReferences(label).toString();
        return put(dittoUrl(TestConstants.API_V_2, path), references.toString())
                .withLogging(LOGGER, "References");
    }

}
