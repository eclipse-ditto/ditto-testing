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

import static org.eclipse.ditto.base.model.common.HttpStatus.BAD_REQUEST;
import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
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
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.AllowedImportAddition;
import org.eclipse.ditto.policies.model.EffectedImports;
import org.eclipse.ditto.policies.model.EntriesAdditions;
import org.eclipse.ditto.policies.model.EntryAddition;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.PolicyImport;
import org.eclipse.ditto.policies.model.Resource;
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
 * Integration tests for the dedicated policy import sub-resource HTTP routes:
 * <ul>
 *   <li>GET/PUT {@code /imports/{id}/entries}</li>
 *   <li>GET/PUT {@code /imports/{id}/entriesAdditions}</li>
 *   <li>GET/PUT/DELETE {@code /imports/{id}/entriesAdditions/{label}}</li>
 * </ul>
 */
public final class PolicyImportSubResourcesIT extends IntegrationTest {

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
    public void getAndPutPolicyImportEntries() {
        // Create imported policy with two importable entries: DEFAULT and EXTRA
        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());
        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, Set.of());
        final PolicyEntry extraEntry = PoliciesModelFactory.newPolicyEntry("EXTRA",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/attributes"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.EXPLICIT, Set.of());

        final Policy importedPolicy = PoliciesModelFactory.newPolicyBuilder(importedPolicyId)
                .set(adminEntry)
                .set(defaultEntry)
                .set(extraEntry)
                .build();
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Create importing policy that imports only DEFAULT
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(importedPolicyId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        final Policy importingPolicy = buildImportingPolicy(importingPolicyId)
                .toBuilder().setPolicyImport(policyImport).build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // GET entries and verify only DEFAULT is listed
        final JsonArray expectedEntries = JsonArray.newBuilder().add("DEFAULT").build();
        getPolicyImportEntries(importingPolicyId, importedPolicyId)
                .expectingBody(containsOnly(expectedEntries))
                .expectingHttpStatus(OK)
                .fire();

        // PUT entries to also import EXTRA
        final JsonArray updatedEntries = JsonArray.newBuilder().add("DEFAULT").add("EXTRA").build();
        putPolicyImportEntries(importingPolicyId, importedPolicyId, updatedEntries)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // GET entries again and verify both are listed
        getPolicyImportEntries(importingPolicyId, importedPolicyId)
                .expectingBody(containsOnly(updatedEntries))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void getAndPutPolicyImportEntriesAdditions() {
        // Create imported policy with DEFAULT entry that allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Create importing policy with subject2 added via entriesAdditions
        final EntryAddition entryAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("DEFAULT"),
                PoliciesModelFactory.newSubjects(subject2), null);
        final EntriesAdditions additions = PoliciesModelFactory.newEntriesAdditions(List.of(entryAddition));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")), additions);
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(importedPolicyId, effectedImports);
        final Policy importingPolicy = buildImportingPolicy(importingPolicyId)
                .toBuilder().setPolicyImport(policyImport).build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // GET entriesAdditions and verify
        getPolicyImportEntriesAdditions(importingPolicyId, importedPolicyId)
                .expectingBody(containsOnly(additions.toJson()))
                .expectingHttpStatus(OK)
                .fire();

        // PUT empty entriesAdditions
        putPolicyImportEntriesAdditions(importingPolicyId, importedPolicyId, JsonObject.empty())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // GET entriesAdditions and verify empty
        getPolicyImportEntriesAdditions(importingPolicyId, importedPolicyId)
                .expectingBody(containsOnly(JsonObject.empty()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putGetAndDeletePolicyImportEntryAddition() {
        // Create imported policy with DEFAULT entry that allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Create importing policy without additions
        final PolicyImport simpleImport = PoliciesModelFactory.newPolicyImport(importedPolicyId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        final Policy importingPolicy = buildImportingPolicy(importingPolicyId)
                .toBuilder().setPolicyImport(simpleImport).build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // PUT single entry addition for DEFAULT
        final EntryAddition entryAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("DEFAULT"),
                PoliciesModelFactory.newSubjects(subject2), null);
        final String additionBody = entryAdditionBodyString(Label.of("DEFAULT"), entryAddition);
        putPolicyImportEntryAddition(importingPolicyId, importedPolicyId, "DEFAULT", additionBody)
                .expectingHttpStatus(CREATED)
                .fire();

        // GET single entry addition
        getPolicyImportEntryAddition(importingPolicyId, importedPolicyId, "DEFAULT")
                .expectingHttpStatus(OK)
                .fire();

        // DELETE single entry addition
        deletePolicyImportEntryAddition(importingPolicyId, importedPolicyId, "DEFAULT")
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // GET after delete returns 404
        getPolicyImportEntryAddition(importingPolicyId, importedPolicyId, "DEFAULT")
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    @Test
    public void getPolicyImportEntryAdditionForNonExistentLabelFails() {
        // Create imported policy
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Create importing policy without additions
        final PolicyImport simpleImport = PoliciesModelFactory.newPolicyImport(importedPolicyId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        final Policy importingPolicy = buildImportingPolicy(importingPolicyId)
                .toBuilder().setPolicyImport(simpleImport).build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // GET non-existent entry addition returns 404
        getPolicyImportEntryAddition(importingPolicyId, importedPolicyId, "NONEXISTENT")
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    @Test
    public void putPolicyImportEntryAdditionViaSubResourceGrantsAccess() {
        // Create imported policy with DEFAULT entry that allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Create importing policy with a simple import (no additions)
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

        // Add subject2 via the entriesAdditions sub-resource route
        final EntryAddition entryAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("DEFAULT"),
                PoliciesModelFactory.newSubjects(subject2), null);
        final String additionBody = entryAdditionBodyString(Label.of("DEFAULT"), entryAddition);
        putPolicyImportEntryAddition(importingPolicyId, importedPolicyId, "DEFAULT", additionBody)
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
    public void deletePolicyImportEntryAdditionRemovesAccess() {
        // Create imported policy with DEFAULT entry that allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Create importing policy with subject2 added via entriesAdditions
        final Policy importingPolicy = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId, importedPolicyId, subject2);
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

        // Verify user2 can access the thing
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // DELETE the entry addition for DEFAULT
        deletePolicyImportEntryAddition(importingPolicyId, importedPolicyId, "DEFAULT")
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
    public void putPolicyImportEntryAdditionViaSubResourceBypassesAllowedAdditionsCheck() {
        // Create imported policy that only allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Create importing policy without additions
        final PolicyImport simpleImport = PoliciesModelFactory.newPolicyImport(importedPolicyId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        final Policy importingPolicy = buildImportingPolicy(importingPolicyId)
                .toBuilder().setPolicyImport(simpleImport).build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // PUT a resource addition via the sub-resource route — accepted because
        // the sub-resource route does not validate against allowedImportAdditions
        final Resource additionalResource = PoliciesModelFactory.newResource(thingResource("/attributes"),
                PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()));
        final EntryAddition resourceAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("DEFAULT"), null,
                PoliciesModelFactory.newResources(additionalResource));
        final String additionBody = entryAdditionBodyString(Label.of("DEFAULT"), resourceAddition);
        putPolicyImportEntryAddition(importingPolicyId, importedPolicyId, "DEFAULT", additionBody)
                .expectingHttpStatus(CREATED)
                .fire();

        // Verify the addition was stored
        getPolicyImportEntryAddition(importingPolicyId, importedPolicyId, "DEFAULT")
                .expectingHttpStatus(OK)
                .fire();

        // In contrast, modifying the full import with resource additions IS rejected
        final EntriesAdditions additions = PoliciesModelFactory.newEntriesAdditions(List.of(resourceAddition));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")), additions);
        final PolicyImport importWithResourceAdditions =
                PoliciesModelFactory.newPolicyImport(importedPolicyId, effectedImports);
        putPolicyImport(importingPolicyId, importWithResourceAdditions)
                .expectingHttpStatus(BAD_REQUEST)
                .expectingErrorCode("policies:import.invalid")
                .fire();
    }

    // --- Helper methods for building policies ---

    private Policy buildImportedPolicy(final PolicyId policyId,
            final Set<AllowedImportAddition> allowedImportAdditions) {

        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());

        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, allowedImportAdditions);

        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .set(adminEntry)
                .set(defaultEntry)
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

    private Policy buildImportingPolicyWithSubjectAdditions(final PolicyId policyId,
            final PolicyId importedPolicyId, final Subject additionalSubject) {

        final EntryAddition entryAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("DEFAULT"),
                PoliciesModelFactory.newSubjects(additionalSubject), null);
        final EntriesAdditions additions = PoliciesModelFactory.newEntriesAdditions(List.of(entryAddition));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")), additions);
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(importedPolicyId, effectedImports);

        return buildImportingPolicy(policyId).toBuilder()
                .setPolicyImport(policyImport)
                .build();
    }

    /**
     * Extracts the JSON body for a single entry addition by label from an EntriesAdditions object.
     */
    private static String entryAdditionBodyString(final Label label, final EntryAddition entryAddition) {
        return PoliciesModelFactory.newEntriesAdditions(List.of(entryAddition))
                .toJson()
                .getValue(label.toString())
                .map(JsonValue::toString)
                .orElseThrow();
    }

    // --- Helper methods for sub-resource HTTP operations ---

    private static GetMatcher getPolicyImportEntries(final CharSequence policyId,
            final CharSequence importedPolicyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyImport(importedPolicyId).toString() + "/entries";
        return get(dittoUrl(TestConstants.API_V_2, path))
                .withLogging(LOGGER, "PolicyImportEntries");
    }

    private static PutMatcher putPolicyImportEntries(final CharSequence policyId,
            final CharSequence importedPolicyId, final JsonArray entries) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyImport(importedPolicyId).toString() + "/entries";
        return put(dittoUrl(TestConstants.API_V_2, path), entries.toString())
                .withLogging(LOGGER, "PolicyImportEntries");
    }

    private static GetMatcher getPolicyImportEntriesAdditions(final CharSequence policyId,
            final CharSequence importedPolicyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyImport(importedPolicyId).toString() + "/entriesAdditions";
        return get(dittoUrl(TestConstants.API_V_2, path))
                .withLogging(LOGGER, "PolicyImportEntriesAdditions");
    }

    private static PutMatcher putPolicyImportEntriesAdditions(final CharSequence policyId,
            final CharSequence importedPolicyId, final JsonObject entriesAdditions) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyImport(importedPolicyId).toString() + "/entriesAdditions";
        return put(dittoUrl(TestConstants.API_V_2, path), entriesAdditions.toString())
                .withLogging(LOGGER, "PolicyImportEntriesAdditions");
    }

    private static GetMatcher getPolicyImportEntryAddition(final CharSequence policyId,
            final CharSequence importedPolicyId, final CharSequence label) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyImport(importedPolicyId).toString() + "/entriesAdditions/" + label;
        return get(dittoUrl(TestConstants.API_V_2, path))
                .withLogging(LOGGER, "PolicyImportEntryAddition");
    }

    private static PutMatcher putPolicyImportEntryAddition(final CharSequence policyId,
            final CharSequence importedPolicyId, final CharSequence label, final String body) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyImport(importedPolicyId).toString() + "/entriesAdditions/" + label;
        return put(dittoUrl(TestConstants.API_V_2, path), body)
                .withLogging(LOGGER, "PolicyImportEntryAddition");
    }

    private static DeleteMatcher deletePolicyImportEntryAddition(final CharSequence policyId,
            final CharSequence importedPolicyId, final CharSequence label) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyImport(importedPolicyId).toString() + "/entriesAdditions/" + label;
        return delete(dittoUrl(TestConstants.API_V_2, path))
                .withLogging(LOGGER, "PolicyImportEntryAddition");
    }

}
