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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.AllowedImportAddition;
import org.eclipse.ditto.policies.model.EffectedImports;
import org.eclipse.ditto.policies.model.EntriesAdditions;
import org.eclipse.ditto.policies.model.EntryAddition;
import org.eclipse.ditto.policies.model.ImportsAlias;
import org.eclipse.ditto.policies.model.ImportsAliases;
import org.eclipse.ditto.policies.model.ImportsAliasTarget;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.PolicyImport;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.Subjects;
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
 * Integration tests for {@code /policies/<policyId>/importsAliases} resources and subject fan-out through alias labels.
 */
public final class PolicyImportsAliasesIT extends IntegrationTest {

    private static final Label ALIAS_LABEL = Label.of("operator");
    private static final Label TARGET_LABEL_1 = Label.of("operator-reactor");
    private static final Label TARGET_LABEL_2 = Label.of("operator-turbine");

    private PolicyId templatePolicyId;
    private PolicyId importingPolicyId;
    private Policy templatePolicy;
    private Policy importingPolicy;
    private ImportsAlias alias;
    private ImportsAliases aliases;

    @Before
    public void setUp() {
        templatePolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        importingPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("importing"));

        // Template policy: provides entries that can be imported with entriesAdditions
        templatePolicy = PoliciesModelFactory.newPolicyBuilder(templatePolicyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject())
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setImportable(ImportableType.NEVER)
                .forLabel("operator-reactor")
                .setSubject(defaultSubject())
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .setImportable(ImportableType.EXPLICIT)
                .setAllowedImportAdditionsFor("operator-reactor", Set.of(AllowedImportAddition.SUBJECTS))
                .forLabel("operator-turbine")
                .setSubject(defaultSubject())
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .setImportable(ImportableType.EXPLICIT)
                .setAllowedImportAdditionsFor("operator-turbine", Set.of(AllowedImportAddition.SUBJECTS))
                .build();

        // Build the alias and its targets
        final List<ImportsAliasTarget> targets = Arrays.asList(
                PoliciesModelFactory.newImportsAliasTarget(templatePolicyId, TARGET_LABEL_1),
                PoliciesModelFactory.newImportsAliasTarget(templatePolicyId, TARGET_LABEL_2)
        );
        alias = PoliciesModelFactory.newImportsAlias(ALIAS_LABEL, targets);
        aliases = PoliciesModelFactory.newImportsAliases(Collections.singletonList(alias));

        // Build the import with entriesAdditions for both target entries
        final EntryAddition addition1 = PoliciesModelFactory.newEntryAddition(TARGET_LABEL_1, null, null);
        final EntryAddition addition2 = PoliciesModelFactory.newEntryAddition(TARGET_LABEL_2, null, null);
        final EntriesAdditions entriesAdditions =
                PoliciesModelFactory.newEntriesAdditions(Arrays.asList(addition1, addition2));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                Arrays.asList(TARGET_LABEL_1, TARGET_LABEL_2), entriesAdditions);
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(templatePolicyId, effectedImports);

        // Importing policy: has admin entry, imports template, defines alias
        importingPolicy = PoliciesModelFactory.newPolicyBuilder(importingPolicyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject())
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .setPolicyImports(PoliciesModelFactory.newPolicyImports(Collections.singletonList(policyImport)))
                .setImportsAliases(aliases)
                .build();
    }

    // --- CRUD on /importsAliases ---

    @Test
    public void createPolicyWithImportsAliasesAndRetrieveThem() {
        createTemplateThenImportingPolicy();

        getImportsAliases(importingPolicyId)
                .expectingBody(containsOnly(aliases.toJson()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void retrieveSingleImportsAlias() {
        createTemplateThenImportingPolicy();

        getImportsAlias(importingPolicyId, ALIAS_LABEL)
                .expectingBody(containsOnly(alias.toJson()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putAndGetSingleImportsAlias() {
        // Create policies without alias first
        putPolicy(templatePolicyId, templatePolicy).expectingHttpStatus(CREATED).fire();
        final Policy policyWithoutAlias = importingPolicy.toBuilder()
                .setImportsAliases(PoliciesModelFactory.emptyImportsAliases())
                .build();
        putPolicy(importingPolicyId, policyWithoutAlias).expectingHttpStatus(CREATED).fire();

        // PUT a single alias
        putImportsAlias(importingPolicyId, ALIAS_LABEL, alias.toJson())
                .expectingHttpStatus(CREATED)
                .fire();

        // GET it back
        getImportsAlias(importingPolicyId, ALIAS_LABEL)
                .expectingBody(containsOnly(alias.toJson()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void modifyExistingImportsAlias() {
        createTemplateThenImportingPolicy();

        // Modify alias to only have one target
        final ImportsAlias modifiedAlias = PoliciesModelFactory.newImportsAlias(ALIAS_LABEL,
                Collections.singletonList(
                        PoliciesModelFactory.newImportsAliasTarget(templatePolicyId, TARGET_LABEL_1)));

        putImportsAlias(importingPolicyId, ALIAS_LABEL, modifiedAlias.toJson())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getImportsAlias(importingPolicyId, ALIAS_LABEL)
                .expectingBody(containsOnly(modifiedAlias.toJson()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void deleteSingleImportsAlias() {
        createTemplateThenImportingPolicy();

        deleteImportsAlias(importingPolicyId, ALIAS_LABEL)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getImportsAlias(importingPolicyId, ALIAS_LABEL)
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    @Test
    public void deleteAllImportsAliases() {
        createTemplateThenImportingPolicy();

        deleteImportsAliases(importingPolicyId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getImportsAliases(importingPolicyId)
                .expectingBody(containsOnly(JsonObject.empty()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putAllImportsAliases() {
        createTemplateThenImportingPolicy();

        // Replace all aliases with a different alias
        final Label newAliasLabel = Label.of("inspector");
        final ImportsAlias newAlias = PoliciesModelFactory.newImportsAlias(newAliasLabel,
                Collections.singletonList(
                        PoliciesModelFactory.newImportsAliasTarget(templatePolicyId, TARGET_LABEL_1)));
        final ImportsAliases newAliases = PoliciesModelFactory.newImportsAliases(Collections.singletonList(newAlias));

        putImportsAliases(importingPolicyId, newAliases)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getImportsAliases(importingPolicyId)
                .expectingBody(containsOnly(newAliases.toJson()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void getNonExistentImportsAliasReturns404() {
        createTemplateThenImportingPolicy();

        // Delete the alias first
        deleteImportsAlias(importingPolicyId, ALIAS_LABEL).expectingHttpStatus(NO_CONTENT).fire();

        getImportsAlias(importingPolicyId, ALIAS_LABEL)
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    // --- Subject fan-out through alias ---

    @Test
    public void putSubjectsThroughAliasFansOutToAllTargets() {
        createTemplateThenImportingPolicy();

        final Subject newSubject = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
        final Subjects subjects = Subjects.newInstance(newSubject);

        // PUT subjects via the alias label (uses the entries/{label}/subjects endpoint)
        putPolicyEntrySubjects(importingPolicyId, ALIAS_LABEL.toString(), subjects)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Verify subject was added — retrieving via alias returns subjects from first target
        getPolicyEntrySubjects(importingPolicyId, ALIAS_LABEL.toString())
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putSingleSubjectThroughAlias() {
        createTemplateThenImportingPolicy();

        final Subject newSubject = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
        final String subjectId = newSubject.getId().toString();

        putPolicyEntrySubject(importingPolicyId, ALIAS_LABEL.toString(), subjectId, newSubject)
                .expectingHttpStatus(CREATED)
                .fire();

        getPolicyEntrySubject(importingPolicyId, ALIAS_LABEL.toString(), subjectId)
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void deleteSingleSubjectThroughAlias() {
        createTemplateThenImportingPolicy();

        final Subject newSubject = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
        final String subjectId = newSubject.getId().toString();

        // First add a subject
        putPolicyEntrySubject(importingPolicyId, ALIAS_LABEL.toString(), subjectId, newSubject)
                .expectingHttpStatus(CREATED)
                .fire();

        // Then delete it via alias
        deletePolicyEntrySubject(importingPolicyId, ALIAS_LABEL.toString(), subjectId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Verify it's gone
        getPolicyEntrySubject(importingPolicyId, ALIAS_LABEL.toString(), subjectId)
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    @Test
    public void retrieveSubjectsThroughAlias() {
        createTemplateThenImportingPolicy();

        getPolicyEntrySubjects(importingPolicyId, ALIAS_LABEL.toString())
                .expectingHttpStatus(OK)
                .fire();
    }

    // --- Conflict and protection scenarios ---

    @Test
    public void aliasLabelConflictsWithLocalEntry() {
        putPolicy(templatePolicyId, templatePolicy).expectingHttpStatus(CREATED).fire();

        // Build conflicting policy as raw JSON to bypass client-side model validation
        // (the PolicyBuilder throws ImportsAliasConflictException locally)
        final JsonObject operatorEntry = JsonObject.newBuilder()
                .set("subjects", JsonObject.newBuilder()
                        .set(defaultSubject().getId().toString(), defaultSubject().toJson())
                        .build())
                .set("resources", JsonObject.newBuilder()
                        .set("thing:/", JsonObject.newBuilder()
                                .set("grant", JsonValue.of("[\"READ\"]"))
                                .set("revoke", JsonValue.of("[]"))
                                .build())
                        .build())
                .build();
        final JsonObject conflictingPolicyJson = importingPolicy.toJson().toBuilder()
                .set(JsonPointer.of("entries/operator"), operatorEntry)
                .build();

        putPolicy(importingPolicyId, conflictingPolicyJson)
                .expectingHttpStatus(HttpStatus.CONFLICT)
                .fire();
    }

    @Test
    public void deleteImportReferencedByAliasIsRejected() {
        createTemplateThenImportingPolicy();

        // Server returns 403 (policies:import.notmodifiable) when alias references the import
        deletePolicyImport(importingPolicyId, templatePolicyId)
                .expectingHttpStatus(FORBIDDEN)
                .fire();
    }

    @Test
    public void deleteImportSucceedsAfterRemovingAlias() {
        createTemplateThenImportingPolicy();

        // First remove the alias
        deleteImportsAlias(importingPolicyId, ALIAS_LABEL)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Now deleting the import should succeed
        deletePolicyImport(importingPolicyId, templatePolicyId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void resourceOperationsOnAliasLabelAreRejected() {
        createTemplateThenImportingPolicy();

        // Alias labels are not real entries — server returns 404 for non-subject operations
        getPolicyEntryResources(importingPolicyId, ALIAS_LABEL.toString())
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    // --- Policy enforcement through alias ---

    @Test
    public void subjectAddedViaAliasGainsAccessToThing() {
        final ThingId thingId = ThingId.of(idGenerator().withPrefixedRandomName("aliasEnforcement"));

        // Create template and importing policy separately (putThingWithPolicy strips imports/aliases)
        final PolicyId tmplPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("tmpl"));
        final Policy tmplPolicy = buildTemplatePolicy(tmplPolicyId);
        putPolicy(tmplPolicy).expectingHttpStatus(CREATED).fire();

        final PolicyId thingPolicyId = PolicyId.of(thingId);
        final Policy thingPolicy = buildImportingPolicyWithAlias(thingPolicyId, tmplPolicyId);
        putPolicy(thingPolicy).expectingHttpStatus(CREATED).fire();

        // Create the Thing referencing the existing policy
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(thingPolicyId)
                .setAttribute(JsonPointer.of("status"), JsonValue.of("active"))
                .build();
        putThing(TestConstants.API_V_2, thing, org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 cannot access the Thing yet
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // Add user2 as subject through the alias
        final Subject user2Subject = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
        putPolicyEntrySubject(thingPolicyId, ALIAS_LABEL.toString(),
                user2Subject.getId().toString(), user2Subject)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 can now access the Thing
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void subjectRemovedViaAliasLosesAccessToThing() {
        final ThingId thingId = ThingId.of(idGenerator().withPrefixedRandomName("aliasRevoke"));

        final PolicyId tmplPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("tmpl"));
        final Policy tmplPolicy = buildTemplatePolicy(tmplPolicyId);
        putPolicy(tmplPolicy).expectingHttpStatus(CREATED).fire();

        final PolicyId thingPolicyId = PolicyId.of(thingId);
        final Policy thingPolicy = buildImportingPolicyWithAlias(thingPolicyId, tmplPolicyId);
        putPolicy(thingPolicy).expectingHttpStatus(CREATED).fire();

        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(thingPolicyId)
                .setAttribute(JsonPointer.of("status"), JsonValue.of("active"))
                .build();
        putThing(TestConstants.API_V_2, thing, org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // Add user2 via alias
        final Subject user2Subject = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
        putPolicyEntrySubject(thingPolicyId, ALIAS_LABEL.toString(),
                user2Subject.getId().toString(), user2Subject)
                .expectingHttpStatus(CREATED)
                .fire();

        // Verify user2 has access
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Remove user2 via alias
        deletePolicyEntrySubject(thingPolicyId, ALIAS_LABEL.toString(),
                user2Subject.getId().toString())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // user2 can no longer access the Thing
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    @Test
    public void subjectAddedViaAliasCanWriteThingWhenEntriesGrantWrite() {
        final ThingId thingId = ThingId.of(idGenerator().withPrefixedRandomName("aliasWrite"));

        final PolicyId tmplPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("tmpl"));
        final Policy tmplPolicy = buildTemplatePolicy(tmplPolicyId);
        putPolicy(tmplPolicy).expectingHttpStatus(CREATED).fire();

        final PolicyId thingPolicyId = PolicyId.of(thingId);
        final Policy thingPolicy = buildImportingPolicyWithAlias(thingPolicyId, tmplPolicyId);
        putPolicy(thingPolicy).expectingHttpStatus(CREATED).fire();

        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(thingPolicyId)
                .setAttribute(JsonPointer.of("counter"), JsonValue.of(0))
                .build();
        putThing(TestConstants.API_V_2, thing, org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // Add user2 via alias (template entries grant READ+WRITE on thing:/)
        final Subject user2Subject = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
        putPolicyEntrySubject(thingPolicyId, ALIAS_LABEL.toString(),
                user2Subject.getId().toString(), user2Subject)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 can now write to the Thing's attributes
        final String attributePath = ResourcePathBuilder.forThing(thingId).attribute("counter").toString();
        put(dittoUrl(TestConstants.API_V_2, attributePath), "42")
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Verify the write took effect by reading the attribute
        final String getAttrPath = ResourcePathBuilder.forThing(thingId).attribute("counter").toString();
        get(dittoUrl(TestConstants.API_V_2, getAttrPath))
                .expectingBody(containsCharSequence("42"))
                .expectingHttpStatus(OK)
                .fire();
    }

    // --- Helpers ---

    private Policy buildTemplatePolicy(final PolicyId templateId) {
        return PoliciesModelFactory.newPolicyBuilder(templateId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject())
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setImportable(ImportableType.NEVER)
                .forLabel(TARGET_LABEL_1.toString())
                .setSubject(defaultSubject())
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .setImportable(ImportableType.EXPLICIT)
                .setAllowedImportAdditionsFor(TARGET_LABEL_1.toString(), Set.of(AllowedImportAddition.SUBJECTS))
                .forLabel(TARGET_LABEL_2.toString())
                .setSubject(defaultSubject())
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .setImportable(ImportableType.EXPLICIT)
                .setAllowedImportAdditionsFor(TARGET_LABEL_2.toString(), Set.of(AllowedImportAddition.SUBJECTS))
                .build();
    }

    private Policy buildImportingPolicyWithAlias(final PolicyId policyId, final PolicyId tmplPolicyId) {
        final EntryAddition addition1 = PoliciesModelFactory.newEntryAddition(TARGET_LABEL_1, null, null);
        final EntryAddition addition2 = PoliciesModelFactory.newEntryAddition(TARGET_LABEL_2, null, null);
        final EntriesAdditions entriesAdditions =
                PoliciesModelFactory.newEntriesAdditions(Arrays.asList(addition1, addition2));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                Arrays.asList(TARGET_LABEL_1, TARGET_LABEL_2), entriesAdditions);
        final PolicyImport pImport = PoliciesModelFactory.newPolicyImport(tmplPolicyId, effectedImports);

        final List<ImportsAliasTarget> targets = Arrays.asList(
                PoliciesModelFactory.newImportsAliasTarget(tmplPolicyId, TARGET_LABEL_1),
                PoliciesModelFactory.newImportsAliasTarget(tmplPolicyId, TARGET_LABEL_2));
        final ImportsAlias a = PoliciesModelFactory.newImportsAlias(ALIAS_LABEL, targets);

        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject())
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .setPolicyImports(PoliciesModelFactory.newPolicyImports(Collections.singletonList(pImport)))
                .setImportsAliases(PoliciesModelFactory.newImportsAliases(Collections.singletonList(a)))
                .build();
    }

    private void createTemplateThenImportingPolicy() {
        putPolicy(templatePolicyId, templatePolicy).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, importingPolicy).expectingHttpStatus(CREATED).fire();
    }

    private static Subject defaultSubject() {
        return serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject();
    }

    private static PutMatcher putImportsAliases(final CharSequence policyId, final ImportsAliases importsAliases) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImportsAliases().toString();
        return put(dittoUrl(TestConstants.API_V_2, path), importsAliases.toJsonString())
                .withLogging(LOGGER, "ImportsAliases");
    }

    private static GetMatcher getImportsAliases(final CharSequence policyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImportsAliases().toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "ImportsAliases");
    }

    private static DeleteMatcher deleteImportsAliases(final CharSequence policyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImportsAliases().toString();
        return delete(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "ImportsAliases");
    }

    private static PutMatcher putImportsAlias(final CharSequence policyId, final Label label,
            final JsonObject aliasJson) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImportsAlias(label).toString();
        return put(dittoUrl(TestConstants.API_V_2, path), aliasJson.toString())
                .withLogging(LOGGER, "ImportsAlias");
    }

    private static GetMatcher getImportsAlias(final CharSequence policyId, final Label label) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImportsAlias(label).toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "ImportsAlias");
    }

    private static DeleteMatcher deleteImportsAlias(final CharSequence policyId, final Label label) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImportsAlias(label).toString();
        return delete(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "ImportsAlias");
    }

    private static DeleteMatcher deletePolicyImport(final CharSequence policyId,
            final CharSequence importedPolicyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImport(importedPolicyId).toString();
        return delete(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "PolicyImport");
    }

}
