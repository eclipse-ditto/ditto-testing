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

import static org.eclipse.ditto.base.model.common.HttpStatus.BAD_REQUEST;
import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
import static org.eclipse.ditto.base.model.common.HttpStatus.NOT_FOUND;
import static org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT;
import static org.eclipse.ditto.base.model.common.HttpStatus.OK;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.policyResource;
import static org.eclipse.ditto.things.api.Permission.READ;
import static org.eclipse.ditto.things.api.Permission.WRITE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.EffectedImports;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.PolicyImport;
import org.eclipse.ditto.policies.model.PolicyImports;
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
 * Integration Tests for /policies/<policyId>/imports resources
 */
public final class PolicyImportsIT extends IntegrationTest {

    private PolicyId importingPolicyId;
    private PolicyId importedPolicyId;
    private Policy importedPolicyFullAccess;
    private Policy importingPolicyFullAccess;
    private Policy importingPolicyRestrictedAccess;
    private Policy importedPolicyRestrictedAccess;
    private PolicyImport policyImport;
    private Policy importedPolicyMinimalAccess;

    @Before
    public void setUp() throws Exception {
        importingPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("importing"));
        importedPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("imported"));
        policyImport = createPolicyImports(importedPolicyId, "EXPLICIT");

        importedPolicyFullAccess = buildImportedPolicyFullAccess(importedPolicyId);
        importingPolicyFullAccess =
                buildImportingPolicyFullAccess(importingPolicyId, createDefaultSubject());

        importedPolicyRestrictedAccess = buildImportedPolicyRestrictedAccess(importedPolicyId);
        importingPolicyRestrictedAccess =
                buildImportingPolicyRestrictedAccess(importingPolicyId, createDefaultSubject());

        importedPolicyMinimalAccess = buildMinimalPolicy(importedPolicyId);
    }

    @Test
    public void putAndGetPolicyWithImportsAndFullAccess() {
        final Policy policyWithImports = importingPolicyFullAccess.toBuilder().setPolicyImport(policyImport).build();
        putAndGetPolicyWithImports(importedPolicyFullAccess, importedPolicyFullAccess.toJson(), policyWithImports,
                policyWithImports.toJson());
    }

    @Test
    public void putAndGetPolicyWithImportsAndRestrictedAccess() {
        final Policy policyWithImports =
                importingPolicyRestrictedAccess.toBuilder().setPolicyImport(policyImport).build();
        putAndGetPolicyWithImports(
                importedPolicyRestrictedAccess,
                importedPolicyRestrictedAccess.toJson(
                        JsonFieldSelector.newInstance("policyId", "/entries/IMPLICIT", "/entries/EXPLICIT",
                                "/entries/NEVER")),
                policyWithImports,
                policyWithImports.toJson(JsonFieldSelector.newInstance("policyId", "/imports", "/entries/IMPORTS"))
        );
    }

    private void putAndGetPolicyWithImports(
            final Policy importedPolicy,
            final JsonValue expectedImportedPolicyJson,
            final Policy importingPolicy,
            final JsonValue expectedImportingPolicyJson) {

        putPolicy(importedPolicyId, importedPolicy).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, importingPolicy).expectingHttpStatus(CREATED).fire();

        getPolicy(importedPolicyId)
                .expectingBody(containsOnly(expectedImportedPolicyJson))
                .expectingHttpStatus(OK)
                .fire();

        getPolicy(importingPolicyId)
                .expectingBody(containsOnly(expectedImportingPolicyJson))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putAndGetPolicyImportsFullAccess() {
        putAndGetPolicyImports(importedPolicyFullAccess, importingPolicyFullAccess);
    }

    @Test
    public void putAndGetPolicyImportsRestrictedAccess() {
        putAndGetPolicyImports(importedPolicyRestrictedAccess, importingPolicyRestrictedAccess);
    }

    private void putAndGetPolicyImports(final Policy importedPolicy, final Policy importingPolicy) {
        final PolicyImports policyImports = PoliciesModelFactory.newPolicyImports(policyImport);

        putPolicy(importedPolicyId, importedPolicy).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, importingPolicy).expectingHttpStatus(CREATED).fire();

        // put and get policy imports
        putPolicyImports(importingPolicyId, policyImports)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
        getPolicyImports(importingPolicyId)
                .expectingBody(containsOnly(policyImports.toJson()))
                .expectingHttpStatus(OK)
                .fire();

        // put empty policy imports
        putPolicyImports(importingPolicyId, PoliciesModelFactory.newPolicyImports(List.of()))
                .expectingHttpStatus(NO_CONTENT)
                .fire();
        // ... and expect empty response
        getPolicyImports(importingPolicyId)
                .expectingBody(containsOnly(JsonObject.empty()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putAndGetAndDeletePolicyImportFullAccess() {
        putAndGetAndDeletePolicyImport(importedPolicyFullAccess, importingPolicyFullAccess);
    }

    @Test
    public void putAndGetAndDeletePolicyImportRestrictedAccess() {
        putAndGetAndDeletePolicyImport(importedPolicyRestrictedAccess, importingPolicyRestrictedAccess);
    }

    private void putAndGetAndDeletePolicyImport(final Policy importedPolicy, final Policy importingPolicy) {

        putPolicy(importedPolicyId, importedPolicy).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, importingPolicy).expectingHttpStatus(CREATED).fire();

        // create individual policy import
        putPolicyImport(importingPolicyId, policyImport).expectingHttpStatus(CREATED).fire();
        // ... and expect created imports are returned
        getPolicyImport(importingPolicyId, importedPolicyId)
                .expectingBody(containsOnly(policyImport.toJson()))
                .expectingHttpStatus(OK)
                .fire();

        // modify individual policy import
        final PolicyImport modifiedPolicyImport = createPolicyImports(importedPolicyId, "NEVER");
        putPolicyImport(importingPolicyId, modifiedPolicyImport).expectingHttpStatus(NO_CONTENT).fire();
        // ... and expect modified policy import is returned
        getPolicyImport(importingPolicyId, importedPolicyId)
                .expectingBody(containsOnly(modifiedPolicyImport.toJson()))
                .expectingHttpStatus(OK)
                .fire();

        // get non existent import and expect 404 response
        getPolicyImport(importingPolicyId, PolicyId.inDefaultNamespace("missing"))
                .expectingErrorCode("policies:import.notfound")
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // delete individual policy import
        deletePolicyImport(importingPolicyId, importedPolicyId).expectingHttpStatus(NO_CONTENT).fire();

        // ... and expect individual policy imports are not found after deletion
        getPolicyImport(importingPolicyId, importedPolicyId).expectingHttpStatus(NOT_FOUND).fire();
    }

    @Test
    public void putPolicyImportsWithNoAccessOnImportedEntry() {


        putPolicy(importedPolicyId, importedPolicyMinimalAccess).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, importingPolicyRestrictedAccess).expectingHttpStatus(CREATED).fire();

        putPolicyImports(importingPolicyId, PolicyImports.newInstance(policyImport))
                .expectingErrorCode("policies:policy.notfound")
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    @Test
    public void putPolicyImportWithNoAccessOnImportedEntry() {
        putPolicy(importedPolicyId, importedPolicyMinimalAccess).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, importingPolicyRestrictedAccess).expectingHttpStatus(CREATED).fire();

        putPolicyImport(importingPolicyId, policyImport)
                .expectingErrorCode("policies:policy.notfound")
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    @Test
    public void putPolicyImportingItselfIsNotAllowed() {

        putPolicy(importedPolicyId, importedPolicyRestrictedAccess)
                .expectingHttpStatus(CREATED)
                .fire();

        final PolicyImport policyImportWithSelfReference = PolicyImport.newInstance(importingPolicyId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("IMPORTS"))));
        final PolicyImports policyImportsWithSelfReference = PolicyImports.newInstance(policyImportWithSelfReference);
        final JsonObject policyWithSelfReference = importingPolicyRestrictedAccess.toJson()
                .set(Policy.JsonFields.IMPORTS, policyImportsWithSelfReference.toJson());

        // try to create policy
        putPolicy(importingPolicyId, policyWithSelfReference)
                .expectingHttpStatus(BAD_REQUEST)
                .expectingErrorCode("policies:import.invalid")
                .fire();

        // try to modify policy
        putPolicy(importingPolicyId, importingPolicyRestrictedAccess).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, policyWithSelfReference)
                .expectingHttpStatus(BAD_REQUEST)
                .expectingErrorCode("policies:import.invalid")
                .fire();

        putPolicyImports(importingPolicyId, policyImportsWithSelfReference)
                .expectingErrorCode("policies:import.invalid")
                .expectingHttpStatus(BAD_REQUEST)
                .fire();

        putPolicyImport(importingPolicyId, policyImportWithSelfReference)
                .expectingErrorCode("policies:import.invalid")
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    @Test
    public void putPolicyWithTooManyImports() {
        final PolicyImports policyImports = PoliciesModelFactory.newPolicyImports(createPolicyImports(11, importedPolicyId));

        putPolicy(importingPolicyId, importingPolicyFullAccess)
                .expectingHttpStatus(CREATED)
                .fire();

        for (PolicyImport policyImport : policyImports) {
            var policy = buildImportedPolicyFullAccess(policyImport.getImportedPolicyId());
            putPolicy(policy.getEntityId().get(), policy)
                    .expectingHttpStatus(CREATED)
                    .fire();
        }

        putPolicyImports(importingPolicyId, policyImports)
                .expectingErrorCode("policies:imports.toolarge")
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    private static PolicyImports createPolicyImports(final int importsNumber, final PolicyId importedPolicyId) {
        final Collection<PolicyImport> allPolicyImports =
                new ArrayList<>(1 + importsNumber);
        for (int i = 1; i <= importsNumber; i++) {
            allPolicyImports.add(PoliciesModelFactory.newPolicyImport(
                    PolicyId.of(importedPolicyId.toString() + i)));
        }
        return PoliciesModelFactory.newPolicyImports(allPolicyImports);
    }

    private static Subject createDefaultSubject() {
        return serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject();
    }

    private static Policy buildImportedPolicyRestrictedAccess(final PolicyId policyId) {

        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(createDefaultSubject())
                .setGrantedPermissions(policyResource("/"), WRITE)
                .setImportable(ImportableType.NEVER)
                .forLabel("EXPLICIT")
                .setSubject(createDefaultSubject())
                .setGrantedPermissions(policyResource("/entries/EXPLICIT"), READ)
                .setImportable(ImportableType.EXPLICIT)
                .forLabel("IMPLICIT")
                .setSubject(createDefaultSubject())
                .setGrantedPermissions(policyResource("/entries/IMPLICIT"), READ)
                .forLabel("NEVER")
                .setSubject(createDefaultSubject())
                .setGrantedPermissions(policyResource("/entries/NEVER"), READ)
                .setImportable(ImportableType.NEVER)
                .build();
    }

    private static Policy buildImportedPolicyFullAccess(final PolicyId policyId) {

        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(createDefaultSubject())
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .build();
    }

    private static Policy buildMinimalPolicy(final PolicyId policyId) {

        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(createDefaultSubject())
                .setGrantedPermissions(policyResource("/"), WRITE)
                .build();
    }

    private static Policy buildImportingPolicyFullAccess(final PolicyId policyId, final Subject subject,
            final PolicyImport... policyImports) {
        final List<PolicyImport> policyImportList = Arrays.stream(policyImports).collect(Collectors.toList());
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(subject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setPolicyImports(PolicyImports.newInstance(policyImportList))
                .build();
    }

    private static Policy buildImportingPolicyRestrictedAccess(final PolicyId policyId, final Subject subject,
            final PolicyImport... policyImports) {
        final List<PolicyImport> policyImportList = Arrays.stream(policyImports).collect(Collectors.toList());
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(subject)
                .setGrantedPermissions(policyResource("/"), WRITE)
                .setImportable(ImportableType.NEVER)
                .forLabel("IMPORTS")
                .setSubject(subject)
                .setGrantedPermissions(policyResource("/imports"), READ, WRITE)
                .setGrantedPermissions(policyResource("/entries/IMPORTS"), READ)
                .setPolicyImports(PolicyImports.newInstance(policyImportList))
                .build();
    }

    private static PolicyImport createPolicyImports(final PolicyId importedPolicyId, final String... labels) {
        return PoliciesModelFactory.newPolicyImport(importedPolicyId,
                EffectedImports.newInstance(Arrays.stream(labels).map(Label::of).collect(Collectors.toList())));
    }

    private static PutMatcher putPolicyImports(final CharSequence policyId, final PolicyImports policyImports) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImports().toString();
        final String thingsServiceUrl = dittoUrl(TestConstants.API_V_2, path);
        final String jsonString = policyImports.toJsonString();

        LOGGER.debug("PUTing Policy Imports JSON to URL '{}': {}", thingsServiceUrl, jsonString);
        return put(thingsServiceUrl, jsonString).withLogging(LOGGER, "Policy");
    }

    private static GetMatcher getPolicyImports(final CharSequence policyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImports().toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "Policy");
    }

    private static GetMatcher getPolicyImport(final CharSequence policyId, final CharSequence importedPolicyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImport(importedPolicyId).toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "Policy");
    }

    private static DeleteMatcher deletePolicyImport(final CharSequence policyId, final CharSequence importedPolicyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImport(importedPolicyId).toString();
        return delete(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "Policy");
    }

}
