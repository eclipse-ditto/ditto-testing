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
package org.eclipse.ditto.testing.system.search.security;

import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
import static org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT;
import static org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.policyResource;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.thingResource;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isCount;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEmpty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;
import static org.eclipse.ditto.things.api.Permission.READ;
import static org.eclipse.ditto.things.api.Permission.WRITE;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.eclipse.ditto.base.model.acks.DittoAcknowledgementLabel;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.model.AllowedAddition;
import org.eclipse.ditto.policies.model.EffectedImports;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.PolicyImport;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.testing.common.ResourcePathBuilder;
import org.eclipse.ditto.testing.common.SearchIntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Search integration tests verifying that the search index is correctly updated when
 * {@code transitiveImports} are added, modified, or removed on policy imports.
 * <p>
 * Uses {@code SEARCH_PERSISTED} acknowledgment for initial thing creation and
 * awaitility-based polling for assertions after policy mutations.
 * </p>
 *
 * @since 3.9.0
 */
public final class SearchWithTransitiveImportsIT extends SearchIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SearchWithTransitiveImportsIT.class);

    private static final ConditionFactory AWAITILITY_SEARCH_CONFIG =
            Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(3, TimeUnit.SECONDS);

    private Subject defaultSubject;
    private Subject subject2;

    @Before
    public void setUp() {
        defaultSubject = serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject();
        subject2 = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
    }

    @Test
    public void secondUserFindsThingViaTransitiveImport() {
        // 3-policy chain: template(C) → intermediate(B) → leaf(A)
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId intermediateId = PolicyId.of(idGenerator().withPrefixedRandomName("intermediate"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();
        putPolicy(intermediateId, buildIntermediatePolicyJson(intermediateId, templateId, subject2))
                .expectingHttpStatus(CREATED).fire();
        putPolicy(buildLeafPolicyWithTransitiveImports(leafId, intermediateId, templateId))
                .expectingHttpStatus(CREATED).fire();

        // Create thing with SEARCH_PERSISTED ack
        final ThingId thingId = ThingId.of(leafId);
        putThingAndWaitForSearchIndex(thingId, leafId);

        // subject2 finds the thing via search (access granted through transitive chain)
        searchThings(V_2)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        searchCount(V_2)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isCount(1))
                .fire();
    }

    @Test
    public void secondUserDoesNotFindThingAfterTransitiveImportsRemoved() {
        // 3-policy chain with transitive imports
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId intermediateId = PolicyId.of(idGenerator().withPrefixedRandomName("intermediate"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();
        putPolicy(intermediateId, buildIntermediatePolicyJson(intermediateId, templateId, subject2))
                .expectingHttpStatus(CREATED).fire();
        putPolicy(buildLeafPolicyWithTransitiveImports(leafId, intermediateId, templateId))
                .expectingHttpStatus(CREATED).fire();

        final ThingId thingId = ThingId.of(leafId);
        putThingAndWaitForSearchIndex(thingId, leafId);

        // subject2 finds the thing initially
        searchThings(V_2)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        // Remove transitiveImports via sub-resource
        putTransitiveImports(leafId, intermediateId, JsonArray.empty())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 no longer finds the thing (search index updated)
        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void secondUserFindsThingAfterTransitiveImportsAdded() {
        // Start WITHOUT transitiveImports, then add them and verify search index updates
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId intermediateId = PolicyId.of(idGenerator().withPrefixedRandomName("intermediate"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();
        putPolicy(intermediateId, buildIntermediatePolicyJson(intermediateId, templateId, subject2))
                .expectingHttpStatus(CREATED).fire();

        // Leaf imports intermediate WITHOUT transitiveImports
        final PolicyImport simpleImport = PoliciesModelFactory.newPolicyImport(intermediateId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("user-access"))));
        final Policy leafPolicy = buildAdminOnlyPolicy(leafId).toBuilder()
                .setPolicyImport(simpleImport)
                .build();
        putPolicy(leafPolicy).expectingHttpStatus(CREATED).fire();

        final ThingId thingId = ThingId.of(leafId);
        putThingAndWaitForSearchIndex(thingId, leafId);

        // subject2 does NOT find the thing (no transitive resolution)
        searchThings(V_2)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEmpty())
                .fire();

        // Add transitiveImports via sub-resource
        final JsonArray transitiveArray = JsonArray.newBuilder()
                .add(templateId.toString())
                .build();
        putTransitiveImports(leafId, intermediateId, transitiveArray)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 now finds the thing (search index updated)
        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();
    }

    @Test
    public void searchIndexUpdatedWhenTemplateChangesInTransitiveChain() {
        // 3-policy chain: template grants thing:/ READ only
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId intermediateId = PolicyId.of(idGenerator().withPrefixedRandomName("intermediate"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();
        putPolicy(intermediateId, buildIntermediatePolicyJson(intermediateId, templateId, subject2))
                .expectingHttpStatus(CREATED).fire();
        putPolicy(buildLeafPolicyWithTransitiveImports(leafId, intermediateId, templateId))
                .expectingHttpStatus(CREATED).fire();

        final ThingId thingId = ThingId.of(leafId);
        putThingAndWaitForSearchIndex(thingId, leafId);

        // subject2 finds the thing
        searchThings(V_2)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        // Modify template: set DEFAULT to importable=NEVER
        final PolicyEntry neverDefaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.NEVER, Set.of(AllowedAddition.SUBJECTS));
        putPolicyEntry(templateId, neverDefaultEntry)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 no longer finds the thing (template change propagated, search index updated)
        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void fourPolicyChainSearchIndexConsistency() {
        // 4-policy chain: globalTemplate(D) → regional(C) → department(B) → leaf(A)
        final PolicyId globalTemplateId = PolicyId.of(idGenerator().withPrefixedRandomName("globalTemplate"));
        final PolicyId regionalId = PolicyId.of(idGenerator().withPrefixedRandomName("regional"));
        final PolicyId departmentId = PolicyId.of(idGenerator().withPrefixedRandomName("department"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        // D: global template
        putPolicy(buildTemplatePolicy(globalTemplateId)).expectingHttpStatus(CREATED).fire();

        // C: regional — imports D with entriesAdditions adding subject2
        putPolicy(regionalId, buildIntermediatePolicyJson(regionalId, globalTemplateId, subject2))
                .expectingHttpStatus(CREATED).fire();

        // B: department — imports C with transitiveImports=["D"]
        final EffectedImports deptEffected = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("user-access")),
                List.of(globalTemplateId));
        final PolicyImport deptImport = PoliciesModelFactory.newPolicyImport(regionalId, deptEffected);
        final Policy departmentPolicy = buildAdminOnlyPolicy(departmentId).toBuilder()
                .setPolicyImport(deptImport)
                .build();
        putPolicy(departmentPolicy).expectingHttpStatus(CREATED).fire();

        // A: leaf — imports B with transitiveImports=["C"]
        final EffectedImports leafEffected = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("user-access")),
                List.of(regionalId));
        final PolicyImport leafImport = PoliciesModelFactory.newPolicyImport(departmentId, leafEffected);
        final Policy leafPolicy = buildAdminOnlyPolicy(leafId).toBuilder()
                .setPolicyImport(leafImport)
                .build();
        putPolicy(leafPolicy).expectingHttpStatus(CREATED).fire();

        final ThingId thingId = ThingId.of(leafId);
        putThingAndWaitForSearchIndex(thingId, leafId);

        // subject2 finds the thing through the 4-policy transitive chain
        searchThings(V_2)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        // Break the chain: remove transitiveImports from department's import of regional
        putTransitiveImports(departmentId, regionalId, JsonArray.empty())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 no longer finds the thing
        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEmpty())
                .fire();

        // Restore the chain
        final JsonArray transitiveArray = JsonArray.newBuilder()
                .add(globalTemplateId.toString())
                .build();
        putTransitiveImports(departmentId, regionalId, transitiveArray)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 finds the thing again
        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();
    }

    // ---- Helper methods ----

    private Policy buildTemplatePolicy(final PolicyId policyId) {
        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());
        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedAddition.SUBJECTS));
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .set(adminEntry)
                .set(defaultEntry)
                .build();
    }

    private JsonObject buildIntermediatePolicyJson(final PolicyId policyId, final PolicyId templateId,
            final Subject additionalSubject) {
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
                                .set("subjects", JsonObject.newBuilder()
                                        .set(defaultSubject.getId().toString(), defaultSubject.toJson())
                                        .build())
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
                                .build())
                        .set("user-access", JsonObject.newBuilder()
                                .set("subjects", JsonObject.newBuilder()
                                        .set(additionalSubject.getId().toString(),
                                                additionalSubject.toJson())
                                        .build())
                                .set("resources", JsonObject.empty())
                                .set("references", JsonArray.of(
                                        JsonObject.newBuilder()
                                                .set("import", templateId.toString())
                                                .set("entry", "DEFAULT")
                                                .build()))
                                .set("importable", "implicit")
                                .build())
                        .build())
                .build();
    }

    private Policy buildLeafPolicyWithTransitiveImports(final PolicyId leafId,
            final PolicyId intermediateId, final PolicyId templateId) {
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("user-access")),
                List.of(templateId));
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(intermediateId, effectedImports);
        return buildAdminOnlyPolicy(leafId).toBuilder()
                .setPolicyImport(policyImport)
                .build();
    }

    private Policy buildAdminOnlyPolicy(final PolicyId policyId) {
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .build();
    }

    private void putThingAndWaitForSearchIndex(final ThingId thingId, final PolicyId policyId) {
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId.toString())
                        .set("policyId", policyId.toString())
                        .build(),
                V_2)
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                        DittoAcknowledgementLabel.SEARCH_PERSISTED.toString())
                .withHeader(DittoHeaderDefinition.TIMEOUT.getKey(), "30s")
                .expectingStatusCodeSuccessful()
                .fire();
    }

    private static PutMatcher putTransitiveImports(final CharSequence policyId,
            final CharSequence importedPolicyId, final JsonArray transitiveImports) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyImport(importedPolicyId).toString() + "/transitiveImports";
        return put(dittoUrl(TestConstants.API_V_2, path), transitiveImports.toString())
                .withLogging(LOGGER, "TransitiveImports");
    }

}
