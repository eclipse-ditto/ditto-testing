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
package org.eclipse.ditto.testing.system.search.sync.common.things;


import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.attribute;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.and;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.PolicyImport;
import org.eclipse.ditto.policies.model.PolicyImports;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.SearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchResponseExtractors;
import org.eclipse.ditto.testing.system.search.sync.common.SearchSyncTestConfig;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SearchResult;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test for the synchronization mechanism for things in the context of changes to the thing's schema version.
 */
public final class ThingPolicyUpdateIT extends SearchIntegrationTest {

    private static final SearchSyncTestConfig CONF = SearchSyncTestConfig.getInstance();

    private static final String KNOWN_ATTR_KEY = "knownAttrKey";
    private static final String KNOWN_ATTR_VAL = "knownAttrVal";
    private static final Attributes KNOWN_ATTRIBUTES = ThingsModelFactory.newAttributes(
            JsonObject.newBuilder().set(KNOWN_ATTR_KEY, KNOWN_ATTR_VAL).build()
    );
    private static final String KNOWN_LABEL = "theLabel";

    private final JsonSchemaVersion apiVersion = JsonSchemaVersion.V_2;

    @Test
    public void createDeletePolicyRecreatePolicy() {
        final ThingId thingId = thingId("deletePolicyRecreatePolicy");
        final PolicyId policyId = policyId("deletePolicyRecreatePolicy");
        final EffectedPermissions effectedPermissions = EffectedPermissions.newInstance(
                PoliciesModelFactory.newPermissions("READ", "WRITE"),
                PoliciesModelFactory.noPermissions());
        final Policy policy = Policy.newBuilder(policyId)
                .setSubjectFor(KNOWN_LABEL, serviceEnv.getDefaultTestingContext().getOAuthClient().getSubject())
                .setResourceFor(KNOWN_LABEL, Resource.newInstance("thing", "/", effectedPermissions))
                .setResourceFor(KNOWN_LABEL, Resource.newInstance("policy", "/", effectedPermissions))
                .build();
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setAttributes(KNOWN_ATTRIBUTES)
                .build();

        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putThing(apiVersion.toInt(), thing, apiVersion)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(idFilter(thingId)).getItems().isEmpty());

        // should not find thing after deleting the policy
        deletePolicy(policyId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> search(idFilter(thingId)).getItems().isEmpty());

        // should find the thing after policy is recreated
        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(idFilter(thingId)).getItems().isEmpty());
    }

    @Test
    public void restorePolicyWithDifferentContent() {
        final ThingId thingId = thingId("restorePolicyWithDifferentContent");
        final PolicyId policyId = policyId("restorePolicyWithDifferentContent");
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setAttributes(KNOWN_ATTRIBUTES)
                .build();
        final SearchFilter forId = idFilter(thingId);
        final SearchFilter forStringAttribute =
                attribute(KNOWN_ATTR_KEY).eq(KNOWN_ATTR_VAL);
        final SearchFilter forIdAndStringAttribute = and(forId, forStringAttribute);

        // GIVEN: thing is in search index
        putPolicy(fullAccessPolicy(policyId))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putThing(apiVersion.toInt(), thing, apiVersion)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(forIdAndStringAttribute).getItems().isEmpty());

        // WHEN: policy is deleted
        deletePolicy(policyId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        // THEN: thing is deleted from index
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> search(forIdAndStringAttribute).getItems().isEmpty());

        // WHEN: policy is restored but string attribute is write-only
        putPolicy(attrWriteOnlyPolicy(policyId, KNOWN_ATTR_KEY))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        // THEN: should find the thing by ID but not by the string attribute
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() ->
                        !search(forId).getItems().isEmpty() &&
                                search(forIdAndStringAttribute).getItems().isEmpty());
    }

    @Test
    public void restoreImportedPolicyWithDifferentContent() {
        final ThingId thingId = thingId("restoreImportedPolicyWithDifferentContent");
        final PolicyId policyId = policyId("restoreImportedPolicyWithDifferentContent");
        final PolicyId importedPolicyId = policyId("restoreImportedPolicyWithDifferentContent");
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setAttributes(KNOWN_ATTRIBUTES)
                .build();
        final SearchFilter forId = idFilter(thingId);
        final SearchFilter forStringAttribute =
                attribute(KNOWN_ATTR_KEY).eq(KNOWN_ATTR_VAL);
        final SearchFilter forIdAndStringAttribute = and(forId, forStringAttribute);

        // GIVEN: thing is in search index
        putPolicy(fullAccessPolicy(importedPolicyId))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putPolicy(emptyPolicy(policyId, importedPolicyId))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putThing(apiVersion.toInt(), thing, apiVersion)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(forIdAndStringAttribute).getItems().isEmpty());

        // WHEN: policy is deleted
        deletePolicy(importedPolicyId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        // THEN: thing is deleted from index
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> search(forIdAndStringAttribute).getItems().isEmpty());

        // WHEN: policy is restored but string attribute is write-only
        putPolicy(attrWriteOnlyPolicy(importedPolicyId, KNOWN_ATTR_KEY))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        // THEN: should find the thing by ID but not by the string attribute
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() ->
                        !search(forId).getItems().isEmpty() &&
                                search(forIdAndStringAttribute).getItems().isEmpty());
    }

    @Test
    public void changePolicyId() {
        final ThingId thingId = thingId("changePolicyId");
        final PolicyId fullAccessPolicyId = policyId("changePolicyId-fullAccess");
        final PolicyId writeOnlyPolicyId = policyId("changePolicyId-writeOnly");
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(fullAccessPolicyId)
                .setAttributes(KNOWN_ATTRIBUTES)
                .build();
        final SearchFilter forId = idFilter(thingId);
        final SearchFilter forStringAttribute =
                attribute(KNOWN_ATTR_KEY).eq(KNOWN_ATTR_VAL);
        final SearchFilter forIdAndStringAttribute = and(forId, forStringAttribute);

        // GIVEN: thing is in search index
        putPolicy(fullAccessPolicy(fullAccessPolicyId))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putPolicy(attrWriteOnlyPolicy(writeOnlyPolicyId, KNOWN_ATTR_KEY))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putThing(apiVersion.toInt(), thing, apiVersion)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(forIdAndStringAttribute).getItems().isEmpty());

        // WHEN: policy ID is changed to the write-only policy
        final Thing newThing = thing.setPolicyId(writeOnlyPolicyId);
        putThing(apiVersion.toInt(), newThing, apiVersion)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // THEN: should find the thing by ID but not by the string attribute
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() ->
                        !search(forId).getItems().isEmpty() &&
                                search(forIdAndStringAttribute).getItems().isEmpty());
    }

    @Test
    public void changeImportedPolicyId() {
        final ThingId thingId = thingId("changeImportedPolicyId");
        final PolicyId fullAccessPolicyId = policyId("changeImportedPolicyId-fullAccess");
        final PolicyId writeOnlyPolicyId = policyId("changeImportedPolicyId-writeOnly");
        final PolicyId policyId = policyId("changeImportedPolicyId");
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setAttributes(KNOWN_ATTRIBUTES)
                .build();
        final SearchFilter forId = idFilter(thingId);
        final SearchFilter forStringAttribute =
                attribute(KNOWN_ATTR_KEY).eq(KNOWN_ATTR_VAL);
        final SearchFilter forIdAndStringAttribute = and(forId, forStringAttribute);

        // GIVEN: thing is in search index
        putPolicy(fullAccessPolicy(fullAccessPolicyId))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putPolicy(attrWriteOnlyPolicy(writeOnlyPolicyId, KNOWN_ATTR_KEY))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putPolicy(emptyPolicy(policyId, fullAccessPolicyId))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putThing(apiVersion.toInt(), thing, apiVersion)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(forIdAndStringAttribute).getItems().isEmpty());

        // WHEN: imported policy ID is changed to the write-only policy
        putPolicy(emptyPolicy(policyId, writeOnlyPolicyId))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // THEN: should find the thing by ID but not by the string attribute
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() ->
                        !search(forId).getItems().isEmpty() &&
                                search(forIdAndStringAttribute).getItems().isEmpty());
    }

    @Test
    public void changePolicyContent() {
        final ThingId thingId = thingId("changePolicyContent");
        final PolicyId policyId = policyId("changePolicyContent");
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setAttributes(KNOWN_ATTRIBUTES)
                .build();
        final SearchFilter forId = idFilter(thingId);
        final SearchFilter forStringAttribute =
                attribute(KNOWN_ATTR_KEY).eq(KNOWN_ATTR_VAL);
        final SearchFilter forIdAndStringAttribute = and(forId, forStringAttribute);

        // GIVEN: thing is in search index
        putPolicy(fullAccessPolicy(policyId))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putThing(apiVersion.toInt(), thing, apiVersion)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(forIdAndStringAttribute).getItems().isEmpty());

        // WHEN: policy is changed to the write-only policy
        putPolicy(attrWriteOnlyPolicy(policyId, KNOWN_ATTR_KEY))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // THEN: should find the thing by ID but not by the string attribute
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() ->
                        !search(forId).getItems().isEmpty() &&
                                search(forIdAndStringAttribute).getItems().isEmpty());
    }

    @Test
    public void changeImportedPolicyContent() {
        final ThingId thingId = thingId("changeImportedPolicyContent");
        final PolicyId policyId = policyId("changeImportedPolicyContent");
        final PolicyId importedPolicyId = policyId("changeImportedPolicyContent");
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setAttributes(KNOWN_ATTRIBUTES)
                .build();
        final SearchFilter forId = idFilter(thingId);
        final SearchFilter forStringAttribute =
                attribute(KNOWN_ATTR_KEY).eq(KNOWN_ATTR_VAL);
        final SearchFilter forIdAndStringAttribute = and(forId, forStringAttribute);

        // GIVEN: thing is in search index
        putPolicy(fullAccessPolicy(importedPolicyId))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putPolicy(emptyPolicy(policyId, importedPolicyId))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putThing(apiVersion.toInt(), thing, apiVersion)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(forIdAndStringAttribute).getItems().isEmpty());

        // WHEN: policy is changed to the write-only policy
        putPolicy(attrWriteOnlyPolicy(importedPolicyId, KNOWN_ATTR_KEY))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // THEN: should find the thing by ID but not by the string attribute
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() ->
                        !search(forId).getItems().isEmpty() &&
                                search(forIdAndStringAttribute).getItems().isEmpty());
    }

    @Test(timeout = Long.MAX_VALUE)
    @Ignore("transform into performance test")
    public void changeBy100kPoliciesImportedPolicyContent() {
        final PolicyId importedPolicyId = policyId("changeBy100kPoliciesImportedPolicyContent");
        putPolicy(fullAccessPolicy(importedPolicyId))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final List<ThingId> thingIds = new ArrayList<>();
        final List<CompletableFuture<Boolean>> completionStages = new ArrayList<>();
        final int numberOfThings = 1_000; //TODO: this should be 100_000 but would take too much time. Maybe gattling is a better alternative
        // GIVEN: thing is in search index
        for (int i = 0; i < numberOfThings; i++) {
            final CompletableFuture<Boolean> createThingCs =
                    createThingWithImportingPolicy(importedPolicyId, i, numberOfThings)
                            .thenApply(thingIds::add)
                            .toCompletableFuture();
            completionStages.add(createThingCs);
        }

        CompletableFuture.allOf(completionStages.toArray(new CompletableFuture[0])).join();
        assertThat(thingIds).hasSize(numberOfThings);

        final SearchFilter forId = idsFilter(thingIds);
        final SearchFilter forStringAttribute =
                attribute(KNOWN_ATTR_KEY).eq(KNOWN_ATTR_VAL);
        final SearchFilter forIdAndStringAttribute = and(forId, forStringAttribute);

        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .untilAsserted(() -> assertThat(search(forIdAndStringAttribute).getItems()).hasSizeGreaterThanOrEqualTo(
                        numberOfThings));

        // WHEN: policy is changed to the write-only policy
        putPolicy(attrWriteOnlyPolicy(importedPolicyId, KNOWN_ATTR_KEY))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // THEN: should find the thing by ID but not by the string attribute
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() ->
                        !search(forId).getItems().isEmpty() &&
                                search(forIdAndStringAttribute).getItems().isEmpty());
    }

    private CompletionStage<ThingId> createThingWithImportingPolicy(final PolicyId importedPolicyId, final int count,
            final int targetNumber) {
        return CompletableFuture.supplyAsync(() -> {
            final ThingId thingId = thingId("changeBy100kPoliciesImportedPolicyContent");
            final PolicyId policyId = policyId("changeBy100kPoliciesImportedPolicyContent");
            final Thing thing = ThingsModelFactory.newThingBuilder()
                    .setId(thingId)
                    .setPolicyId(policyId)
                    .setAttributes(KNOWN_ATTRIBUTES)
                    .build();
            putPolicy(emptyPolicy(policyId, importedPolicyId))
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire();
            putThing(apiVersion.toInt(), thing, apiVersion)
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire();
            IntegrationTest.LOGGER.info("Created thing <{}> of <{}>.", count, targetNumber);
            return thingId;
        });
    }

    @Test
    public void createDeleteImportedPolicyRecreateImportedPolicy() {
        final ThingId thingId = thingId("deleteImportedPolicyRecreateImportedPolicy");
        final PolicyId policyId = policyId("deleteImportedPolicyRecreateImportedPolicy");
        final PolicyId importedPolicyId = policyId("deleteImportedPolicyRecreateImportedPolicy");
        final EffectedPermissions effectedPermissions = EffectedPermissions.newInstance(
                PoliciesModelFactory.newPermissions("READ", "WRITE"),
                PoliciesModelFactory.noPermissions());
        final Policy policy = Policy.newBuilder(importedPolicyId)
                .setSubjectFor(KNOWN_LABEL, serviceEnv.getDefaultTestingContext().getOAuthClient().getSubject())
                .setResourceFor(KNOWN_LABEL, Resource.newInstance("thing", "/", effectedPermissions))
                .setResourceFor(KNOWN_LABEL, Resource.newInstance("policy", "/", effectedPermissions))
                .build();
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setAttributes(KNOWN_ATTRIBUTES)
                .build();

        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putPolicy(emptyPolicy(policyId, importedPolicyId))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        putThing(apiVersion.toInt(), thing, apiVersion)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(idFilter(thingId)).getItems().isEmpty());

        // should not find thing after deleting the policy
        deletePolicy(importedPolicyId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> search(idFilter(thingId)).getItems().isEmpty());

        // should find the thing after policy is recreated
        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        Awaitility.await()
                .atMost(CONF.getThingsSyncWaitTimeout())
                .until(() -> !search(idFilter(thingId)).getItems().isEmpty());
    }

    private static Policy fullAccessPolicy(final PolicyId policyId) {
        final EffectedPermissions readWrite = EffectedPermissions.newInstance(
                PoliciesModelFactory.newPermissions("READ", "WRITE"),
                PoliciesModelFactory.noPermissions());
        return Policy.newBuilder(policyId)
                .setSubjectFor(KNOWN_LABEL, serviceEnv.getDefaultTestingContext().getOAuthClient().getSubject())
                .setResourceFor(KNOWN_LABEL, Resource.newInstance("thing", "/", readWrite))
                .setResourceFor(KNOWN_LABEL, Resource.newInstance("policy", "/", readWrite))
                .build();
    }

    private static Policy emptyPolicy(final PolicyId policyId, final PolicyId... importedPolicyIds) {
        final List<PolicyImport> policyImportList = Arrays.stream(importedPolicyIds)
                .map(PoliciesModelFactory::newPolicyImport)
                .collect(Collectors.toList());
        return Policy.newBuilder(policyId)
                .setPolicyImports(PolicyImports.newInstance(policyImportList))
                .build();
    }

    private static Policy attrWriteOnlyPolicy(final PolicyId policyId, final String attributeKey,
            final PolicyId... importedPolicyIds) {
        final String writeOnlyAttr = "/attributes/" + attributeKey;
        final EffectedPermissions readWrite = EffectedPermissions.newInstance(
                PoliciesModelFactory.newPermissions("READ", "WRITE"),
                PoliciesModelFactory.noPermissions());
        final EffectedPermissions writeOnly = EffectedPermissions.newInstance(
                PoliciesModelFactory.newPermissions("WRITE"),
                PoliciesModelFactory.newPermissions("READ"));
        final List<PolicyImport> policyImportList = Arrays.stream(importedPolicyIds)
                .map(PoliciesModelFactory::newPolicyImport)
                .collect(Collectors.toList());
        return Policy.newBuilder(policyId)
                .setPolicyImports(PolicyImports.newInstance(policyImportList))
                .setSubjectFor(KNOWN_LABEL, serviceEnv.getDefaultTestingContext().getOAuthClient().getSubject())
                .setResourceFor(KNOWN_LABEL, Resource.newInstance("thing", "/", readWrite))
                .setResourceFor(KNOWN_LABEL, Resource.newInstance("policy", "/", readWrite))
                .setResourceFor(KNOWN_LABEL, Resource.newInstance("thing", writeOnlyAttr, writeOnly))
                .build();
    }

    private SearchResult search(final SearchFilter searchFilter) {
        return SearchResponseExtractors.asSearchResult(searchThings(apiVersion).filter(searchFilter).fire().asString());
    }
}
