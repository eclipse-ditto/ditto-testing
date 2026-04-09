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
package org.eclipse.ditto.testing.system.search.things;

import static org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.policyResource;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.thingResource;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEmpty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;
import static org.eclipse.ditto.things.api.Permission.READ;
import static org.eclipse.ditto.things.api.Permission.WRITE;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.eclipse.ditto.base.model.json.FieldType;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
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
import org.eclipse.ditto.testing.common.SearchIntegrationTest;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * Search integration tests verifying that the search index correctly reflects access granted through
 * policy import aliases. When a subject is added via an alias (which fans out to entries additions targets),
 * the search index must be updated so that the subject can find the Thing.
 */
public final class QueryThingsWithImportsAliasesIT extends SearchIntegrationTest {

    private static final ConditionFactory AWAITILITY_SEARCH_CONFIG =
            Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(3, TimeUnit.SECONDS);

    private static final Label ALIAS_LABEL = Label.of("operator");
    private static final Label TARGET_LABEL_1 = Label.of("operator-reactor");
    private static final Label TARGET_LABEL_2 = Label.of("operator-turbine");

    private AuthClient secondClient;

    @Before
    public void setUp() {
        final TestingContext testingContext =
                TestingContext.withGeneratedMockClient(serviceEnv.getTestingContext2().getSolution(), TEST_CONFIG);
        secondClient = testingContext.getOAuthClient();
    }

    @Test
    public void thingNotVisibleInSearchBeforeSubjectAddedViaAlias() {
        // Create Thing with importing policy + alias, but user2 is NOT yet a subject
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId thingPolicyId = PolicyId.of(thingId);

        final Policy templatePolicy = buildTemplatePolicy(
                PolicyId.of(idGenerator().withPrefixedRandomName("tmpl")));
        putPolicy(templatePolicy).fire();

        final PolicyId tmplPolicyId = templatePolicy.getEntityId().orElseThrow();
        final Policy thingPolicy = buildImportingPolicyWithAlias(thingPolicyId, tmplPolicyId);

        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonFactory.newPointer("status"), JsonFactory.newValue("active"))
                .build();

        persistThingWithPolicy(thing, thingPolicy);

        // user2 should NOT see the Thing in search
        searchThings(V_2)
                .filter(idFilter(thingId))
                .withJWT(secondClient.getAccessToken())
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void thingBecomesVisibleInSearchAfterSubjectAddedViaAlias() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId thingPolicyId = PolicyId.of(thingId);

        final Policy templatePolicy = buildTemplatePolicy(
                PolicyId.of(idGenerator().withPrefixedRandomName("tmpl")));
        putPolicy(templatePolicy).fire();

        final PolicyId tmplPolicyId = templatePolicy.getEntityId().orElseThrow();
        final Policy thingPolicy = buildImportingPolicyWithAlias(thingPolicyId, tmplPolicyId);

        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonFactory.newPointer("status"), JsonFactory.newValue("active"))
                .build();

        persistThingWithPolicy(thing, thingPolicy);

        // Add user2 via the alias — fans out to both entries additions targets
        final Subject user2Subject = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
        putPolicyEntrySubject(thingPolicyId, ALIAS_LABEL.toString(),
                user2Subject.getId().toString(), user2Subject)
                .fire();

        // user2 should now see the Thing in search (wait for eventual consistency)
        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(idFilter(thingId))
                .withJWT(secondClient.getAccessToken())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();
    }

    @Test
    public void thingDisappearsFromSearchAfterSubjectRemovedViaAlias() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId thingPolicyId = PolicyId.of(thingId);

        final Policy templatePolicy = buildTemplatePolicy(
                PolicyId.of(idGenerator().withPrefixedRandomName("tmpl")));
        putPolicy(templatePolicy).fire();

        final PolicyId tmplPolicyId = templatePolicy.getEntityId().orElseThrow();
        final Policy thingPolicy = buildImportingPolicyWithAlias(thingPolicyId, tmplPolicyId);

        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonFactory.newPointer("status"), JsonFactory.newValue("active"))
                .build();

        // Create with user2 already as subject in entries additions
        final Subject user2Subject = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
        final Policy thingPolicyWithUser2 = addUser2ToEntriesAdditions(thingPolicy, tmplPolicyId, user2Subject);
        persistThingWithPolicy(thing, thingPolicyWithUser2);

        // user2 can see the Thing in search
        searchThings(V_2)
                .filter(idFilter(thingId))
                .withJWT(secondClient.getAccessToken())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        // Remove user2 via the alias
        deletePolicyEntrySubject(thingPolicyId, ALIAS_LABEL.toString(),
                user2Subject.getId().toString())
                .fire();

        // user2 should no longer see the Thing (wait for eventual consistency)
        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(idFilter(thingId))
                .withJWT(secondClient.getAccessToken())
                .expectingBody(isEmpty())
                .fire();
    }

    // --- Helpers ---

    private static Policy buildTemplatePolicy(final PolicyId templateId) {
        return PoliciesModelFactory.newPolicyBuilder(templateId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject())
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setImportable(ImportableType.NEVER)
                .forLabel(TARGET_LABEL_1.toString())
                .setSubject(defaultSubject())
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .setImportable(ImportableType.EXPLICIT)
                .forLabel(TARGET_LABEL_2.toString())
                .setSubject(defaultSubject())
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .setImportable(ImportableType.EXPLICIT)
                .build();
    }

    private static Policy buildImportingPolicyWithAlias(final PolicyId policyId, final PolicyId tmplPolicyId) {
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
        final ImportsAlias alias = PoliciesModelFactory.newImportsAlias(ALIAS_LABEL, targets);

        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject())
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .setPolicyImports(PoliciesModelFactory.newPolicyImports(Collections.singletonList(pImport)))
                .setImportsAliases(PoliciesModelFactory.newImportsAliases(Collections.singletonList(alias)))
                .build();
    }

    private static Policy addUser2ToEntriesAdditions(final Policy policy, final PolicyId tmplPolicyId,
            final Subject user2Subject) {
        // Rebuild the policy with user2 already in entries additions
        final EntryAddition addition1 = PoliciesModelFactory.newEntryAddition(TARGET_LABEL_1,
                org.eclipse.ditto.policies.model.Subjects.newInstance(user2Subject), null);
        final EntryAddition addition2 = PoliciesModelFactory.newEntryAddition(TARGET_LABEL_2,
                org.eclipse.ditto.policies.model.Subjects.newInstance(user2Subject), null);
        final EntriesAdditions entriesAdditions =
                PoliciesModelFactory.newEntriesAdditions(Arrays.asList(addition1, addition2));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                Arrays.asList(TARGET_LABEL_1, TARGET_LABEL_2), entriesAdditions);
        final PolicyImport pImport = PoliciesModelFactory.newPolicyImport(tmplPolicyId, effectedImports);

        final List<ImportsAliasTarget> targets = Arrays.asList(
                PoliciesModelFactory.newImportsAliasTarget(tmplPolicyId, TARGET_LABEL_1),
                PoliciesModelFactory.newImportsAliasTarget(tmplPolicyId, TARGET_LABEL_2));
        final ImportsAlias alias = PoliciesModelFactory.newImportsAlias(ALIAS_LABEL, targets);

        return policy.toBuilder()
                .setPolicyImports(PoliciesModelFactory.newPolicyImports(Collections.singletonList(pImport)))
                .setImportsAliases(PoliciesModelFactory.newImportsAliases(Collections.singletonList(alias)))
                .build();
    }

    private static Subject defaultSubject() {
        return serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject();
    }

    private static void persistThingWithPolicy(final Thing thing, final Policy policy) {
        final JsonObject thingJson = thing.toJson(V_2, FieldType.notHidden())
                .setAll(policy.toInlinedJson(V_2, FieldType.notHidden()));
        persistThingsAndWaitTillAvailable(thingJson.toString(), 1L, V_2, serviceEnv.getDefaultTestingContext());
    }

}
