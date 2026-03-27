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

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.Permissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.ResourceKey;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ResourcePathBuilder;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for the dedicated policy entry sub-resource HTTP route:
 * <ul>
 *   <li>GET/PUT {@code /entries/{label}/namespaces}</li>
 * </ul>
 */
public final class PolicyEntryNamespacesSubResourceIT extends IntegrationTest {

    private PolicyId policyId;
    private Subject defaultSubject;
    private Subject subject2;

    @Before
    public void setUp() {
        policyId = PolicyId.of(idGenerator().withPrefixedRandomName("nsSubResource"));
        defaultSubject = serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject();
        subject2 = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
    }

    @Test
    public void getAndPutPolicyEntryNamespaces() {
        final Policy policy = buildPolicyWithNamespaceScopedEntry(policyId,
                Arrays.asList("com.acme", "com.acme.*"));
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        // GET namespaces and verify initial value
        getPolicyEntryNamespaces(policyId, "SCOPED")
                .expectingBody(containsOnly(
                        JsonArray.newBuilder().add("com.acme").add("com.acme.*").build()))
                .expectingHttpStatus(OK)
                .fire();

        // PUT namespaces with new value
        final JsonArray updatedNamespaces = JsonArray.newBuilder()
                .add("org.example")
                .add("org.example.*")
                .build();
        putPolicyEntryNamespaces(policyId, "SCOPED", updatedNamespaces)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // GET namespaces again and verify change
        getPolicyEntryNamespaces(policyId, "SCOPED")
                .expectingBody(containsOnly(updatedNamespaces))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void getNamespacesReturns404WhenNeverConfigured() {
        // Create policy with an entry that has no namespaces field
        final Policy policy = buildPolicyWithoutNamespaces(policyId);
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        // GET namespaces should return 404 when never configured
        getPolicyEntryNamespaces(policyId, "DEFAULT")
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    @Test
    public void getNamespacesReturnsEmptyArrayWhenExplicitlySet() {
        // Create policy with entry that has namespaces explicitly set to empty
        final Policy policy = buildPolicyWithNamespaceScopedEntry(policyId, Collections.emptyList());
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        // GET namespaces should return 200 with empty array
        getPolicyEntryNamespaces(policyId, "SCOPED")
                .expectingBody(containsOnly(JsonArray.empty()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putNamespacesOnPreviouslyUnscopedEntry() {
        // Create policy without namespaces (global entry)
        final Policy policy = buildPolicyWithoutNamespaces(policyId);
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        // Verify namespaces sub-resource returns 404
        getPolicyEntryNamespaces(policyId, "DEFAULT")
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // PUT namespaces to restrict the entry
        final JsonArray namespaces = JsonArray.newBuilder().add("com.acme").build();
        putPolicyEntryNamespaces(policyId, "DEFAULT", namespaces)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Verify namespaces are now set
        getPolicyEntryNamespaces(policyId, "DEFAULT")
                .expectingBody(containsOnly(namespaces))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putNamespacesChangesEnforcement() {
        // Create policy with namespace-scoped entry for "com.acme" + "com.acme.*"
        final Policy policy = buildPolicyWithNamespaceScopedEntry(policyId,
                Arrays.asList("com.acme", "com.acme.*"));
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        final ThingId acmeThingId = ThingId.of("com.acme", idGenerator().withRandomName());
        final ThingId otherThingId = ThingId.of("org.example", idGenerator().withRandomName());

        putThing(TestConstants.API_V_2,
                Thing.newBuilder().setId(acmeThingId).setPolicyId(policyId).build(),
                JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        putThing(TestConstants.API_V_2,
                Thing.newBuilder().setId(otherThingId).setPolicyId(policyId).build(),
                JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2 can access com.acme thing, but not org.example thing
        getThing(TestConstants.API_V_2, acmeThingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        getThing(TestConstants.API_V_2, otherThingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        // Change namespaces to "org.example" via sub-resource PUT
        final JsonArray updatedNamespaces = JsonArray.newBuilder().add("org.example").build();
        putPolicyEntryNamespaces(policyId, "SCOPED", updatedNamespaces)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Now subject2 can access org.example thing, but not com.acme thing
        getThing(TestConstants.API_V_2, otherThingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        getThing(TestConstants.API_V_2, acmeThingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void putEmptyNamespacesRestoresGlobalAccess() {
        // Create policy with namespace-scoped entry for "com.acme"
        final Policy policy = buildPolicyWithNamespaceScopedEntry(policyId,
                Arrays.asList("com.acme"));
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        final ThingId otherThingId = ThingId.of("org.example", idGenerator().withRandomName());

        putThing(TestConstants.API_V_2,
                Thing.newBuilder().setId(otherThingId).setPolicyId(policyId).build(),
                JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2 cannot access org.example thing (scoped to com.acme)
        getThing(TestConstants.API_V_2, otherThingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        // Set namespaces to empty array to restore global access
        putPolicyEntryNamespaces(policyId, "SCOPED", JsonArray.empty())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Now subject2 can access org.example thing (entry is global)
        getThing(TestConstants.API_V_2, otherThingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    // --- Helper methods for building policies ---

    private Policy buildPolicyWithNamespaceScopedEntry(final PolicyId policyId,
            final List<String> namespacePatterns) {

        final EffectedPermissions ownerPermissions = EffectedPermissions.newInstance(
                PoliciesModelFactory.newPermissions(READ, WRITE), Permissions.none());
        final PolicyEntry ownerEntry = PoliciesModelFactory.newPolicyEntry(
                Label.of("OWNER"),
                PoliciesModelFactory.newSubjects(defaultSubject),
                PoliciesModelFactory.newResources(
                        Resource.newInstance(ResourceKey.newInstance("policy:"), ownerPermissions),
                        Resource.newInstance(ResourceKey.newInstance("thing:"), ownerPermissions)));

        final PolicyEntry scopedEntry = PoliciesModelFactory.newPolicyEntry(
                Label.of("SCOPED"),
                PoliciesModelFactory.newSubjects(subject2),
                PoliciesModelFactory.newResources(
                        Resource.newInstance(ResourceKey.newInstance("thing:"),
                                EffectedPermissions.newInstance(
                                        PoliciesModelFactory.newPermissions(READ), Permissions.none()))),
                namespacePatterns,
                ImportableType.IMPLICIT,
                Collections.emptySet());

        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .set(ownerEntry)
                .set(scopedEntry)
                .build();
    }

    private Policy buildPolicyWithoutNamespaces(final PolicyId policyId) {
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("OWNER")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .forLabel("DEFAULT")
                .setSubject(subject2)
                .setGrantedPermissions(thingResource("/"), READ)
                .build();
    }

    // --- Helper methods for sub-resource HTTP operations ---

    private static GetMatcher getPolicyEntryNamespaces(final CharSequence policyId,
            final CharSequence label) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyEntry(label).toString() + "/namespaces";
        return get(dittoUrl(TestConstants.API_V_2, path))
                .withLogging(LOGGER, "PolicyEntryNamespaces");
    }

    private static PutMatcher putPolicyEntryNamespaces(final CharSequence policyId,
            final CharSequence label, final JsonArray namespaces) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyEntry(label).toString() + "/namespaces";
        return put(dittoUrl(TestConstants.API_V_2, path), namespaces.toString())
                .withLogging(LOGGER, "PolicyEntryNamespaces");
    }

}
