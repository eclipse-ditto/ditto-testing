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

package org.eclipse.ditto.testing.system.client.policies;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.policies.model.EffectedImports;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.PolicyImport;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.Resources;
import org.eclipse.ditto.testing.common.StringUtils;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.ThingsSubjectIssuer;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration test for testing CRUD on policies with the Ditto client.
 */
public final class PoliciesIT extends AbstractClientIT {

    private static final long CLIENT_TIMEOUT_SECONDS = 15;
    private static final TestingContext TESTING_CONTEXT = serviceEnv.getDefaultTestingContext();
    private static final Label SOLUTION_LABEL = Label.of("solution");
    private static final Label READER_LABEL = Label.of("reader");

    private DittoClient dittoClient;

    @Before
    public void setUp() {
        dittoClient = newDittoClientV2(serviceEnv.getDefaultTestingContext().getOAuthClient());
    }

    @Test
    public void create() throws ExecutionException, InterruptedException, TimeoutException {
        final Policy policy = buildPolicyWithRandomName(serviceEnv.getDefaultNamespaceName());
        rememberForCleanUp(policy);

        final Policy createdPolicy = dittoClient.policies()
                .create(policy)
                .toCompletableFuture()
                .get(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(createdPolicy).isEqualTo(policy);

        assertPolicyExists(policy);
    }

    @Test
    public void retrieve() throws ExecutionException, InterruptedException, TimeoutException {
        final Policy policy = buildPolicyWithRandomName(serviceEnv.getDefaultNamespaceName());
        rememberForCleanUp(policy);

        createPolicy(policy);

        final Policy readPolicy = dittoClient.policies()
                .retrieve(policy.getEntityId().get())
                .toCompletableFuture()
                .get(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(readPolicy).isEqualTo(policy);
    }

    @Test
    public void retrieveWithFieldSelector() throws ExecutionException, InterruptedException, TimeoutException {
        final Policy policy = buildPolicyWithRandomName(serviceEnv.getDefaultNamespaceName());
        rememberForCleanUp(policy);

        createPolicy(policy);

        final Policy readPolicy = dittoClient.policies()
                .retrieve(policy.getEntityId().get(), JsonFieldSelector.newInstance("_revision"))
                .toCompletableFuture()
                .get(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(readPolicy).isEqualTo(Policy.newBuilder().setRevision(1).build());
    }

    @Test
    public void update() throws ExecutionException, InterruptedException, TimeoutException {
        final Policy policy = buildPolicyWithRandomName(serviceEnv.getDefaultNamespaceName());
        rememberForCleanUp(policy);
        createPolicy(policy);

        final Policy modifiedPolicy = policy.toBuilder()
                .remove(READER_LABEL)
                .build();

        dittoClient.policies()
                .update(modifiedPolicy)
                .toCompletableFuture()
                .get(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertPolicyExists(modifiedPolicy);
    }

    @Test
    public void updateWithPolicyImports() throws ExecutionException, InterruptedException, TimeoutException {
        final Policy policy = buildPolicyWithRandomName(serviceEnv.getDefaultNamespaceName());
        final Policy importedPolicy = buildPolicyWithRandomName(serviceEnv.getDefaultNamespaceName());
        rememberForCleanUp(policy);
        rememberForCleanUp(importedPolicy);
        createPolicy(policy);
        createPolicy(importedPolicy);

        final Policy modifiedPolicy = policy.toBuilder()
                .setPolicyImport(PolicyImport.newInstance(importedPolicy.getEntityId().orElseThrow(), EffectedImports.newInstance(List.of(READER_LABEL))))
                .build();

        dittoClient.policies()
                .update(modifiedPolicy)
                .toCompletableFuture()
                .get(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertPolicyExists(modifiedPolicy);
    }

    @Test
    public void putWithCreate() throws ExecutionException, InterruptedException, TimeoutException {
        final Policy policy = buildPolicyWithRandomName(serviceEnv.getDefaultNamespaceName());
        rememberForCleanUp(policy);

        final Optional<Policy> createdPolicy = dittoClient.policies()
                .put(policy)
                .toCompletableFuture()
                .get(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(createdPolicy).contains(policy);

        assertPolicyExists(policy);
    }

    @Test
    public void putWithUpdate() throws ExecutionException, InterruptedException, TimeoutException {
        final Policy policy = buildPolicyWithRandomName(serviceEnv.getDefaultNamespaceName());
        rememberForCleanUp(policy);
        createPolicy(policy);

        final Policy policyUpdate = policy.toBuilder()
                .remove(READER_LABEL)
                .build();

        final Optional<Policy> modifiedPolicy = dittoClient.policies()
                .put(policyUpdate)
                .toCompletableFuture()
                .get(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(modifiedPolicy).isEmpty();

        assertPolicyExists(policyUpdate);
    }

    @Test
    public void delete() throws ExecutionException, InterruptedException, TimeoutException {
        final Policy policy = buildPolicyWithRandomName(serviceEnv.getDefaultNamespaceName());
        rememberForCleanUp(policy);
        createPolicy(policy);

        dittoClient.policies()
                .delete(policy.getEntityId().get())
                .toCompletableFuture()
                .get(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertPolicyDoesNotExist(policy);
    }

    private void createPolicy(final Policy policyToCreate) {
        putPolicy(policyToCreate)
                .withJWT(TESTING_CONTEXT.getOAuthClient().getAccessToken(), true)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
    }

    private void assertPolicyExists(final Policy expectedPolicy) {
        getPolicy(expectedPolicy.getEntityId().get())
                .withJWT(TESTING_CONTEXT.getOAuthClient().getAccessToken(), true)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(containsPolicy(expectedPolicy))
                .fire();
    }

    private void assertPolicyDoesNotExist(final Policy policy) {
        getPolicy(policy.getEntityId().get())
                .withJWT(TESTING_CONTEXT.getOAuthClient().getAccessToken(), true)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    private void rememberForCleanUp(final Policy policy) {
        policy.getEntityId()
                .ifPresent(policyId ->
                        rememberForCleanUp(() ->
                                deletePolicy(policyId)));
    }

    private Policy buildPolicyWithRandomName(final String namespace) {
        return Policy.newBuilder(PolicyId.inNamespaceWithRandomName(namespace))
                .forLabel(SOLUTION_LABEL)
                .setSubject(ThingsSubjectIssuer.DITTO, TESTING_CONTEXT.getOAuthClient().getClientId())
                .setResources(Resources.newInstance(
                        Resource.newInstance(PoliciesResourceType.thingResource("/"), revokedPermissions()),
                        Resource.newInstance(PoliciesResourceType.policyResource("/"), allPermissions())
                ))
                .forLabel(READER_LABEL)
                .setSubject(ThingsSubjectIssuer.DITTO, "some-reader")
                .setResources(Resources.newInstance(Resource.newInstance(
                        PoliciesResourceType.thingResource("/features"),
                        readPermissions()
                )))
                .build();
    }

    private EffectedPermissions revokedPermissions() {
        return EffectedPermissions.newInstance(Collections.emptyList(), Arrays.asList("READ", "WRITE"));
    }

    private EffectedPermissions allPermissions() {
        return EffectedPermissions.newInstance(Arrays.asList("READ", "WRITE"), Collections.emptyList());
    }

    private EffectedPermissions readPermissions() {
        return EffectedPermissions.newInstance(Collections.singletonList("READ"), Collections.emptyList());
    }

    private Matcher<String> containsPolicy(final Policy expectedPolicy) {
        return new PolicyMatcher(expectedPolicy);
    }

    private static class PolicyMatcher extends TypeSafeMatcher<String> {

        private final Policy expectedPolicy;

        private PolicyMatcher(final Policy expectedPolicy) {
            this.expectedPolicy = checkNotNull(expectedPolicy, "expectedPolicy");
        }

        @Override
        protected boolean matchesSafely(final String actualJsonString) {
            if (StringUtils.isEmpty(actualJsonString)) {
                return false;
            }

            final Policy actual = PoliciesModelFactory.newPolicy(actualJsonString);

            return expectedPolicy.equals(actual);
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("a JSON string containing the policy ")
                    .appendValue(expectedPolicy.toJsonString())
                    .appendText(".");
        }

    }

}
