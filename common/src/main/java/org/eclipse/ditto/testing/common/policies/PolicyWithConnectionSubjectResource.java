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
package org.eclipse.ditto.testing.common.policies;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;
import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;

import java.text.MessageFormat;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.base.model.auth.AuthorizationSubject;
import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.SubjectType;
import org.eclipse.ditto.testing.common.ServiceEnvironment;
import org.eclipse.ditto.testing.common.TestSolutionSupplierRule;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a policy that adds a solution connection to the authorized subjects before the test.
 */
@NotThreadSafe
public final class PolicyWithConnectionSubjectResource extends ExternalResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PolicyWithConnectionSubjectResource.class);

    private final TestSolutionSupplierRule testSolutionSupplierRule;
    private final PoliciesHttpClientResource policiesClientResource;
    private final String connectionName;

    private PolicyId policyId;
    private Policy policy;

    private PolicyWithConnectionSubjectResource(final TestSolutionSupplierRule testSolutionSupplierRule,
            final PoliciesHttpClientResource policiesClientResource,
            final String connectionName) {

        this.testSolutionSupplierRule = testSolutionSupplierRule;
        this.policiesClientResource = policiesClientResource;
        this.connectionName = connectionName;

        policyId = null;
        policy = null;
    }

    public static PolicyWithConnectionSubjectResource newInstance(
            final TestSolutionSupplierRule testSolutionSupplierRule,
            final PoliciesHttpClientResource policiesClientResource,
            final String connectionName) {

        return new PolicyWithConnectionSubjectResource(
                checkNotNull(testSolutionSupplierRule, "testSolutionSupplierRule"),
                checkNotNull(policiesClientResource, "policiesClientResource"),
                ConditionChecker.argumentNotEmpty(connectionName, "connectionName")
        );
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        policyId = initPolicyId();
        policy = createPolicy(initPolicy());
    }

    private PolicyId initPolicyId() {
        return PolicyId.inNamespaceWithRandomName(ServiceEnvironment.DEFAULT_NAMESPACE);
    }

    private Policy initPolicy() {
        final var authorizationSubject = getAuthorizationSubject();
        return Policy.newBuilder()
                .setId(policyId)
                .forLabel("CONNECTION")
                .setSubject(authorizationSubject.getId(), SubjectType.GENERATED)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .forLabel("DEFAULT")
                .setSubject("{{ request:subjectId }}", SubjectType.GENERATED)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();
    }

    private Policy createPolicy(final Policy policy) {
        final var policiesClient = policiesClientResource.getPoliciesClient();
        final var putPolicyResult = policiesClient.putPolicy(policyId, policy, CorrelationId.random());
        final var createdPolicy = putPolicyResult.orElseThrow();
        LOGGER.info("Created policy <{}>.", createdPolicy);
        return createdPolicy;
    }

    private AuthorizationSubject getAuthorizationSubject() {
        final var testSolution = testSolutionSupplierRule.getTestSolution();
        return AuthorizationSubject.newInstance(MessageFormat.format("integration:{0}:{1}",
                testSolution.getUsername(),
                connectionName));
    }

    public PolicyId getPolicyId() {
        if (null == policyId) {
            throw new IllegalStateException("The policy ID gets only initialised by running the test.");
        } else {
            return policyId;
        }
    }

    public Policy getPolicy() {
        if (null == policy) {
            throw new IllegalStateException("The policy gets only initialised by running the test.");
        } else {
            return policy;
        }
    }

    @Override
    protected void after() {
        final var policiesClient = policiesClientResource.getPoliciesClient();
        policiesClient.deletePolicy(getPolicyId(), CorrelationId.random());
        policy = null;
        policyId = null;

        super.after();
    }

}
