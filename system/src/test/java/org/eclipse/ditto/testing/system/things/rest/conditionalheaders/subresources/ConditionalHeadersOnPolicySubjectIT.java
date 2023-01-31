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
package org.eclipse.ditto.testing.system.things.rest.conditionalheaders.subresources;

import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectType;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.testing.system.things.rest.conditionalheaders.ConditionalHeadersPolicyOnRootResourceIT;

public class ConditionalHeadersOnPolicySubjectIT extends AbstractConditionalHeadersOnPolicySubResourceITBase {

    private static final String SUBJECT_TYPE_1 = "ditto-userid";
    private static final String SUBJECT_TYPE_2 = "ditto-groupid";

    public ConditionalHeadersOnPolicySubjectIT() {
    }

    @Override
    protected boolean alwaysExists() {
        return false;
    }

    @Override
    protected PutMatcher createSubResourceMatcher(final CharSequence policyId) {
        return putPolicyEntrySubject(policyId, ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL, createSubjectId(), subject(SUBJECT_TYPE_1));
    }

    @Override
    protected GetMatcher getSubResourceMatcher(final CharSequence policyId) {
        return getPolicyEntrySubject(policyId, ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL, createSubjectId());
    }

    @Override
    protected PutMatcher overwriteSubResourceMatcher(final CharSequence policyId) {
        return putPolicyEntrySubject(policyId, ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL, createSubjectId(), subject(SUBJECT_TYPE_2));
    }

    @Override
    protected DeleteMatcher deleteSubResourceMatcher(final CharSequence policyId) {
        return deletePolicyEntrySubject(policyId, ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL, createSubjectId());
    }

    private String createSubjectId() {
        return serviceEnv.getTestingContext2().getOAuthClient().getSubject().getId().toString();
    }

    private Subject subject(final String type) {
        return Subject.newInstance(createSubjectId(), SubjectType.newInstance(type));
    }
}
