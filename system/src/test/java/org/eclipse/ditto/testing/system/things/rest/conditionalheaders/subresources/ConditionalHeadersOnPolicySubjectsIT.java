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

import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.testing.system.things.rest.conditionalheaders.ConditionalHeadersPolicyOnRootResourceIT;

public class ConditionalHeadersOnPolicySubjectsIT extends AbstractConditionalHeadersOnPolicySubResourceITBase {

    @Override
    protected boolean alwaysExists() {
        return true;
    }

    @Override
    protected PutMatcher createSubResourceMatcher(final CharSequence policyId) {
        throw policySubjectsAlwaysExist();
    }

    @Override
    protected GetMatcher getSubResourceMatcher(final CharSequence policyId) {
        return getPolicyEntrySubjects(policyId, ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL);
    }

    @Override
    protected PutMatcher overwriteSubResourceMatcher(final CharSequence policyId) {
        return putPolicyEntrySubjects(policyId, ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL, subjects());
    }

    @Override
    protected DeleteMatcher deleteSubResourceMatcher(final CharSequence policyId) {
        throw policySubjectsAlwaysExist();
    }

    private UnsupportedOperationException policySubjectsAlwaysExist() {
        return new UnsupportedOperationException("policy subjects are always available");
    }

    private static Subjects subjects() {
        return Subjects.newInstance(serviceEnv.getDefaultTestingContext().getOAuthClient().getSubject(),
                serviceEnv.getTestingContext2().getOAuthClient().getSubject());
    }

}
