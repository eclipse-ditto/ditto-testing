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

import java.util.Arrays;
import java.util.List;

import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.ResourceKey;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.testing.system.things.rest.conditionalheaders.ConditionalHeadersPolicyOnRootResourceIT;

public class ConditionalHeadersOnPolicyResourceIT extends AbstractConditionalHeadersOnPolicySubResourceITBase {

    private static final String RESOURCE_PATH = "thing:/";
    private final List<String> GRANTED_PERMISSIONS_1 = Arrays.asList("READ", "WRITE", "ADMINISTRATE");
    private final List<String> GRANTED_PERMISSIONS_2 = Arrays.asList("READ", "WRITE");

    @Override
    protected boolean alwaysExists() {
        return false;
    }

    @Override
    protected PutMatcher createSubResourceMatcher(final CharSequence policyId) {
        return putPolicyEntryResource(policyId, ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL, RESOURCE_PATH, resource(GRANTED_PERMISSIONS_1));
    }

    @Override
    protected GetMatcher getSubResourceMatcher(final CharSequence policyId) {
        return getPolicyEntryResource(policyId, ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL, RESOURCE_PATH);
    }

    @Override
    protected PutMatcher overwriteSubResourceMatcher(final CharSequence policyId) {
        return putPolicyEntryResource(policyId, ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL, RESOURCE_PATH, resource(GRANTED_PERMISSIONS_2));
    }

    @Override
    protected DeleteMatcher deleteSubResourceMatcher(final CharSequence policyId) {
        return deletePolicyEntryResource(policyId, ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL, RESOURCE_PATH);
    }

    private static Resource resource(final List<String> grantedPermissions) {
        return Resource.newInstance(ResourceKey.newInstance(RESOURCE_PATH),
                EffectedPermissions.newInstance(grantedPermissions, null));
    }
}
