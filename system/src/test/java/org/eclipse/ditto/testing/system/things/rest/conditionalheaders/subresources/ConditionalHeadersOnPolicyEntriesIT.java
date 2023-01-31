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

import static java.util.Collections.singletonList;

import java.util.Arrays;
import java.util.List;

import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.ResourceKey;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.testing.system.things.rest.conditionalheaders.ConditionalHeadersPolicyOnRootResourceIT;

public class ConditionalHeadersOnPolicyEntriesIT extends AbstractConditionalHeadersOnPolicySubResourceITBase {

    @Override
    protected boolean alwaysExists() {
        return true;
    }

    @Override
    protected PutMatcher createSubResourceMatcher(final CharSequence policyId) {
        throw policyEntriesAlwaysExist();
    }

    @Override
    protected GetMatcher getSubResourceMatcher(final CharSequence policyId) {
        return getPolicyEntries(policyId);
    }

    @Override
    protected PutMatcher overwriteSubResourceMatcher(final CharSequence policyId) {
        return putPolicyEntries(policyId, createPolicyEntries());
    }

    @Override
    protected DeleteMatcher deleteSubResourceMatcher(final CharSequence policyId) {
        throw policyEntriesAlwaysExist();
    }

    private UnsupportedOperationException policyEntriesAlwaysExist() {
        return new UnsupportedOperationException("policy entries is always available");
    }

    private JsonObject createPolicyEntries() {
        return JsonObject.newBuilder().set(ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL, policyEntry().toJson()).build();
    }

    private static PolicyEntry policyEntry() {
        return PolicyEntry.newInstance(ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL,
                Arrays.asList(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject(),
                        serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject()),
                singletonList(resource()));
    }

    private static Resource resource() {
        final List<String> grantedPermissions = Arrays.asList("READ", "WRITE", "ADMINISTRATE");
        return Resource.newInstance(ResourceKey.newInstance(ConditionalHeadersPolicyOnRootResourceIT.RESOURCE_PATH),
                EffectedPermissions.newInstance(grantedPermissions, null));
    }
}
