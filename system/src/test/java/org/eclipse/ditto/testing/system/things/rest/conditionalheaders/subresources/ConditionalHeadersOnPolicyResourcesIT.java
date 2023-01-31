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

import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.ResourceKey;
import org.eclipse.ditto.policies.model.Resources;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.testing.system.things.rest.conditionalheaders.ConditionalHeadersPolicyOnRootResourceIT;

public class ConditionalHeadersOnPolicyResourcesIT extends AbstractConditionalHeadersOnPolicySubResourceITBase {

    @Override
    protected boolean alwaysExists() {
        return true;
    }

    @Override
    protected PutMatcher createSubResourceMatcher(final CharSequence policyId) {
        throw policyResourcesAlwaysExist();
    }

    @Override
    protected GetMatcher getSubResourceMatcher(final CharSequence policyId) {
        return getPolicyEntryResources(policyId, ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL);
    }

    @Override
    protected PutMatcher overwriteSubResourceMatcher(final CharSequence policyId) {
        return putPolicyEntryResources(policyId, ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL, resources());
    }

    @Override
    protected DeleteMatcher deleteSubResourceMatcher(final CharSequence policyId) {
        throw policyResourcesAlwaysExist();
    }

    private UnsupportedOperationException policyResourcesAlwaysExist() {
        return new UnsupportedOperationException("policy resources are always available");
    }

    private static Resources resources() {
        return Resources.newInstance(resource("thing:/"), resource("policy:/"));
    }

    private static Resource resource(final String resourcePath) {
        return Resource.newInstance(ResourceKey.newInstance(resourcePath),
                EffectedPermissions.newInstance(Arrays.asList("READ", "WRITE", "ADMINISTRATE"), null));
    }
}
