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

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PatchMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.things.model.ThingId;

public class ConditionalHeadersOnPolicyIdIT extends AbstractConditionalHeadersOnThingSubResourceITBase {

    public ConditionalHeadersOnPolicyIdIT() {
        super(JsonSchemaVersion.V_2);
    }

    @Override
    protected boolean alwaysExists() {
        return true;
    }

    @Override
    protected PutMatcher createSubResourceMatcher(final CharSequence thingId) {
        throw policyIdCanOnlyBeOverwritten();
    }

    @Override
    protected GetMatcher getSubResourceMatcher(final CharSequence thingId) {
        return getPolicyId(thingId);
    }

    @Override
    protected PutMatcher overwriteSubResourceMatcher(final CharSequence thingId) {
        final Policy policyCopy = createCopiedPolicy(thingId);
        final CharSequence policyCopyId = policyCopy.getEntityId().orElseThrow().toString();

        putPolicy(policyCopy).fire();

        return putPolicyId(thingId, policyCopyId);
    }

    @Override
    protected PatchMatcher patchSubResourceMatcher(final CharSequence thingId) {
        final Policy policyCopy = createCopiedPolicy(thingId);
        final CharSequence policyCopyId = policyCopy.getEntityId().orElseThrow().toString();

        putPolicy(policyCopy).fire();

        return patchThing(version.toInt(), ThingId.of(thingId), POLICY_ID_JSON_POINTER,
                JsonValue.of(policyCopyId));
    }

    @Override
    protected DeleteMatcher deleteSubResourceMatcher(final CharSequence thingId) {
        throw policyIdCanOnlyBeOverwritten();
    }

    private UnsupportedOperationException policyIdCanOnlyBeOverwritten() {
        return new UnsupportedOperationException("PolicyID can only be overwritten!");
    }

    private Policy createCopiedPolicy(final CharSequence thingId) {
        // Create a copy of the associated default-policy and assign this one to the respective thing
        final String policyResponseBody =
                getPolicy(thingId).expectingHttpStatus(HttpStatus.OK).fire().body().asString();
        final Policy responsePolicy = PoliciesModelFactory.newPolicy(policyResponseBody);

        final PolicyId policyCopyId = PolicyId.of(idGenerator().withPrefixedRandomName(getClass().getName()));
        final Policy policyCopy = PoliciesModelFactory.newPolicyBuilder(responsePolicy).setId(policyCopyId).build();

        return policyCopy;
    }
}
