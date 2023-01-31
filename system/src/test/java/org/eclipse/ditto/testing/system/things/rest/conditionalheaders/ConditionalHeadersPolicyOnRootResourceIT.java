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
package org.eclipse.ditto.testing.system.things.rest.conditionalheaders;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;

import java.util.Arrays;
import java.util.List;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyBuilder;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.ResourceKey;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.junit.Test;

/**
 * Tests ETag and Conditional Headers on the Things API.
 */
public class ConditionalHeadersPolicyOnRootResourceIT extends IntegrationTest {

    public static final String OWNER_LABEL = "owner";
    public static final String RESOURCE_PATH = "policy:/";
    private static final String E_TAG_HEADER_KEY = DittoHeaderDefinition.ETAG.getKey();
    private static final String IF_NONE_MATCH_HEADER_KEY = DittoHeaderDefinition.IF_NONE_MATCH.getKey();
    private static final String IF_MATCH_HEADER_KEY = DittoHeaderDefinition.IF_MATCH.getKey();
    private static final String ASTERISK = "*";
    private static final String E_TAG_1 = "\"rev:1\"";
    private static final String E_TAG_2 = "\"rev:2\"";


    @Test
    public void successfulRequestReturnsExpectedETag() {
        final PolicyId policyId = createPolicy();

        assertGetProvidesExpectedETag(policyId, E_TAG_1);

        putPolicyMatcher(policyId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_2)
                .fire();

        assertGetProvidesExpectedETag(policyId, E_TAG_2);
    }

    @Test
    public void getWithIfMatchReturnsEntityIfETagMatches() {
        final PolicyId policyId = createPolicy();

        getPolicyMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, E_TAG_1)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(not(isEmptyOrNullString()))
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_1)
                .fire();
    }

    @Test
    public void getWithIfMatchReturnsPreconditionFailedIfETagDoesNotMatch() {
        final PolicyId policyId = createPolicy();

        getPolicyMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, E_TAG_2)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingBody(containsString(IF_MATCH_HEADER_KEY))
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_1)
                .fire();
    }

    @Test
    public void getWithIfMatchAsteriskReturns404IfEntityDoesNotYetExist() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName(getClass().getName()));

        getPolicyMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void getWithIfMatchAsteriskReturnsEntityIfEntityAlreadyExists() {
        final PolicyId policyId = createPolicy();

        getPolicyMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(not(isEmptyOrNullString()))
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_1)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchReturnsEntityIfETagDoesNotMatch() {
        final PolicyId policyId = createPolicy();

        getPolicyMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, E_TAG_2)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(not(isEmptyOrNullString()))
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_1)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchReturnsNotModifiedIfETagMatches() {
        final PolicyId policyId = createPolicy();

        getPolicyMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, E_TAG_1)
                .expectingHttpStatus(HttpStatus.NOT_MODIFIED)
                .expectingBody(isEmptyString())
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_1)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchAsteriskReturns404IfEntityDoesNotYetExist() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName(getClass().getName()));

        getPolicyMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchAsteriskReturnsNotModifiedIfEntityAlreadyExists() {
        final PolicyId policyId = createPolicy();

        getPolicyMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.NOT_MODIFIED)
                .expectingBody(isEmptyString())
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_1)
                .fire();
    }

    @Test
    public void putWithIfMatchSucceedsIfETagMatches() {
        final PolicyId policyId = createPolicy();

        putPolicyMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, E_TAG_1)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_2)
                .fire();

        getPolicyMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, E_TAG_2).fire();
    }

    @Test
    public void putWithIfMatchFailsIfETagDoesNotMatch() {
        final PolicyId policyId = createPolicy();

        putPolicyMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, E_TAG_2)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_1)
                .expectingBody(containsString(IF_MATCH_HEADER_KEY))
                .fire();

        getPolicyMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, E_TAG_1).fire();
    }

    @Test
    public void putWithIfMatchAsteriskFailsIfEntityDoesNotYetExist() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName(getClass().getName()));

        putPolicyMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, (String) null)
                .expectingBody(containsString(IF_MATCH_HEADER_KEY))
                .fire();

        getPolicyMatcher(policyId).expectingHttpStatus(HttpStatus.NOT_FOUND).fire();
    }

    @Test
    public void putWithIfMatchAsteriskSucceedsIfEntityAlreadyExists() {
        final PolicyId policyId = createPolicy();

        putPolicyMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_2)
                .fire();

        getPolicyMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, E_TAG_2).fire();
    }

    @Test
    public void putWithIfNoneMatchSucceedsIfETagDoesNotMatch() {
        final PolicyId policyId = createPolicy();

        putPolicyMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, E_TAG_2)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_2)
                .fire();

        getPolicyMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, E_TAG_2).fire();
    }

    @Test
    public void putWithIfNoneMatchFailsIfETagMatches() {
        final PolicyId policyId = createPolicy();

        putPolicyMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, E_TAG_1)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_1)
                .expectingBody(containsString(IF_NONE_MATCH_HEADER_KEY))
                .fire();

        getPolicyMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, E_TAG_1).fire();
    }

    @Test
    public void putWithIfNoneMatchAsteriskSucceedsIfEntityDoesNotYetExist() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName(getClass().getName()));

        putPolicyMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.CREATED)
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_1)
                .fire();

        getPolicyMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, E_TAG_1).fire();
    }

    @Test
    public void putWithIfNoneMatchAsteriskFailsIfEntityAlreadyExists() {
        final PolicyId policyId = createPolicy();

        putPolicyMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_1)
                .expectingBody(containsString(IF_NONE_MATCH_HEADER_KEY))
                .fire();

        getPolicyMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, E_TAG_1).fire();
    }

    @Test
    public void deleteWithIfMatchSucceedsIfETagMatches() {
        final PolicyId policyId = createPolicy();

        deletePolicyMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, E_TAG_1)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .expectingHeader(E_TAG_HEADER_KEY, (String) null)
                .fire();

        getPolicyMatcher(policyId).expectingHttpStatus(HttpStatus.NOT_FOUND).fire();
    }

    @Test
    public void deleteWithIfMatchFailsIfETagDoesNotMatch() {
        final PolicyId policyId = createPolicy();

        deletePolicyMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, E_TAG_2)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, E_TAG_1)
                .expectingBody(containsString(IF_MATCH_HEADER_KEY))
                .fire();

        getPolicyMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, E_TAG_1).fire();
    }

    @Test
    public void invalidIfMatchHeaderReturnsBadRequest() {
        final PolicyId policyId = createPolicy();

        getPolicyMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, "invalid")
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(containsString(IF_MATCH_HEADER_KEY))
                .fire();

    }

    @Test
    public void invalidIfNoneMatchHeaderReturnsBadRequest() {
        final PolicyId policyId = createPolicy();

        getPolicyMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, "invalid")
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(containsString(IF_NONE_MATCH_HEADER_KEY))
                .fire();

    }

    private void assertGetProvidesExpectedETag(final CharSequence policyId, final String expectedETagValue) {
        getPolicyMatcher(policyId)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingHeader(E_TAG_HEADER_KEY, expectedETagValue)
                .fire();
    }

    private GetMatcher getPolicyMatcher(final CharSequence policyId) {
        return getPolicy(policyId);
    }

    private PolicyId createPolicy() {
        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName(getClass().getName()));
        final Policy policy = policyBuilder(policyId).build();

        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED)
            .expectingHeader(E_TAG_HEADER_KEY, E_TAG_1)
            .fire();

        return policyId;
    }

    private static PutMatcher putPolicyMatcher(final PolicyId policyId) {
        final Policy policy = policyBuilder(policyId).build();
        return putPolicy(policyId, policy);
    }

    private static DeleteMatcher deletePolicyMatcher(final CharSequence policyId) {
        return deletePolicy(policyId);
    }

    private static PolicyBuilder policyBuilder(final PolicyId policyId) {
        return PoliciesModelFactory
                .newPolicyBuilder(policyId)
                .set(policyEntry());
    }

    private static PolicyEntry policyEntry() {
        return PolicyEntry.newInstance(OWNER_LABEL, singletonList(subject()), singletonList(resource()));
    }

    private static Subject subject() {
        return serviceEnv.getDefaultTestingContext().getOAuthClient().getSubject();
    }

    private static Resource resource() {
        final List<String> grantedPermissions = Arrays.asList("READ", "WRITE");
        return Resource.newInstance(ResourceKey.newInstance(RESOURCE_PATH),
                EffectedPermissions.newInstance(grantedPermissions, null));
    }
}
