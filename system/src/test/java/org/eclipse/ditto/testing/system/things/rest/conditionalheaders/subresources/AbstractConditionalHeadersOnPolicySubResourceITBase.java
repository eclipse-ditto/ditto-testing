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
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.testing.system.things.rest.conditionalheaders.ConditionalHeadersTestHelper.ASTERISK;
import static org.eclipse.ditto.testing.system.things.rest.conditionalheaders.ConditionalHeadersTestHelper.E_TAG_HEADER_KEY;
import static org.eclipse.ditto.testing.system.things.rest.conditionalheaders.ConditionalHeadersTestHelper.IF_MATCH_HEADER_KEY;
import static org.eclipse.ditto.testing.system.things.rest.conditionalheaders.ConditionalHeadersTestHelper.IF_NONE_MATCH_HEADER_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;

import java.util.Arrays;
import java.util.List;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyBuilder;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.ResourceKey;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.HttpVerbMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.testing.system.things.rest.conditionalheaders.ConditionalHeadersPolicyOnRootResourceIT;
import org.junit.Test;

public abstract class AbstractConditionalHeadersOnPolicySubResourceITBase extends IntegrationTest {

    private static final String NON_MATCH_E_TAG = "\"never-match-etag-value-32bbbb57-73ae-4151-82e2-a6af828005d8\"";

    protected abstract boolean alwaysExists();

    protected abstract PutMatcher createSubResourceMatcher(final CharSequence policyId);

    protected abstract GetMatcher getSubResourceMatcher(final CharSequence policyId);

    protected abstract PutMatcher overwriteSubResourceMatcher(final CharSequence policyId);

    protected abstract DeleteMatcher deleteSubResourceMatcher(final CharSequence policyId);

    @Test
    public void successfulRequestReturnsExpectedETag() {
        final PolicyId policyId = generateRandomPolicyId();
        final String eTag1 = createEntityWithSubResource(policyId);

        assertGetProvidesExpectedETag(policyId, eTag1);

        final String eTag2 = overwriteSubResourceMatcher(policyId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag2).isNotNull().isNotEqualTo(eTag1);

        assertGetProvidesExpectedETag(policyId, eTag2);
    }

    @Test
    public void getWithIfMatchReturnsSubEntityIfETagMatches() {
        final PolicyId policyId = generateRandomPolicyId();
        final String eTag1 = createEntityWithSubResource(policyId);

        getSubResourceMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, eTag1)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(not(isEmptyOrNullString()))
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .fire();
    }

    @Test
    public void getWithIfMatchReturnsPreconditionFailedIfETagDoesNotMatch() {
        final PolicyId policyId = generateRandomPolicyId();
        final String eTag1 = createEntityWithSubResource(policyId);

        getSubResourceMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, NON_MATCH_E_TAG)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingBody(containsString(IF_MATCH_HEADER_KEY))
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .fire();
    }

    @Test
    public void getWithIfMatchAsteriskReturns404IfSubEntityDoesNotYetExist() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName(getClass().getName()));
        createEntity(policyId);

        getSubResourceMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void getWithIfMatchAsteriskReturnsSubEntityIfEntityAlreadyExists() {
        final PolicyId policyId = generateRandomPolicyId();
        final String eTag1 = createEntityWithSubResource(policyId);

        getSubResourceMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(not(isEmptyOrNullString()))
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchReturnsSubEntityIfETagDoesNotMatch() {
        final PolicyId policyId = generateRandomPolicyId();
        final String eTag1 = createEntityWithSubResource(policyId);

        getSubResourceMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, NON_MATCH_E_TAG)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(not(isEmptyOrNullString()))
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchReturnsNotModifiedIfETagMatches() {
        final PolicyId policyId = generateRandomPolicyId();
        final String eTag1 = createEntityWithSubResource(policyId);

        getSubResourceMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, eTag1)
                .expectingHttpStatus(HttpStatus.NOT_MODIFIED)
                .expectingBody(isEmptyString())
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchAsteriskReturns404IfEntityDoesNotYetExist() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final PolicyId policyId = PolicyId.of(idGenerator().withPrefixedRandomName(getClass().getName()));
        createEntity(policyId);

        getSubResourceMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void getWithIfNoneMatchAsteriskReturnsNotModifiedIfEntityAlreadyExists() {
        final PolicyId policyId = generateRandomPolicyId();
        final String eTag1 = createEntityWithSubResource(policyId);

        getSubResourceMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.NOT_MODIFIED)
                .expectingBody(isEmptyString())
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .fire();
    }

    @Test
    public void putWithIfMatchSucceedsIfETagMatches() {
        final PolicyId policyId = generateRandomPolicyId();

        final String eTag1 = createEntityWithSubResource(policyId);

        final String eTag2 = overwriteSubResourceMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, eTag1)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag2).isNotNull().isNotEqualTo(eTag1);

        getSubResourceMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, eTag2).fire();
    }

    @Test
    public void putWithIfMatchFailsIfETagDoesNotMatch() {
        final PolicyId policyId = generateRandomPolicyId();

        final String eTag1 = createEntityWithSubResource(policyId);

        overwriteSubResourceMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, NON_MATCH_E_TAG)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .expectingBody(containsString(IF_MATCH_HEADER_KEY))
                .fire();

        getSubResourceMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, eTag1).fire();
    }

    @Test
    public void putWithIfMatchAsteriskFailsIfSubEntityDoesNotYetExist() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final PolicyId policyId = generateRandomPolicyId();
        createEntity(policyId);

        overwriteSubResourceMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, (String) null)
                .expectingBody(containsString(IF_MATCH_HEADER_KEY))
                .fire();

        getSubResourceMatcher(policyId).expectingHttpStatus(HttpStatus.NOT_FOUND).fire();
    }

    @Test
    public void putWithIfMatchAsteriskSucceedsIfSubEntityAlreadyExists() {
        final PolicyId policyId = generateRandomPolicyId();

        final String eTag1 = createEntityWithSubResource(policyId);

        final String eTag2 = overwriteSubResourceMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag2).isNotNull().isNotEqualTo(eTag1);

        getSubResourceMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, eTag2).fire();
    }

    @Test
    public void putWithIfNoneMatchSucceedsIfETagDoesNotMatch() {
        final PolicyId policyId = generateRandomPolicyId();

        final String eTag1 = createEntityWithSubResource(policyId);

        final String eTag2 = overwriteSubResourceMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, NON_MATCH_E_TAG)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag2).isNotNull().isNotEqualTo(eTag1);

        getSubResourceMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, eTag2).fire();
    }

    @Test
    public void putWithIfNoneMatchFailsIfETagMatches() {
        final PolicyId policyId = generateRandomPolicyId();

        final String eTag1 = createEntityWithSubResource(policyId);

        overwriteSubResourceMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, eTag1)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .expectingBody(containsString(IF_NONE_MATCH_HEADER_KEY))
                .fire();

        getSubResourceMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, eTag1).fire();
    }

    @Test
    public void putWithIfNoneMatchAsteriskSucceedsIfSubEntityDoesNotYetExist() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final PolicyId policyId = generateRandomPolicyId();

        createEntity(policyId);

        final String eTag1 = createSubResourceMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag1).isNotNull();

        getSubResourceMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, eTag1).fire();
    }

    @Test
    public void putWithIfNoneMatchAsteriskFailsIfSubEntityAlreadyExists() {
        final PolicyId policyId = generateRandomPolicyId();

        final String eTag1 = createEntityWithSubResource(policyId);

        overwriteSubResourceMatcher(policyId)
                .withHeader(IF_NONE_MATCH_HEADER_KEY, ASTERISK)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .expectingBody(containsString(IF_NONE_MATCH_HEADER_KEY))
                .fire();

        getSubResourceMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, eTag1).fire();
    }

    @Test
    public void deleteWithIfMatchSucceedsIfETagMatches() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final PolicyId policyId = generateRandomPolicyId();

        final String eTag1 = createEntityWithSubResource(policyId);

        deleteSubResourceMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, eTag1)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .expectingHeader(E_TAG_HEADER_KEY, (String) null)
                .fire();

        getSubResourceMatcher(policyId).expectingHttpStatus(HttpStatus.NOT_FOUND).fire();
    }

    @Test
    public void deleteWithIfMatchFailsIfETagDoesNotMatch() {
        if (alwaysExists()) {
            // test is not possible for this case
            return;
        }

        final PolicyId policyId = generateRandomPolicyId();

        final String eTag1 = createEntityWithSubResource(policyId);

        deleteSubResourceMatcher(policyId)
                .withHeader(IF_MATCH_HEADER_KEY, NON_MATCH_E_TAG)
                .expectingHttpStatus(HttpStatus.PRECONDITION_FAILED)
                .expectingHeader(E_TAG_HEADER_KEY, eTag1)
                .expectingBody(containsString(IF_MATCH_HEADER_KEY))
                .fire();

        getSubResourceMatcher(policyId).expectingHeader(E_TAG_HEADER_KEY, eTag1).fire();
    }

    private PolicyId generateRandomPolicyId() {
        return PolicyId.of(idGenerator().withPrefixedRandomName(getClass().getName()));
    }

    private String createOrGetSubResource(final CharSequence policyId) {
        final HttpVerbMatcher eTagMatcher = alwaysExists() ?
                getSubResourceMatcher(policyId).expectingHttpStatus(HttpStatus.OK) :
                createSubResourceMatcher(policyId).expectingHttpStatus(HttpStatus.CREATED);

        final String eTag = eTagMatcher
                .fire()
                .header(E_TAG_HEADER_KEY);
        assertThat(eTag).isNotNull();
        return eTag;
    }

    private void assertGetProvidesExpectedETag(final CharSequence policyId, final String expectedETagValue) {
        getSubResourceMatcher(policyId)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingHeader(E_TAG_HEADER_KEY, expectedETagValue)
                .fire();
    }

    private String createEntityWithSubResource(final PolicyId policyId) {
        createEntity(policyId);

        return createOrGetSubResource(policyId);
    }

    private void createEntity(final PolicyId policyId) {
        putPolicyMatcher(policyId)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
    }

    private static PutMatcher putPolicyMatcher(final PolicyId policyId) {
        final Policy policy = policyBuilder(policyId, ConditionalHeadersPolicyOnRootResourceIT.OWNER_LABEL, ConditionalHeadersPolicyOnRootResourceIT.RESOURCE_PATH).build();

        return putPolicy(policy);
    }

    private static PolicyBuilder policyBuilder(final PolicyId policyId, final String label, final String resourcePath) {
        return PoliciesModelFactory
                .newPolicyBuilder(policyId)
                .set(policyEntry(label, resourcePath));
    }

    private static PolicyEntry policyEntry(final String label, final String resourcePath) {
        return PolicyEntry.newInstance(label,
                singletonList(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject()),
                singletonList(resource(resourcePath)));
    }

    private static Resource resource(final String resourcePath) {
        final List<String> grantedPermissions = Arrays.asList("READ", "WRITE", "ADMINISTRATE");
        return Resource.newInstance(ResourceKey.newInstance(resourcePath),
                EffectedPermissions.newInstance(grantedPermissions, null));
    }
}
