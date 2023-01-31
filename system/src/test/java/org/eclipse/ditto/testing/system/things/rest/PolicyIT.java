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
package org.eclipse.ditto.testing.system.things.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.base.model.common.HttpStatus.BAD_REQUEST;
import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
import static org.eclipse.ditto.base.model.common.HttpStatus.FORBIDDEN;
import static org.eclipse.ditto.base.model.common.HttpStatus.NOT_FOUND;
import static org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT;
import static org.eclipse.ditto.base.model.common.HttpStatus.OK;
import static org.eclipse.ditto.base.model.common.HttpStatus.UNAUTHORIZED;
import static org.eclipse.ditto.policies.api.Permission.EXECUTE;
import static org.eclipse.ditto.testing.common.TestConstants.Policy.ARBITRARY_SUBJECT_TYPE;
import static org.eclipse.ditto.testing.common.TestConstants.Policy.DITTO_AUTH_SUBJECT_TYPE;
import static org.eclipse.ditto.things.api.Permission.READ;
import static org.eclipse.ditto.things.api.Permission.WRITE;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

import org.eclipse.ditto.base.model.common.DittoDuration;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.api.Permission;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.Permissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyBuilder;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.Resources;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectAnnouncement;
import org.eclipse.ditto.policies.model.SubjectId;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ResourcePathBuilder;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.ThingsSubjectIssuer;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.matcher.PostMatcher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.restassured.response.Response;
import io.restassured.response.ResponseBody;

/**
 * Integration Tests for /policy resources and implicit Policy management via /things (e.g. creation).
 */
public final class PolicyIT extends IntegrationTest {

    @Test
    @Category(Acceptance.class)
    public void createPolicyWithMinimumRequiredPermissions() {
        final PolicyId policyId =
                PolicyId.of(idGenerator().withPrefixedRandomName("createPolicyWithMinimumRequiredPermissions"));
        final Policy policyToPut = buildMinimalPolicy(policyId);

        final Response response = putPolicy(policyId, policyToPut)
                .expectingHttpStatus(CREATED)
                .fire();

        final String policyIdFromLocation = parseIdFromLocation(response.header("Location"));

        getPolicy(policyIdFromLocation)
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(policyToPut.toJson()))
                .fire();

        deletePolicy(policyIdFromLocation)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void putCompletePolicyWithInsufficientPermissions() {
        final PolicyId policyId = PolicyId.of(idGenerator().withName("putCompletePolicyWithInsufficientPermissions"));
        final Policy policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("READ")
                .setSubject(ThingsSubjectIssuer.DITTO, "sid_read", ARBITRARY_SUBJECT_TYPE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                .build();

        putPolicy(policyId, policy)
                .expectingHttpStatus(FORBIDDEN)
                .fire();
    }

    @Test
    public void putGetAndDeleteSinglePolicyEntry() {
        PolicyId policyId = PolicyId.of(idGenerator().withName("putGetAndDeleteSinglePolicyEntry"));
        final Policy policyToPut = buildMinimalPolicy(policyId);

        final Response response = putPolicy(policyId, policyToPut)
                .expectingHttpStatus(CREATED)
                .fire();

        final String policyIdFromLocation = parseIdFromLocation(response.header("Location"));

        final String subjectId = ThingsSubjectIssuer.DITTO + ":sid_read";

        final PolicyEntry policyEntry =
                createPolicyEntry("READ", subjectId, Permission.MIN_REQUIRED_POLICY_PERMISSIONS);

        putPolicyEntry(policyIdFromLocation, policyEntry)
                .expectingHttpStatus(CREATED)
                .fire();

        getPolicyEntry(policyIdFromLocation, policyEntry.getLabel())
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(policyEntry.toJson()))
                .fire();

        final PolicyEntry anotherPolicyEntry = createPolicyEntry("FOO", subjectId,
                Permission.MIN_REQUIRED_POLICY_PERMISSIONS);

        putPolicyEntry(policyIdFromLocation, anotherPolicyEntry)
                .expectingHttpStatus(CREATED)
                .fire();

        deletePolicyEntry(policyIdFromLocation, anotherPolicyEntry.getLabel())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getPolicyEntry(policyIdFromLocation, anotherPolicyEntry.getLabel())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        deletePolicy(policyIdFromLocation)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void addMultipleSinglePolicyEntries() {
        final PolicyId policyId = PolicyId.of(idGenerator().withName("addMultipleSinglePolicyEntries"));
        final Policy policyToPut = buildMinimalPolicy(policyId);

        final Response response = putPolicy(policyId, policyToPut)
                .expectingHttpStatus(CREATED)
                .fire();

        final String policyIdFromLocation = parseIdFromLocation(response.header("Location"));

        final String subjectId1 = ThingsSubjectIssuer.DITTO + ":sid_1";
        final PolicyEntry policyEntry1 = createPolicyEntry("ONE", subjectId1, Permissions.newInstance(READ));

        putPolicyEntry(policyIdFromLocation, policyEntry1)
                .expectingHttpStatus(CREATED)
                .fire();

        final String subjectId2 = ThingsSubjectIssuer.DITTO + ":sid_2";
        final PolicyEntry policyEntry2 =
                createPolicyEntry("TWO", subjectId2, Permissions.newInstance(WRITE));

        putPolicyEntry(policyIdFromLocation, policyEntry2)
                .expectingHttpStatus(CREATED)
                .fire();

        final Collection<PolicyEntry> addedPolicyEntries = new HashSet<>();
        addedPolicyEntries.add(policyEntry1);
        addedPolicyEntries.add(policyEntry2);
        final Policy expectedPolicy = PoliciesModelFactory.newPolicyBuilder(policyToPut)
                .setAll(addedPolicyEntries)
                .build();

        getPolicy(policyIdFromLocation)
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(expectedPolicy.toJson()))
                .fire();

        deletePolicy(policyIdFromLocation)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void changeExistingPolicyEntry() {
        final PolicyId policyId = PolicyId.of(idGenerator().withName("changeExistingPolicyEntry"));
        final Policy policyToPut = buildMinimalPolicy(policyId);

        final Response response = putPolicy(policyId, policyToPut)
                .expectingHttpStatus(CREATED)
                .fire();

        final String policyIdFromLocation = parseIdFromLocation(response.header("Location"));

        final String subjectId = ThingsSubjectIssuer.DITTO + ":sid_1";
        final PolicyEntry policyEntry = createPolicyEntry("ONE", subjectId, Permission.MIN_REQUIRED_POLICY_PERMISSIONS);

        putPolicyEntry(policyIdFromLocation, policyEntry)
                .expectingHttpStatus(CREATED)
                .fire();

        getPolicyEntry(policyIdFromLocation, policyEntry.getLabel())
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(policyEntry.toJson()))
                .fire();

        final PolicyEntry changedPolicyEntry =
                createPolicyEntry("ONE", subjectId, Permissions.newInstance(READ));

        putPolicyEntry(policyIdFromLocation, changedPolicyEntry)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getPolicyEntry(policyIdFromLocation, changedPolicyEntry.getLabel())
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(changedPolicyEntry.toJson()))
                .fire();

        deletePolicy(policyIdFromLocation)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void addCompletePolicyAndQueryItAfterwards() {
        final PolicyId policyId = PolicyId.of(idGenerator().withName("addCompletePolicyAndQueryItAfterwards"));
        final Policy policyToPut = buildMinimalPolicy(policyId);

        final Response response = putPolicy(policyId, policyToPut)
                .expectingHttpStatus(CREATED)
                .fire();

        final String policyIdFromLocation = parseIdFromLocation(response.header("Location"));

        final Response getPolicyResponse = getPolicy(policyIdFromLocation)
                .expectingHttpStatus(OK)
                .fire();

        final ResponseBody responseBody = getPolicyResponse.getBody();
        final JsonObject jsonObject = JsonFactory.newObject(responseBody.asString());
        final Policy oldPolicy = PoliciesModelFactory.newPolicy(jsonObject);

        final Policy newPolicy = PoliciesModelFactory.newPolicyBuilder(oldPolicy)
                .forLabel("MINIMAL") // overwrite existing PolicyEntry
                .setRevokedPermissions(PoliciesResourceType.thingResource("/attributes"),
                        Permissions.newInstance(WRITE))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/attributes/foo"),
                        Permissions.newInstance(WRITE))
                .build();

        putPolicy(policyIdFromLocation, newPolicy)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getPolicy(policyIdFromLocation)
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(newPolicy.toJson()))
                .fire();

        deletePolicy(policyIdFromLocation)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void addCompletePolicyAndModifyEntriesAfterwards() {
        final PolicyId policyId = PolicyId.of(idGenerator().withName("addCompletePolicyAndModifyEntriesAfterwards"));
        final Policy policyToPut = buildMinimalPolicy(policyId);

        final Response response = putPolicy(policyId, policyToPut)
                .expectingHttpStatus(CREATED)
                .fire();

        final String policyIdFromLocation = parseIdFromLocation(response.header("Location"));

        final Response getPolicyResponse = getPolicy(policyIdFromLocation)
                .expectingHttpStatus(OK)
                .fire();

        final ResponseBody responseBody = getPolicyResponse.getBody();
        final JsonObject jsonObject = JsonFactory.newObject(responseBody.asString());
        final Policy oldPolicy = PoliciesModelFactory.newPolicy(jsonObject);

        final Policy newPolicy = PoliciesModelFactory.newPolicyBuilder(oldPolicy)
                .forLabel("ANOTHER")
                .setRevokedPermissions(PoliciesResourceType.thingResource("/attributes"),
                        Permissions.newInstance(WRITE))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/attributes/foo"),
                        Permissions.newInstance(WRITE))
                .build();

        putPolicy(policyIdFromLocation, newPolicy)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getPolicy(policyIdFromLocation)
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(newPolicy.toJson()))
                .fire();

        final Policy changedPolicy =
                PoliciesModelFactory.newPolicyBuilder(PolicyId.of(policyIdFromLocation))
                        .forLabel("READ")
                        .setSubject(ThingsSubjectIssuer.DITTO, "sid_read",
                                ARBITRARY_SUBJECT_TYPE)
                        .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                        .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ)
                        .forLabel("WRITE")
                        .setSubject(ThingsSubjectIssuer.DITTO, "sid_write",
                                ARBITRARY_SUBJECT_TYPE)
                        .setGrantedPermissions(PoliciesResourceType.thingResource("/"), WRITE)
                        .setGrantedPermissions(PoliciesResourceType.policyResource("/"), WRITE)
                        .forLabel("ALL")
                        .setSubject(ThingsSubjectIssuer.DITTO, "sid_all",
                                ARBITRARY_SUBJECT_TYPE)
                        .setGrantedPermissions(PoliciesResourceType.thingResource("/"), WRITE, READ)
                        .setGrantedPermissions(PoliciesResourceType.policyResource("/"), WRITE, READ)
                        .build();

        for (final PolicyEntry policyEntry : changedPolicy) {
            putPolicyEntry(policyIdFromLocation, policyEntry)
                    .expectingHttpStatus(CREATED)
                    .fire();
        }
        for (final PolicyEntry policyEntry : changedPolicy) {
            getPolicyEntry(policyIdFromLocation, policyEntry.getLabel())
                    .expectingHttpStatus(OK)
                    .expectingBody(containsOnly(policyEntry.toJson()))
                    .fire();
        }

        final Policy expectedPolicy = newPolicy.toBuilder().setAll(changedPolicy).build();
        getPolicy(policyIdFromLocation)
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(expectedPolicy.toJson()))
                .fire();

        deletePolicy(policyIdFromLocation)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void addAndDeleteEntries() {
        final PolicyId policyId = PolicyId.of(idGenerator().withName("addAndDeleteEntries"));
        final Policy policyToPut = buildMinimalPolicy(policyId);

        final Response response = putPolicy(policyId, policyToPut)
                .expectingHttpStatus(CREATED)
                .fire();

        final String policyIdFromLocation = parseIdFromLocation(response.header("Location"));

        final Response getPolicyResponse = getPolicy(policyIdFromLocation)
                .expectingHttpStatus(OK)
                .fire();

        final ResponseBody responseBody = getPolicyResponse.getBody();
        final JsonObject jsonObject = JsonFactory.newObject(responseBody.asString());
        final Policy oldPolicy = PoliciesModelFactory.newPolicy(jsonObject);

        final String anotherLabel = "ANOTHER";
        final Policy newPolicy = PoliciesModelFactory.newPolicyBuilder(oldPolicy)
                .forLabel(anotherLabel)
                .setRevokedPermissions(PoliciesResourceType.thingResource("/attributes"),
                        Permissions.newInstance(WRITE))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/attributes/foo"),
                        Permissions.newInstance(WRITE))
                .build();

        putPolicy(policyIdFromLocation, newPolicy)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getPolicy(policyIdFromLocation)
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(newPolicy.toJson()))
                .fire();

        deletePolicyEntry(policyIdFromLocation, anotherLabel)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getPolicy(policyIdFromLocation)
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(oldPolicy.toJson()))
                .fire();

        // Deleting the last with all permissions must be forbidden:
        final Set<PolicyEntry> oldPolicyEntriesSet = oldPolicy.getEntriesSet();
        final Optional<PolicyEntry> originalPolicyEntryOptional = oldPolicyEntriesSet.stream().findFirst();
        if (originalPolicyEntryOptional.isPresent()) {
            final PolicyEntry originalPolicyEntry = originalPolicyEntryOptional.get();
            deletePolicyEntry(policyIdFromLocation, originalPolicyEntry.getLabel())
                    .expectingHttpStatus(FORBIDDEN)
                    .fire();
        }

        deletePolicy(policyIdFromLocation)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void putTooLargePolicy() {
        final PolicyId policyId = PolicyId.of(idGenerator().withName("putTooLargePolicy"));
        Policy policyToPut = buildMinimalPolicy(policyId);

        final PolicyBuilder policyBuilder = policyToPut.toBuilder();
        int i = 0;
        do {
            policyBuilder.forLabel("ENTRY-NO" + i)
                    .setSubject(createDefaultSubject())
                    .setGrantedPermissions(PoliciesResourceType.policyResource("/"), "READ", "WRITE")
                    .setGrantedPermissions(PoliciesResourceType.thingResource("/"), "READ", "WRITE");
            policyToPut = policyBuilder.build();
            i++;
        } while (policyToPut.toJsonString().length() < DEFAULT_MAX_POLICY_SIZE);

        putPolicy(policyId, policyToPut)
                .expectingHttpStatus(HttpStatus.REQUEST_ENTITY_TOO_LARGE)
                .fire();

        deletePolicy(policyId)
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    @Test
    public void putPolicyWithSubjectIdPlaceholder() {
        final PolicyId policyId = PolicyId.of(idGenerator().withName("putPolicyWithSubjectIdPlaceholder"));
        final Subject subject =
                Subject.newInstance(TestConstants.REQUEST_SUBJECT_ID_PLACEHOLDER, DITTO_AUTH_SUBJECT_TYPE);
        final Policy policyWithPlaceholder = buildMinimalPolicy(policyId, subject);

        putPolicy(policyId, policyWithPlaceholder)
                .expectingHttpStatus(CREATED)
                .fire();

        final Policy expectedPolicy = buildMinimalPolicy(policyId, createDefaultSubject());
        getPolicy(policyId)
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(expectedPolicy.toJson()))
                .fire();
    }

    @Test
    public void putPolicyWithLegacySubjectIdPlaceholder() {
        final PolicyId policyId = PolicyId.of(idGenerator().withName("putPolicyWithLegacySubjectIdPlaceholder"));
        final Subject subject =
                Subject.newInstance("${request.subjectId}", DITTO_AUTH_SUBJECT_TYPE);
        final Policy policyWithPlaceholder = buildMinimalPolicy(policyId, subject);

        putPolicy(policyId, policyWithPlaceholder)
                .expectingHttpStatus(CREATED)
                .fire();

        final Policy expectedPolicy = buildMinimalPolicy(policyId, createDefaultSubject());
        getPolicy(policyId)
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(expectedPolicy.toJson()))
                .fire();
    }

    @Test
    public void putPolicyWithUnknownPlaceholder() {
        final PolicyId policyId = PolicyId.of(idGenerator().withName("putPolicyWithUnknownPlaceholder"));
        final Subject subject =
                Subject.newInstance("{{ unknown }}", ARBITRARY_SUBJECT_TYPE);
        final Policy policyWithPlaceholder = buildMinimalPolicy(policyId, subject);

        putPolicy(policyId, policyWithPlaceholder)
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    @Test
    public void putPolicyWithUnknownLegacyPlaceholder() {
        final PolicyId policyId = PolicyId.of(idGenerator().withName("putPolicyWithUnknownLegacyPlaceholder"));
        final Subject subject =
                Subject.newInstance("${unknown}", ARBITRARY_SUBJECT_TYPE);
        final Policy policyWithPlaceholder = buildMinimalPolicy(policyId, subject);

        putPolicy(policyId, policyWithPlaceholder)
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    @Test
    public void activateAndDeactivatePolicyTokenIntegration() {
        // GIVEN: policy grants EXECUTE on the entry "activate"
        final String policyId = idGenerator().withName("activateAndDeactivatePolicyTokenIntegration");
        final Policy policy = buildPolicyWithExecute(policyId,
                serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject());
        putPolicy(policyId, policy).expectingHttpStatus(CREATED).fire();

        // WHEN: the action "activateTokenIntegration" is executed at the policy level
        final SubjectAnnouncement subjectAnnouncement = SubjectAnnouncement.of(DittoDuration.parseDuration("7s"), false);
        final JsonObject activateTokenIntegrationPayload = JsonObject.newBuilder()
                .set("announcement", subjectAnnouncement.toJson())
                .build();
        postPolicy(policyId, "actions/activateTokenIntegration", activateTokenIntegrationPayload.toString())
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(NO_CONTENT)
                .expectingBody(Matchers.isEmptyString())
                .fire();

        // THEN: the entry with the granted EXECUTE permission has an expiring subject injected
        final Response retrieveResponse = getPolicy(policyId).expectingHttpStatus(OK).fire();
        final Policy retrievedPolicy = PoliciesModelFactory.newPolicy(retrieveResponse.body().asString());
        final Subjects activatedSubjects = retrievedPolicy.getEntryFor("activate").orElseThrow().getSubjects();

        final SubjectId expectedSubjectId = SubjectId.newInstance(String.format("integration:%s:%s",
                serviceEnv.getTestingContext2().getSolution().getUsername(),
                TestingContext.DEFAULT_SCOPE));
        assertThat(activatedSubjects.getSubject(expectedSubjectId)).isNotEmpty();
        final Subject activatedSubject = activatedSubjects.getSubject(expectedSubjectId).orElseThrow();
        assertThat(activatedSubject.getExpiry()).isNotEmpty();
        final Instant expiry = activatedSubject.getExpiry().orElseThrow().getTimestamp();
        final Instant now = Instant.now();
        assertThat(expiry).isBetween(now.minus(Duration.ofSeconds(10L)), now.plus(Duration.ofHours(5)));

        assertThat(activatedSubject.getAnnouncement()).contains(subjectAnnouncement);

        // WHEN: token integration is deactivated at the policy level
        postPolicy(policyId, "actions/deactivateTokenIntegration")
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(NO_CONTENT)
                .expectingBody(Matchers.isEmptyString())
                .fire();

        // THEN: the injected subject is gone
        final Response retrieveResponse2 = getPolicy(policyId).expectingHttpStatus(OK).fire();
        assertThat(PoliciesModelFactory.newPolicy(retrieveResponse2.body().asString())).isEqualTo(policy);
    }

    @Test
    public void activateAndDeactivateTokenIntegrationOnEntryLevel() {
        // GIVEN: policy grants EXECUTE on the entry "activate"
        final String policyId = idGenerator().withName("activateAndDeactivateTokenIntegrationOnEntryLevel");
        final Policy policy = buildPolicyWithExecute(policyId,
                serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject());
        putPolicy(policyId, policy).expectingHttpStatus(CREATED).fire();

        // WHEN: the action "activateTokenIntegration" is executed at the entry level
        postPolicy(policyId, "entries/activate/actions/activateTokenIntegration")
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(NO_CONTENT)
                .expectingBody(Matchers.isEmptyString())
                .fire();

        // THEN: the entry has an expiring subject injected
        final Response retrieveResponse = getPolicy(policyId).expectingHttpStatus(OK).fire();
        final Policy retrievedPolicy = PoliciesModelFactory.newPolicy(retrieveResponse.body().asString());
        final Subjects activatedSubjects = retrievedPolicy.getEntryFor("activate").orElseThrow().getSubjects();

        final SubjectId expectedSubjectId = SubjectId.newInstance(String.format("integration:%s:%s",
                serviceEnv.getTestingContext2().getSolution().getUsername(),
                TestingContext.DEFAULT_SCOPE));
        assertThat(activatedSubjects.getSubject(expectedSubjectId)).isNotEmpty();
        final Subject activatedSubject = activatedSubjects.getSubject(expectedSubjectId).orElseThrow();
        assertThat(activatedSubject.getExpiry()).isNotEmpty();
        final Instant expiry = activatedSubject.getExpiry().orElseThrow().getTimestamp();
        final Instant now = Instant.now();
        assertThat(expiry).isBetween(now.minus(Duration.ofSeconds(10L)), now.plus(Duration.ofHours(5)));

        // WHEN: token integration is deactivated at the entry level
        postPolicy(policyId, "entries/activate/actions/deactivateTokenIntegration")
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(NO_CONTENT)
                .expectingBody(Matchers.isEmptyString())
                .fire();

        // THEN: the injected subject is gone
        final Response retrieveResponse2 = getPolicy(policyId).expectingHttpStatus(OK).fire();
        assertThat(PoliciesModelFactory.newPolicy(retrieveResponse2.body().asString())).isEqualTo(policy);
    }

    @Test
    public void activateAndDeactivateTokenIntegrationWithoutPermission() {
        final String policyId = idGenerator().withName("activateAndDeactivateTokenIntegrationWithoutPermission");
        final Policy policy = buildPolicyWithExecute(policyId,
                serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject());
        putPolicy(policyId, policy).expectingHttpStatus(CREATED).fire();

        // no entry with EXECUTE granted
        postPolicy(policyId, "actions/activateTokenIntegration")
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // no entry with EXECUTE granted
        postPolicy(policyId, "actions/deactivateTokenIntegration")
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // authenticated subject not contained in policy entry
        postPolicy(policyId, "entries/activate-without-subject/actions/activateTokenIntegration")
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // incorrect authentication method
        postPolicy(policyId, "entries/activate/actions/activateTokenIntegration")
                .withBasicAuth(serviceEnv.getDefaultAuthUsername(),
                        serviceEnv.getDefaultTestingContext().getSolution().getSecret())
                .expectingHttpStatus(UNAUTHORIZED)
                .fire();

        // no EXECUTE grant on target entry
        postPolicy(policyId, "entries/admin/actions/deactivateTokenIntegration")
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // entry with EXECUTE granted is deleted
        deletePolicyEntry(policyId, "activate").expectingHttpStatus(NO_CONTENT).fire();

        // 403: no permitted entry exist
        postPolicy(policyId, "actions/activateTokenIntegration")
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // 404: target entry has EXECUTE granted, but was deleted
        postPolicy(policyId, "entries/activate/actions/activateTokenIntegration")
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    private static PolicyEntry createPolicyEntry(final CharSequence label, final CharSequence subjectId,
            final Iterable<String> grantedPermissions) {
        final Subject subject =
                Subject.newInstance(ThingsSubjectIssuer.DITTO, subjectId, ARBITRARY_SUBJECT_TYPE);
        return createPolicyEntry(label, subject, grantedPermissions);
    }

    private static PolicyEntry createPolicyEntry(final CharSequence label, final Subject subject,
            final Iterable<String> grantedPermissions) {
        return PolicyEntry.newInstance(label,
                Subjects.newInstance(
                        subject),
                Resources.newInstance(Resource.newInstance(TestConstants.Policy.THING_RESOURCE_TYPE, "/",
                        EffectedPermissions.newInstance(grantedPermissions, null))));
    }

    private static Policy buildMinimalPolicy(final PolicyId policyId) {
        final Subject subject = createDefaultSubject();
        return buildMinimalPolicy(policyId, subject);
    }

    private static Subject createDefaultSubject() {
        return serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject();
    }

    private static Policy buildMinimalPolicy(final PolicyId policyId, final Subject subject) {
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("MINIMAL")
                .setSubject(subject)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .build();
    }

    private static Policy buildPolicyWithExecute(final CharSequence policyId, final Subject adminSubject) {
        return Policy.newBuilder(PolicyId.of(policyId))
                .forLabel("admin")
                .setSubject(adminSubject)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .forLabel("execute")
                .setSubject(serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(PoliciesResourceType.policyResource("/entries/activate"), EXECUTE)
                .forLabel("activate")
                .setSubject(serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                .forLabel("activate-without-subject")
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                .build();
    }

    private static PostMatcher postPolicy(final CharSequence policyId, final String pathAfterPolicyId) {
        return postPolicy(policyId, pathAfterPolicyId, null);
    }

    private static PostMatcher postPolicy(final CharSequence policyId, final String pathAfterPolicyId,
            @Nullable final String jsonPayload) {
        final String path = ResourcePathBuilder.forPolicy(policyId) + "/" + pathAfterPolicyId;
        final String thingsServiceUrl = dittoUrl(TestConstants.API_V_2, path);
        LOGGER.debug("POSTing URL '{}'", thingsServiceUrl);
        return post(thingsServiceUrl, jsonPayload).withLogging(LOGGER, "Policy");
    }

}
