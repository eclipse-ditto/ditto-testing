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
import static org.eclipse.ditto.base.model.common.HttpStatus.NOT_FOUND;
import static org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT;
import static org.eclipse.ditto.base.model.common.HttpStatus.OK;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.policyResource;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.thingResource;
import static org.eclipse.ditto.things.api.Permission.READ;
import static org.eclipse.ditto.things.api.Permission.WRITE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.EffectedImports;
import org.eclipse.ditto.policies.model.EntryReference;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.PolicyImport;
import org.eclipse.ditto.policies.model.PolicyImports;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.signals.commands.PolicyCommandResponse;
import org.eclipse.ditto.policies.model.signals.commands.query.RetrievePolicy;
import org.eclipse.ditto.policies.model.signals.commands.query.RetrievePolicyResponse;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ResourcePathBuilder;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityTestWebsocketClient;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration Tests for /policies/<policyId>/imports resources
 */
public final class PolicyImportsIT extends IntegrationTest {

    private static final String POLICY_VIEW_PARAM = "policy-view";
    private static final String VIEW_RESOLVED = "resolved";
    private static final String VIEW_ORIGINAL = "original";

    private PolicyId importingPolicyId;
    private PolicyId importedPolicyId;
    private Policy importedPolicyFullAccess;
    private Policy importingPolicyFullAccess;
    private Policy importingPolicyRestrictedAccess;
    private Policy importedPolicyRestrictedAccess;
    private PolicyImport policyImport;
    private Policy importedPolicyMinimalAccess;

    @Before
    public void setUp() throws Exception {
        importingPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("importing"));
        importedPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("imported"));
        policyImport = createPolicyImports(importedPolicyId, "EXPLICIT");

        importedPolicyFullAccess = buildImportedPolicyFullAccess(importedPolicyId);
        importingPolicyFullAccess =
                buildImportingPolicyFullAccess(importingPolicyId, createDefaultSubject());

        importedPolicyRestrictedAccess = buildImportedPolicyRestrictedAccess(importedPolicyId);
        importingPolicyRestrictedAccess =
                buildImportingPolicyRestrictedAccess(importingPolicyId, createDefaultSubject());

        importedPolicyMinimalAccess = buildMinimalPolicy(importedPolicyId);
    }

    @Test
    public void putAndGetPolicyWithImportsAndFullAccess() {
        final Policy policyWithImports = importingPolicyFullAccess.toBuilder().setPolicyImport(policyImport).build();
        putAndGetPolicyWithImports(importedPolicyFullAccess, importedPolicyFullAccess.toJson(), policyWithImports,
                policyWithImports.toJson());
    }

    @Test
    public void putAndGetPolicyWithImportsAndRestrictedAccess() {
        final Policy policyWithImports =
                importingPolicyRestrictedAccess.toBuilder().setPolicyImport(policyImport).build();
        putAndGetPolicyWithImports(
                importedPolicyRestrictedAccess,
                importedPolicyRestrictedAccess.toJson(
                        JsonFieldSelector.newInstance("policyId", "/entries/IMPLICIT", "/entries/EXPLICIT",
                                "/entries/NEVER")),
                policyWithImports,
                policyWithImports.toJson(JsonFieldSelector.newInstance("policyId", "/imports", "/entries/IMPORTS"))
        );
    }

    private void putAndGetPolicyWithImports(
            final Policy importedPolicy,
            final JsonValue expectedImportedPolicyJson,
            final Policy importingPolicy,
            final JsonValue expectedImportingPolicyJson) {

        putPolicy(importedPolicyId, importedPolicy).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, importingPolicy).expectingHttpStatus(CREATED).fire();

        getPolicy(importedPolicyId)
                .expectingBody(containsOnly(expectedImportedPolicyJson))
                .expectingHttpStatus(OK)
                .fire();

        getPolicy(importingPolicyId)
                .expectingBody(containsOnly(expectedImportingPolicyJson))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putAndGetPolicyImportsFullAccess() {
        putAndGetPolicyImports(importedPolicyFullAccess, importingPolicyFullAccess);
    }

    @Test
    public void putAndGetPolicyImportsRestrictedAccess() {
        putAndGetPolicyImports(importedPolicyRestrictedAccess, importingPolicyRestrictedAccess);
    }

    private void putAndGetPolicyImports(final Policy importedPolicy, final Policy importingPolicy) {
        final PolicyImports policyImports = PoliciesModelFactory.newPolicyImports(policyImport);

        putPolicy(importedPolicyId, importedPolicy).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, importingPolicy).expectingHttpStatus(CREATED).fire();

        // put and get policy imports
        putPolicyImports(importingPolicyId, policyImports)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
        getPolicyImports(importingPolicyId)
                .expectingBody(containsOnly(policyImports.toJson()))
                .expectingHttpStatus(OK)
                .fire();

        // put empty policy imports
        putPolicyImports(importingPolicyId, PoliciesModelFactory.newPolicyImports(List.of()))
                .expectingHttpStatus(NO_CONTENT)
                .fire();
        // ... and expect empty response
        getPolicyImports(importingPolicyId)
                .expectingBody(containsOnly(JsonObject.empty()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putAndGetAndDeletePolicyImportFullAccess() {
        putAndGetAndDeletePolicyImport(importedPolicyFullAccess, importingPolicyFullAccess);
    }

    @Test
    public void putAndGetAndDeletePolicyImportRestrictedAccess() {
        putAndGetAndDeletePolicyImport(importedPolicyRestrictedAccess, importingPolicyRestrictedAccess);
    }

    private void putAndGetAndDeletePolicyImport(final Policy importedPolicy, final Policy importingPolicy) {

        putPolicy(importedPolicyId, importedPolicy).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, importingPolicy).expectingHttpStatus(CREATED).fire();

        // create individual policy import
        putPolicyImport(importingPolicyId, policyImport).expectingHttpStatus(CREATED).fire();
        // ... and expect created imports are returned
        getPolicyImport(importingPolicyId, importedPolicyId)
                .expectingBody(containsOnly(policyImport.toJson()))
                .expectingHttpStatus(OK)
                .fire();

        // modify individual policy import
        final PolicyImport modifiedPolicyImport = createPolicyImports(importedPolicyId, "NEVER");
        putPolicyImport(importingPolicyId, modifiedPolicyImport).expectingHttpStatus(NO_CONTENT).fire();
        // ... and expect modified policy import is returned
        getPolicyImport(importingPolicyId, importedPolicyId)
                .expectingBody(containsOnly(modifiedPolicyImport.toJson()))
                .expectingHttpStatus(OK)
                .fire();

        // get non existent import and expect 404 response
        getPolicyImport(importingPolicyId, PolicyId.inDefaultNamespace("missing"))
                .expectingErrorCode("policies:import.notfound")
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // delete individual policy import
        deletePolicyImport(importingPolicyId, importedPolicyId).expectingHttpStatus(NO_CONTENT).fire();

        // ... and expect individual policy imports are not found after deletion
        getPolicyImport(importingPolicyId, importedPolicyId).expectingHttpStatus(NOT_FOUND).fire();
    }

    @Test
    public void putPolicyImportsWithNoAccessOnImportedEntry() {


        putPolicy(importedPolicyId, importedPolicyMinimalAccess).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, importingPolicyRestrictedAccess).expectingHttpStatus(CREATED).fire();

        putPolicyImports(importingPolicyId, PolicyImports.newInstance(policyImport))
                .expectingErrorCode("policies:policy.notfound")
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    @Test
    public void putPolicyImportWithNoAccessOnImportedEntry() {
        putPolicy(importedPolicyId, importedPolicyMinimalAccess).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, importingPolicyRestrictedAccess).expectingHttpStatus(CREATED).fire();

        putPolicyImport(importingPolicyId, policyImport)
                .expectingErrorCode("policies:policy.notfound")
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    @Test
    public void putPolicyImportingItselfIsNotAllowed() {

        putPolicy(importedPolicyId, importedPolicyRestrictedAccess)
                .expectingHttpStatus(CREATED)
                .fire();

        final PolicyImport policyImportWithSelfReference = PolicyImport.newInstance(importingPolicyId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("IMPORTS"))));
        final PolicyImports policyImportsWithSelfReference = PolicyImports.newInstance(policyImportWithSelfReference);
        final JsonObject policyWithSelfReference = importingPolicyRestrictedAccess.toJson()
                .set(Policy.JsonFields.IMPORTS, policyImportsWithSelfReference.toJson());

        // try to create policy
        putPolicy(importingPolicyId, policyWithSelfReference)
                .expectingHttpStatus(BAD_REQUEST)
                .expectingErrorCode("policies:import.invalid")
                .fire();

        // try to modify policy
        putPolicy(importingPolicyId, importingPolicyRestrictedAccess).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, policyWithSelfReference)
                .expectingHttpStatus(BAD_REQUEST)
                .expectingErrorCode("policies:import.invalid")
                .fire();

        putPolicyImports(importingPolicyId, policyImportsWithSelfReference)
                .expectingErrorCode("policies:import.invalid")
                .expectingHttpStatus(BAD_REQUEST)
                .fire();

        putPolicyImport(importingPolicyId, policyImportWithSelfReference)
                .expectingErrorCode("policies:import.invalid")
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    @Test
    public void putPolicyWithTooManyImports() {
        final PolicyImports policyImports = PoliciesModelFactory.newPolicyImports(createPolicyImports(11, importedPolicyId));

        putPolicy(importingPolicyId, importingPolicyFullAccess)
                .expectingHttpStatus(CREATED)
                .fire();

        for (PolicyImport policyImport : policyImports) {
            var policy = buildImportedPolicyFullAccess(policyImport.getImportedPolicyId());
            putPolicy(policy.getEntityId().get(), policy)
                    .expectingHttpStatus(CREATED)
                    .fire();
        }

        putPolicyImports(importingPolicyId, policyImports)
                .expectingErrorCode("policies:imports.toolarge")
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    // ---- policy-view query parameter (resolved / effective view) ----

    @Test
    public void getPolicyWithoutPolicyViewReturnsStoredPolicy() {
        final Policy policyWithImports =
                importingPolicyRestrictedAccess.toBuilder().setPolicyImport(policyImport).build();
        putPolicy(importedPolicyId, importedPolicyRestrictedAccess).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, policyWithImports).expectingHttpStatus(CREATED).fire();

        final JsonObject body = JsonFactory.newObject(getPolicyAsJsonString(importingPolicyId, null));
        final JsonObject entries = body.getValue("entries").orElseThrow().asObject();

        final List<String> importedKeys = entries.getKeys().stream()
                .map(Object::toString)
                .filter(k -> k.startsWith("imported-"))
                .toList();
        assertThat(importedKeys).as("no imported entries should appear in unresolved view").isEmpty();
    }

    @Test
    public void policyViewOriginalEqualsNoView() {
        final Policy policyWithImports =
                importingPolicyRestrictedAccess.toBuilder().setPolicyImport(policyImport).build();
        putPolicy(importedPolicyId, importedPolicyRestrictedAccess).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, policyWithImports).expectingHttpStatus(CREATED).fire();

        final String noView = getPolicyAsJsonString(importingPolicyId, null);
        final String original = getPolicyAsJsonString(importingPolicyId, VIEW_ORIGINAL);
        assertThat(original).as("policy-view=original must equal no-view response").isEqualTo(noView);
    }

    @Test
    public void policyViewResolvedMergesImportedEntries() {
        // Use the full-access importing fixture: the per-subject READ filter (applyEnforcerJsonView) runs AFTER
        // the merge, walking the JSON view of the merged policy. Imported entries land in JSON under rewritten
        // labels (imported-<id>-<label>), so the user needs READ on policy:/ to see them. With the restricted
        // fixture (READ only on /imports and /entries/IMPORTS) the filter would correctly drop the imported
        // entries — that is intentional security and a separate test.
        final Policy policyWithImports =
                importingPolicyFullAccess.toBuilder().setPolicyImport(policyImport).build();
        putPolicy(importedPolicyId, importedPolicyRestrictedAccess).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, policyWithImports).expectingHttpStatus(CREATED).fire();

        final JsonObject body = JsonFactory.newObject(getPolicyAsJsonString(importingPolicyId, VIEW_RESOLVED));
        final JsonObject entries = body.getValue("entries").orElseThrow().asObject();

        // The consumer's own ADMIN entry is still there.
        assertThat(entries.contains(JsonFactory.newPointer("ADMIN"))).isTrue();

        // EXPLICIT (in import filter) and IMPLICIT (auto-imported) appear with rewritten labels.
        // ADMIN and NEVER are ImportableType.NEVER → never imported.
        assertThat(entries.contains(JsonFactory.newPointer("imported-" + importedPolicyId + "-EXPLICIT"))).isTrue();
        assertThat(entries.contains(JsonFactory.newPointer("imported-" + importedPolicyId + "-IMPLICIT"))).isTrue();
        assertThat(entries.contains(JsonFactory.newPointer("imported-" + importedPolicyId + "-ADMIN"))).isFalse();
        assertThat(entries.contains(JsonFactory.newPointer("imported-" + importedPolicyId + "-NEVER"))).isFalse();

        // imports config block is preserved unchanged.
        assertThat(body.contains(JsonFactory.newPointer("imports"))).isTrue();
    }

    @Test
    public void policyViewResolvedOnPolicyWithoutImportsEqualsOriginal() {
        putPolicy(importedPolicyId, importedPolicyFullAccess).expectingHttpStatus(CREATED).fire();

        final JsonObject originalEntries =
                JsonFactory.newObject(getPolicyAsJsonString(importedPolicyId, null))
                        .getValue("entries").orElseThrow().asObject();
        final JsonObject resolvedEntries =
                JsonFactory.newObject(getPolicyAsJsonString(importedPolicyId, VIEW_RESOLVED))
                        .getValue("entries").orElseThrow().asObject();

        assertThat(resolvedEntries)
                .as("policy-view=resolved on no-imports policy must equal original entries")
                .isEqualTo(originalEntries);
    }

    @Test
    public void policyViewResolvedDoesNotLeakHiddenFields() {
        putPolicy(importedPolicyId, importedPolicyFullAccess).expectingHttpStatus(CREATED).fire();

        // The resolved view must mirror the original-view shape: hidden fields like _revision / _modified /
        // __lifecycle are opt-in via fields= and absent by default. The merge step keeps only the top-level
        // keys present in the strategy-built original entity, so it doesn't leak special fields the user
        // didn't ask for.
        final JsonObject originalBody = JsonFactory.newObject(getPolicyAsJsonString(importedPolicyId, null));
        final JsonObject resolvedBody = JsonFactory.newObject(getPolicyAsJsonString(importedPolicyId, VIEW_RESOLVED));

        assertThat(resolvedBody.getKeys())
                .as("resolved view top-level keys must equal original view")
                .containsExactlyInAnyOrderElementsOf(originalBody.getKeys());
        assertThat(resolvedBody.contains(JsonFactory.newPointer("_revision"))).isFalse();
        assertThat(resolvedBody.contains(JsonFactory.newPointer("_modified"))).isFalse();
        assertThat(resolvedBody.contains(JsonFactory.newPointer("__lifecycle"))).isFalse();
    }

    @Test
    public void policyViewResolvedRespectsFieldsSelector() {
        // full-access importing — same reason as policyViewResolvedMergesImportedEntries.
        final Policy policyWithImports =
                importingPolicyFullAccess.toBuilder().setPolicyImport(policyImport).build();
        putPolicy(importedPolicyId, importedPolicyRestrictedAccess).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, policyWithImports).expectingHttpStatus(CREATED).fire();

        // fields=entries on resolved view must trim everything except entries (no policyId, no imports),
        // matching the behaviour of fields=entries on the unresolved view.
        final JsonObject body = JsonFactory.newObject(
                getPolicy(importingPolicyId)
                        .withParam(POLICY_VIEW_PARAM, VIEW_RESOLVED)
                        .withParam("fields", "entries")
                        .expectingHttpStatus(OK)
                        .fire().body().asString());

        assertThat(body.getKeys().stream().map(Object::toString).toList())
                .as("only 'entries' key should be returned when fields=entries on resolved view")
                .containsExactly("entries");

        // and the merged content is still inside.
        final JsonObject entries = body.getValue("entries").orElseThrow().asObject();
        assertThat(entries.contains(JsonFactory.newPointer("imported-" + importedPolicyId + "-EXPLICIT")))
                .as("merged content still present under entries").isTrue();
    }

    @Test
    public void policyViewResolvedHasNoEtagHeader() {
        putPolicy(importedPolicyId, importedPolicyFullAccess).expectingHttpStatus(CREATED).fire();

        // Original view exposes etag.
        final String originalEtag =
                getPolicy(importedPolicyId).expectingHttpStatus(OK).fire().header("etag");
        assertThat(originalEtag).as("original view must expose etag").isNotEmpty();

        // Resolved view must NOT expose etag, because the etag would only reflect the importing policy's
        // revision and would otherwise let clients 304 stale merged content when imported state has changed.
        getPolicy(importedPolicyId)
                .withParam(POLICY_VIEW_PARAM, VIEW_RESOLVED)
                .expectingHttpStatus(OK)
                .expectingHeaderIsNotPresent("etag")
                .fire();
    }

    @Test
    public void policyViewResolvedIgnoresIfNoneMatchAndReturns200() {
        putPolicy(importedPolicyId, importedPolicyFullAccess).expectingHttpStatus(CREATED).fire();

        // Capture the policy's current etag from the original view, then send it as If-None-Match together
        // with policy-view=resolved. The resolved view must NOT short-circuit to 304 (the importing policy's
        // revision alone is a poor proxy for "merged content unchanged"); it must serve a fresh 200.
        final String etag = getPolicy(importedPolicyId).expectingHttpStatus(OK).fire().header("etag");
        assertThat(etag).as("baseline etag must be present").isNotEmpty();

        getPolicy(importedPolicyId)
                .withParam(POLICY_VIEW_PARAM, VIEW_RESOLVED)
                .withHeader("If-None-Match", etag)
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void policyViewIsNoOpOnEntriesSubResource() {
        final Policy policyWithImports =
                importingPolicyRestrictedAccess.toBuilder().setPolicyImport(policyImport).build();
        putPolicy(importedPolicyId, importedPolicyRestrictedAccess).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, policyWithImports).expectingHttpStatus(CREATED).fire();

        // policy-view is no longer auto-promoted via query-params-as-headers, so on the entries sub-resource
        // it must be a no-op: the response only contains local entries, never imported ones.
        final String entriesViewResolved = getPolicyEntries(importingPolicyId)
                .withParam(POLICY_VIEW_PARAM, VIEW_RESOLVED)
                .expectingHttpStatus(OK)
                .fire().body().asString();
        final String entriesNoView = getPolicyEntries(importingPolicyId)
                .expectingHttpStatus(OK)
                .fire().body().asString();

        assertThat(entriesViewResolved)
                .as("policy-view must be silently ignored on /entries (no merging)")
                .isEqualTo(entriesNoView);
        final JsonObject entries = JsonFactory.newObject(entriesViewResolved);
        assertThat(entries.getKeys().stream().map(Object::toString).filter(k -> k.startsWith("imported-")).toList())
                .as("no imported-* entries on /entries even with policy-view=resolved").isEmpty();
    }

    /**
     * Regression: when {@code policy-view} is invalid AND the policy exists, the WS error envelope used to
     * carry topic {@code unknown/unknown/policies/errors} and lose the request {@code correlation-id} because
     * the {@link org.eclipse.ditto.policies.model.PolicyViewInvalidException} was thrown without the response
     * {@code DittoHeaders} attached. The error envelope must keep the original correlation-id and resolve to
     * the actual policy id.
     */
    @Test
    public void retrievePolicyViaWebSocketWithInvalidPolicyViewKeepsCorrelationIdAndPolicyId() throws Exception {
        putPolicy(importedPolicyId, importedPolicyFullAccess).expectingHttpStatus(CREATED).fire();

        final String correlationId = "policy-view-ws-bogus-" + UUID.randomUUID();
        final ConnectivityTestWebsocketClient client = ConnectivityTestWebsocketClient.newInstance(
                dittoWsUrl(TestConstants.API_V_2),
                serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken());
        try {
            client.connect(correlationId);

            final DittoHeaders headers = DittoHeaders.newBuilder()
                    .correlationId(correlationId)
                    .responseRequired(true)
                    .putHeader(POLICY_VIEW_PARAM, "bogus")
                    .build();
            final RetrievePolicy retrievePolicy = RetrievePolicy.of(importedPolicyId, headers);

            final PolicyCommandResponse<?> response = (PolicyCommandResponse<?>)
                    client.send(retrievePolicy).get(20, TimeUnit.SECONDS);

            assertThat(response.getHttpStatus().getCode()).as("400 expected").isEqualTo(400);
            assertThat(response.getDittoHeaders().getCorrelationId())
                    .as("correlation-id must be preserved on error envelope").contains(correlationId);
            assertThat(response.getEntityId().toString())
                    .as("error envelope topic must resolve to the requested policy id, not unknown:unknown")
                    .isEqualTo(importedPolicyId.toString());
        } finally {
            client.disconnect();
        }
    }

    @Test
    public void policyViewResolvedMergesLocalEntryReferences() {
        // Build a consumer with a local entry reference: VIEWER -> OWNER (local).
        final Label ownerLabel = Label.of("OWNER");
        final Label viewerLabel = Label.of("VIEWER");
        final Subject defaultSubject = createDefaultSubject();
        final Policy localRefPolicy = PoliciesModelFactory.newPolicyBuilder(importingPolicyId)
                .forLabel(ownerLabel)
                    .setSubject(defaultSubject)
                    .setGrantedPermissions(policyResource("/"), READ, WRITE)
                    .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .forLabel(viewerLabel)
                    .setSubject(defaultSubject)
                .setReferencesFor(viewerLabel,
                        List.of((EntryReference) PoliciesModelFactory.newLocalEntryReference(ownerLabel)))
                .build();
        putPolicy(importingPolicyId, localRefPolicy).expectingHttpStatus(CREATED).fire();

        final JsonObject body = JsonFactory.newObject(getPolicyAsJsonString(importingPolicyId, VIEW_RESOLVED));
        final JsonObject viewer = body.getValue("entries/" + viewerLabel).orElseThrow().asObject();

        // VIEWER's own subject survives resolution.
        final JsonObject subjects = viewer.getValue("subjects").orElseThrow().asObject();
        assertThat(subjects.contains(JsonFactory.newPointer(defaultSubject.getId().toString()))).isTrue();

        // Local-ref to OWNER pulls OWNER's resources into VIEWER.
        final JsonObject resources = viewer.getValue("resources").orElseThrow().asObject();
        assertThat(resources.getKeys()).as("VIEWER inherits resources from local-ref target OWNER").isNotEmpty();
    }

    @Test
    public void policyViewEffectiveAliasIsRejected() {
        // The 'effective' alias was removed during review — only 'original' and 'resolved' are accepted.
        putPolicy(importedPolicyId, importedPolicyFullAccess).expectingHttpStatus(CREATED).fire();

        getPolicy(importedPolicyId)
                .withParam(POLICY_VIEW_PARAM, "effective")
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    @Test
    public void policyViewParameterIsCaseInsensitive() {
        putPolicy(importedPolicyId, importedPolicyFullAccess).expectingHttpStatus(CREATED).fire();

        getPolicy(importedPolicyId).withParam(POLICY_VIEW_PARAM, "Resolved").expectingHttpStatus(OK).fire();
        getPolicy(importedPolicyId).withParam(POLICY_VIEW_PARAM, "RESOLVED").expectingHttpStatus(OK).fire();
        getPolicy(importedPolicyId).withParam(POLICY_VIEW_PARAM, "ORIGINAL").expectingHttpStatus(OK).fire();
    }

    @Test
    public void invalidPolicyViewValueReturnsBadRequest() {
        putPolicy(importedPolicyId, importedPolicyFullAccess).expectingHttpStatus(CREATED).fire();

        getPolicy(importedPolicyId)
                .withParam(POLICY_VIEW_PARAM, "junk")
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    @Test
    public void policyViewResolvedOnNonExistentPolicyReturnsNotFound() {
        // Parameter is valid → 400 doesn't apply; missing policy surfaces as 404.
        getPolicy(importedPolicyId)
                .withParam(POLICY_VIEW_PARAM, VIEW_RESOLVED)
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    /**
     * Cross-protocol parity: the resolution lives in {@code PolicyCommandEnforcement.filterResponse}
     * (transport-neutral), so a {@code RetrievePolicy} sent over WebSocket with a {@code policy-view: resolved}
     * header must produce the same merged view as the HTTP test above.
     */
    @Test
    public void retrievePolicyViaWebSocketWithPolicyViewResolvedReturnsMergedView() throws Exception {
        // full-access importing — same reason as policyViewResolvedMergesImportedEntries.
        final Policy policyWithImports =
                importingPolicyFullAccess.toBuilder().setPolicyImport(policyImport).build();
        putPolicy(importedPolicyId, importedPolicyRestrictedAccess).expectingHttpStatus(CREATED).fire();
        putPolicy(importingPolicyId, policyWithImports).expectingHttpStatus(CREATED).fire();

        final String correlationId = "policy-view-ws-" + UUID.randomUUID();
        final ConnectivityTestWebsocketClient client = ConnectivityTestWebsocketClient.newInstance(
                dittoWsUrl(TestConstants.API_V_2),
                serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken());
        try {
            client.connect(correlationId);

            final DittoHeaders headers = DittoHeaders.newBuilder()
                    .correlationId(correlationId)
                    .responseRequired(true)
                    .putHeader(POLICY_VIEW_PARAM, VIEW_RESOLVED)
                    .build();
            final RetrievePolicy retrievePolicy = RetrievePolicy.of(importingPolicyId, headers);

            final PolicyCommandResponse<?> response = (PolicyCommandResponse<?>)
                    client.send(retrievePolicy).get(20, TimeUnit.SECONDS);

            assertThat(response).as("expected RetrievePolicyResponse").isInstanceOf(RetrievePolicyResponse.class);
            final RetrievePolicyResponse retrieveResp = (RetrievePolicyResponse) response;
            final JsonObject entries = retrieveResp.getPolicy().toJson()
                    .getValue("entries").orElseThrow().asObject();

            assertThat(entries.contains(JsonFactory.newPointer("imported-" + importedPolicyId + "-EXPLICIT")))
                    .isTrue();
            assertThat(entries.contains(JsonFactory.newPointer("imported-" + importedPolicyId + "-IMPLICIT")))
                    .isTrue();
        } finally {
            client.disconnect();
        }
    }

    private static String getPolicyAsJsonString(final PolicyId policyId, final String viewValue) {
        final var matcher = getPolicy(policyId).expectingHttpStatus(OK);
        if (viewValue != null) {
            matcher.withParam(POLICY_VIEW_PARAM, viewValue);
        }
        return matcher.fire().body().asString();
    }

    private static PolicyImports createPolicyImports(final int importsNumber, final PolicyId importedPolicyId) {
        final Collection<PolicyImport> allPolicyImports =
                new ArrayList<>(1 + importsNumber);
        for (int i = 1; i <= importsNumber; i++) {
            allPolicyImports.add(PoliciesModelFactory.newPolicyImport(
                    PolicyId.of(importedPolicyId.toString() + i)));
        }
        return PoliciesModelFactory.newPolicyImports(allPolicyImports);
    }

    private static Subject createDefaultSubject() {
        return serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject();
    }

    private static Policy buildImportedPolicyRestrictedAccess(final PolicyId policyId) {

        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(createDefaultSubject())
                .setGrantedPermissions(policyResource("/"), WRITE)
                .setImportable(ImportableType.NEVER)
                .forLabel("EXPLICIT")
                .setSubject(createDefaultSubject())
                .setGrantedPermissions(policyResource("/entries/EXPLICIT"), READ)
                .setImportable(ImportableType.EXPLICIT)
                .forLabel("IMPLICIT")
                .setSubject(createDefaultSubject())
                .setGrantedPermissions(policyResource("/entries/IMPLICIT"), READ)
                .forLabel("NEVER")
                .setSubject(createDefaultSubject())
                .setGrantedPermissions(policyResource("/entries/NEVER"), READ)
                .setImportable(ImportableType.NEVER)
                .build();
    }

    private static Policy buildImportedPolicyFullAccess(final PolicyId policyId) {

        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(createDefaultSubject())
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .build();
    }

    private static Policy buildMinimalPolicy(final PolicyId policyId) {

        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(createDefaultSubject())
                .setGrantedPermissions(policyResource("/"), WRITE)
                .build();
    }

    private static Policy buildImportingPolicyFullAccess(final PolicyId policyId, final Subject subject,
            final PolicyImport... policyImports) {
        final List<PolicyImport> policyImportList = Arrays.stream(policyImports).collect(Collectors.toList());
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(subject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setPolicyImports(PolicyImports.newInstance(policyImportList))
                .build();
    }

    private static Policy buildImportingPolicyRestrictedAccess(final PolicyId policyId, final Subject subject,
            final PolicyImport... policyImports) {
        final List<PolicyImport> policyImportList = Arrays.stream(policyImports).collect(Collectors.toList());
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(subject)
                .setGrantedPermissions(policyResource("/"), WRITE)
                .setImportable(ImportableType.NEVER)
                .forLabel("IMPORTS")
                .setSubject(subject)
                .setGrantedPermissions(policyResource("/imports"), READ, WRITE)
                .setGrantedPermissions(policyResource("/entries/IMPORTS"), READ)
                .setPolicyImports(PolicyImports.newInstance(policyImportList))
                .build();
    }

    private static PolicyImport createPolicyImports(final PolicyId importedPolicyId, final String... labels) {
        return PoliciesModelFactory.newPolicyImport(importedPolicyId,
                EffectedImports.newInstance(Arrays.stream(labels).map(Label::of).collect(Collectors.toList())));
    }

    private static PutMatcher putPolicyImports(final CharSequence policyId, final PolicyImports policyImports) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImports().toString();
        final String thingsServiceUrl = dittoUrl(TestConstants.API_V_2, path);
        final String jsonString = policyImports.toJsonString();

        LOGGER.debug("PUTing Policy Imports JSON to URL '{}': {}", thingsServiceUrl, jsonString);
        return put(thingsServiceUrl, jsonString).withLogging(LOGGER, "Policy");
    }

    private static GetMatcher getPolicyImports(final CharSequence policyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImports().toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "Policy");
    }

    private static GetMatcher getPolicyImport(final CharSequence policyId, final CharSequence importedPolicyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImport(importedPolicyId).toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "Policy");
    }

    private static DeleteMatcher deletePolicyImport(final CharSequence policyId, final CharSequence importedPolicyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyImport(importedPolicyId).toString();
        return delete(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "Policy");
    }

}
