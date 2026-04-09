/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
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
package org.eclipse.ditto.testing.system.things.ws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.policyResource;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.thingResource;
import static org.eclipse.ditto.testing.common.TestConstants.API_V_2;
import static org.eclipse.ditto.things.api.Permission.READ;
import static org.eclipse.ditto.things.api.Permission.WRITE;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.policies.model.EffectedImports;
import org.eclipse.ditto.policies.model.EntriesAdditions;
import org.eclipse.ditto.policies.model.EntryAddition;
import org.eclipse.ditto.policies.model.ImportsAlias;
import org.eclipse.ditto.policies.model.ImportsAliases;
import org.eclipse.ditto.policies.model.ImportsAliasTarget;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.PolicyImport;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectType;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.policies.model.signals.commands.modify.CreatePolicy;
import org.eclipse.ditto.policies.model.signals.commands.modify.CreatePolicyResponse;
import org.eclipse.ditto.policies.model.signals.commands.modify.DeleteImportsAlias;
import org.eclipse.ditto.policies.model.signals.commands.modify.DeleteImportsAliasResponse;
import org.eclipse.ditto.policies.model.signals.commands.modify.DeleteImportsAliases;
import org.eclipse.ditto.policies.model.signals.commands.modify.DeleteImportsAliasesResponse;
import org.eclipse.ditto.policies.model.signals.commands.modify.DeletePolicy;
import org.eclipse.ditto.policies.model.signals.commands.modify.ModifyImportsAlias;
import org.eclipse.ditto.policies.model.signals.commands.modify.ModifyImportsAliasResponse;
import org.eclipse.ditto.policies.model.signals.commands.modify.ModifyImportsAliases;
import org.eclipse.ditto.policies.model.signals.commands.modify.ModifyImportsAliasesResponse;
import org.eclipse.ditto.policies.model.signals.commands.modify.ModifySubjects;
import org.eclipse.ditto.policies.model.signals.commands.modify.ModifySubjectsResponse;
import org.eclipse.ditto.policies.model.signals.commands.query.RetrieveImportsAlias;
import org.eclipse.ditto.policies.model.signals.commands.query.RetrieveImportsAliasResponse;
import org.eclipse.ditto.policies.model.signals.commands.query.RetrieveImportsAliases;
import org.eclipse.ditto.policies.model.signals.commands.query.RetrieveImportsAliasesResponse;
import org.eclipse.ditto.policies.model.signals.commands.query.RetrieveSubjects;
import org.eclipse.ditto.policies.model.signals.commands.query.RetrieveSubjectsResponse;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ServiceEnvironment;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.config.TestEnvironment;
import org.eclipse.ditto.testing.common.ws.ThingsWebsocketClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WebSocket / Ditto Protocol integration tests for policy import aliases — verifying that CRUD operations and
 * subject fan-out work correctly through the WebSocket channel.
 */
public final class PolicyImportsAliasesWebSocketIT extends IntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PolicyImportsAliasesWebSocketIT.class);
    private static final long TIMEOUT_SECONDS = 15;

    private static final Label ALIAS_LABEL = Label.of("operator");
    private static final Label TARGET_LABEL_1 = Label.of("operator-reactor");
    private static final Label TARGET_LABEL_2 = Label.of("operator-turbine");

    private ThingsWebsocketClient wsClient;
    private TestingContext testingContext;

    private PolicyId templatePolicyId;
    private PolicyId importingPolicyId;
    private ImportsAlias alias;
    private ImportsAliases aliases;

    @Before
    public void setUp() {
        if (TestConfig.getInstance().getTestEnvironment() == TestEnvironment.DEPLOYMENT) {
            testingContext = serviceEnv.getDefaultTestingContext();
        } else {
            testingContext = TestingContext.withGeneratedMockClient(
                    ServiceEnvironment.createSolutionWithRandomUsernameRandomNamespace(), TEST_CONFIG);
        }

        wsClient = newTestWebsocketClient(testingContext, new HashMap<>(), API_V_2);
        wsClient.connect("PolicyImportsAliasesWS-" + UUID.randomUUID());

        templatePolicyId = PolicyId.inNamespaceWithRandomName(
                testingContext.getSolution().getDefaultNamespace());
        importingPolicyId = PolicyId.inNamespaceWithRandomName(
                testingContext.getSolution().getDefaultNamespace());

        final List<ImportsAliasTarget> targets = Arrays.asList(
                PoliciesModelFactory.newImportsAliasTarget(templatePolicyId, TARGET_LABEL_1),
                PoliciesModelFactory.newImportsAliasTarget(templatePolicyId, TARGET_LABEL_2)
        );
        alias = PoliciesModelFactory.newImportsAlias(ALIAS_LABEL, targets);
        aliases = PoliciesModelFactory.newImportsAliases(Collections.singletonList(alias));
    }

    @After
    public void tearDown() {
        if (wsClient != null) {
            try {
                wsClient.send(DeletePolicy.of(importingPolicyId, headers())).toCompletableFuture()
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (final Exception e) {
                LOGGER.debug("Cleanup of importing policy failed: {}", e.getMessage());
            }
            try {
                wsClient.send(DeletePolicy.of(templatePolicyId, headers())).toCompletableFuture()
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (final Exception e) {
                LOGGER.debug("Cleanup of template policy failed: {}", e.getMessage());
            }
            wsClient.disconnect();
        }
    }

    // --- CRUD via WebSocket ---

    @Test
    public void createPolicyWithAliasesAndRetrieveViaWebSocket() throws Exception {
        createTemplateThenImportingPolicy();

        final Signal<?> response = wsClient.send(
                RetrieveImportsAliases.of(importingPolicyId, headers())
        ).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(response).isInstanceOf(RetrieveImportsAliasesResponse.class);
        final RetrieveImportsAliasesResponse retrieveResponse = (RetrieveImportsAliasesResponse) response;
        assertThat(retrieveResponse.getImportsAliases()).isEqualTo(aliases);
    }

    @Test
    public void retrieveSingleImportsAliasViaWebSocket() throws Exception {
        createTemplateThenImportingPolicy();

        final Signal<?> response = wsClient.send(
                RetrieveImportsAlias.of(importingPolicyId, ALIAS_LABEL, headers())
        ).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(response).isInstanceOf(RetrieveImportsAliasResponse.class);
        final RetrieveImportsAliasResponse retrieveResponse = (RetrieveImportsAliasResponse) response;
        assertThat(retrieveResponse.getImportsAlias()).isEqualTo(alias);
    }

    @Test
    public void modifySingleImportsAliasViaWebSocket() throws Exception {
        createTemplateThenImportingPolicy();

        // Modify alias to have only one target
        final ImportsAlias modifiedAlias = PoliciesModelFactory.newImportsAlias(ALIAS_LABEL,
                Collections.singletonList(
                        PoliciesModelFactory.newImportsAliasTarget(templatePolicyId, TARGET_LABEL_1)));

        final Signal<?> modifyResponse = wsClient.send(
                ModifyImportsAlias.of(importingPolicyId, modifiedAlias, headers())
        ).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(modifyResponse).isInstanceOf(ModifyImportsAliasResponse.class);

        // Verify the modification
        final Signal<?> retrieveResponse = wsClient.send(
                RetrieveImportsAlias.of(importingPolicyId, ALIAS_LABEL, headers())
        ).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(retrieveResponse).isInstanceOf(RetrieveImportsAliasResponse.class);
        assertThat(((RetrieveImportsAliasResponse) retrieveResponse).getImportsAlias()).isEqualTo(modifiedAlias);
    }

    @Test
    public void modifyAllImportsAliasesViaWebSocket() throws Exception {
        createTemplateThenImportingPolicy();

        // Replace all aliases with a new one
        final Label newLabel = Label.of("inspector");
        final ImportsAlias newAlias = PoliciesModelFactory.newImportsAlias(newLabel,
                Collections.singletonList(
                        PoliciesModelFactory.newImportsAliasTarget(templatePolicyId, TARGET_LABEL_1)));
        final ImportsAliases newAliases = PoliciesModelFactory.newImportsAliases(Collections.singletonList(newAlias));

        final Signal<?> modifyResponse = wsClient.send(
                ModifyImportsAliases.of(importingPolicyId, newAliases, headers())
        ).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(modifyResponse).isInstanceOf(ModifyImportsAliasesResponse.class);

        // Verify
        final Signal<?> retrieveResponse = wsClient.send(
                RetrieveImportsAliases.of(importingPolicyId, headers())
        ).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(retrieveResponse).isInstanceOf(RetrieveImportsAliasesResponse.class);
        assertThat(((RetrieveImportsAliasesResponse) retrieveResponse).getImportsAliases()).isEqualTo(newAliases);
    }

    @Test
    public void deleteSingleImportsAliasViaWebSocket() throws Exception {
        createTemplateThenImportingPolicy();

        final Signal<?> deleteResponse = wsClient.send(
                DeleteImportsAlias.of(importingPolicyId, ALIAS_LABEL, headers())
        ).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(deleteResponse).isInstanceOf(DeleteImportsAliasResponse.class);

        // Verify deletion — retrieve should return empty aliases
        final Signal<?> retrieveResponse = wsClient.send(
                RetrieveImportsAliases.of(importingPolicyId, headers())
        ).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(retrieveResponse).isInstanceOf(RetrieveImportsAliasesResponse.class);
        assertThat(((RetrieveImportsAliasesResponse) retrieveResponse).getImportsAliases().isEmpty()).isTrue();
    }

    @Test
    public void deleteAllImportsAliasesViaWebSocket() throws Exception {
        createTemplateThenImportingPolicy();

        final Signal<?> deleteResponse = wsClient.send(
                DeleteImportsAliases.of(importingPolicyId, headers())
        ).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(deleteResponse).isInstanceOf(DeleteImportsAliasesResponse.class);

        // Verify
        final Signal<?> retrieveResponse = wsClient.send(
                RetrieveImportsAliases.of(importingPolicyId, headers())
        ).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(retrieveResponse).isInstanceOf(RetrieveImportsAliasesResponse.class);
        assertThat(((RetrieveImportsAliasesResponse) retrieveResponse).getImportsAliases().isEmpty()).isTrue();
    }

    // --- Subject fan-out via WebSocket ---

    @Test
    public void modifySubjectsViaAliasLabelFansOutThroughWebSocket() throws Exception {
        createTemplateThenImportingPolicy();

        final Subject newSubject = Subject.newInstance(
                serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject().getId(),
                SubjectType.GENERATED);
        final Subjects subjects = Subjects.newInstance(newSubject);

        // ModifySubjects using the alias label → should fan out to all targets
        final Signal<?> modifyResponse = wsClient.send(
                ModifySubjects.of(importingPolicyId, ALIAS_LABEL, subjects, headers())
        ).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(modifyResponse).isInstanceOf(ModifySubjectsResponse.class);

        // Retrieve subjects through alias — should return subjects from first target
        final Signal<?> retrieveResponse = wsClient.send(
                RetrieveSubjects.of(importingPolicyId, ALIAS_LABEL, headers())
        ).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(retrieveResponse).isInstanceOf(RetrieveSubjectsResponse.class);
        final RetrieveSubjectsResponse subjectsResponse = (RetrieveSubjectsResponse) retrieveResponse;
        assertThat(subjectsResponse.getSubjects().getSubject(newSubject.getId())).isPresent();
    }

    // --- Helpers ---

    private DittoHeaders headers() {
        return DittoHeaders.newBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .randomCorrelationId()
                .build();
    }

    private void createTemplateThenImportingPolicy() throws Exception {
        // Create template policy via REST (simpler setup)
        final Policy templatePolicy = PoliciesModelFactory.newPolicyBuilder(templatePolicyId)
                .forLabel("ADMIN")
                .setSubject(testingContext.getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setImportable(ImportableType.NEVER)
                .forLabel("operator-reactor")
                .setSubject(testingContext.getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .setImportable(ImportableType.EXPLICIT)
                .forLabel("operator-turbine")
                .setSubject(testingContext.getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .setImportable(ImportableType.EXPLICIT)
                .build();

        final Signal<?> createTemplateResponse = wsClient.send(
                CreatePolicy.of(templatePolicy, headers())
        ).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(createTemplateResponse).isInstanceOf(CreatePolicyResponse.class);

        // Build import with entries additions
        final EntryAddition addition1 = PoliciesModelFactory.newEntryAddition(TARGET_LABEL_1, null, null);
        final EntryAddition addition2 = PoliciesModelFactory.newEntryAddition(TARGET_LABEL_2, null, null);
        final EntriesAdditions entriesAdditions =
                PoliciesModelFactory.newEntriesAdditions(Arrays.asList(addition1, addition2));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                Arrays.asList(TARGET_LABEL_1, TARGET_LABEL_2), entriesAdditions);
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(templatePolicyId, effectedImports);

        // Create importing policy with alias via WebSocket
        final Policy importingPolicy = PoliciesModelFactory.newPolicyBuilder(importingPolicyId)
                .forLabel("ADMIN")
                .setSubject(testingContext.getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .setPolicyImports(PoliciesModelFactory.newPolicyImports(Collections.singletonList(policyImport)))
                .setImportsAliases(aliases)
                .build();

        final Signal<?> createImportingResponse = wsClient.send(
                CreatePolicy.of(importingPolicy, headers())
        ).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(createImportingResponse).isInstanceOf(CreatePolicyResponse.class);
    }

}
