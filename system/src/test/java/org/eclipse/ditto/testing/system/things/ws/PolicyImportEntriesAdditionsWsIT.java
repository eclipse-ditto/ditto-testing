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
import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
import static org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.policyResource;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.thingResource;
import static org.eclipse.ditto.testing.common.TestConstants.API_V_2;
import static org.eclipse.ditto.things.api.Permission.READ;
import static org.eclipse.ditto.things.api.Permission.WRITE;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.signals.commands.CommandResponse;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.model.AllowedImportAddition;
import org.eclipse.ditto.policies.model.EffectedImports;
import org.eclipse.ditto.policies.model.EntriesAdditions;
import org.eclipse.ditto.policies.model.EntryAddition;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.PolicyImport;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.signals.commands.modify.ModifyPolicyImport;
import org.eclipse.ditto.policies.model.signals.commands.modify.ModifyPolicyImportResponse;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.ws.ThingsWebsocketClient;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.exceptions.ThingNotAccessibleException;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThingResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.DeleteThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThingResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WebSocket integration tests for policy import {@code entriesAdditions} and {@code allowedImportAdditions}.
 */
public final class PolicyImportEntriesAdditionsWsIT extends IntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PolicyImportEntriesAdditionsWsIT.class);
    private static final long TIMEOUT_SECONDS = 20L;

    private ThingsWebsocketClient clientUser1;
    private ThingsWebsocketClient clientUser2;
    private Subject defaultSubject;
    private Subject subject2;

    @Before
    public void setUp() {
        defaultSubject = serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject();
        subject2 = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();

        clientUser1 = newTestWebsocketClient(serviceEnv.getDefaultTestingContext(), Map.of(), API_V_2);
        clientUser2 = newTestWebsocketClient(serviceEnv.getTestingContext2(), Map.of(), API_V_2);

        clientUser1.connect("WsClient-User1-" + UUID.randomUUID());
        clientUser2.connect("WsClient-User2-" + UUID.randomUUID());
    }

    @After
    public void tearDown() {
        if (clientUser1 != null) {
            clientUser1.disconnect();
        }
        if (clientUser2 != null) {
            clientUser2.disconnect();
        }
    }

    @Test
    public void modifyPolicyImportWithEntriesAdditionsViaWebSocket() throws Exception {
        final PolicyId importedPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("imported"));
        final PolicyId importingPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("importing"));

        // Create imported (template) policy via REST - allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Create importing policy via REST (without imports initially)
        final Policy importingPolicy = buildImportingPolicy(importingPolicyId);
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Now modify the policy import via WebSocket with entriesAdditions
        final PolicyImport policyImport = buildPolicyImportWithSubjectAdditions(importedPolicyId, subject2);
        final ModifyPolicyImport modifyPolicyImport = ModifyPolicyImport.of(
                importingPolicyId, policyImport, dittoHeaders());

        final CommandResponse<?> response = clientUser1.send(modifyPolicyImport)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(response).isInstanceOf(ModifyPolicyImportResponse.class);
        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.CREATED);
    }

    @Test
    public void retrieveThingViaWebSocketAfterSubjectAddedViaAdditions() throws Exception {
        final PolicyId importedPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("imported"));
        final PolicyId importingPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("importing"));
        final ThingId thingId = ThingId.of(importingPolicyId);

        // Create imported (template) policy via REST - allows subject additions, grants thing:/ READ
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Create importing policy with subject2 added via entriesAdditions
        final PolicyImport policyImport = buildPolicyImportWithSubjectAdditions(importedPolicyId, subject2);
        final Policy importingPolicy = buildImportingPolicy(importingPolicyId).toBuilder()
                .setPolicyImport(policyImport)
                .build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create thing via WS with user1
        final Thing thing = Thing.newBuilder().setId(thingId).build();
        final CreateThing createThing = CreateThing.of(thing, null, dittoHeaders());
        final CommandResponse<?> createResponse = clientUser1.send(createThing)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(createResponse).isInstanceOf(CreateThingResponse.class);

        // Verify user2 can retrieve the thing via WS (subject was added through entriesAdditions)
        final RetrieveThing retrieveThing = RetrieveThing.of(thingId, dittoHeaders());
        final CommandResponse<?> retrieveResponse = clientUser2.send(retrieveThing)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(retrieveResponse).isInstanceOf(RetrieveThingResponse.class);

        // Cleanup
        clientUser1.send(DeleteThing.of(thingId, dittoHeaders()));
    }

    @Test
    public void modifyPolicyImportWithDisallowedAdditionsViaWebSocket() throws Exception {
        final PolicyId importedPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("imported"));
        final PolicyId importingPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("importing"));

        // Create imported (template) policy via REST - NO allowedImportAdditions
        final Policy importedPolicy = buildImportedPolicyWithoutAllowedAdditions(importedPolicyId);
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Create importing policy via REST (without imports initially)
        final Policy importingPolicy = buildImportingPolicy(importingPolicyId);
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Attempt to modify policy import via WebSocket with disallowed entriesAdditions
        final PolicyImport policyImport = buildPolicyImportWithSubjectAdditions(importedPolicyId, subject2);
        final ModifyPolicyImport modifyPolicyImport = ModifyPolicyImport.of(
                importingPolicyId, policyImport, dittoHeaders());

        final CommandResponse<?> response = clientUser1.send(modifyPolicyImport)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Expect an error response
        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private Policy buildImportedPolicy(final PolicyId policyId,
            final Set<AllowedImportAddition> allowedImportAdditions) {

        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());

        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, allowedImportAdditions);

        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .set(adminEntry)
                .set(defaultEntry)
                .build();
    }

    private Policy buildImportedPolicyWithoutAllowedAdditions(final PolicyId policyId) {
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setImportable(ImportableType.NEVER)
                .forLabel("DEFAULT")
                .setSubject(defaultSubject)
                .setGrantedPermissions(thingResource("/"), READ)
                .build();
    }

    private Policy buildImportingPolicy(final PolicyId policyId) {
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .build();
    }

    private PolicyImport buildPolicyImportWithSubjectAdditions(final PolicyId importedPolicyId,
            final Subject additionalSubject) {

        final EntryAddition entryAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("DEFAULT"),
                PoliciesModelFactory.newSubjects(additionalSubject), null);
        final EntriesAdditions additions = PoliciesModelFactory.newEntriesAdditions(List.of(entryAddition));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")), additions);
        return PoliciesModelFactory.newPolicyImport(importedPolicyId, effectedImports);
    }

    private static DittoHeaders dittoHeaders() {
        return DittoHeaders.newBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .correlationId(UUID.randomUUID().toString())
                .build();
    }

}
