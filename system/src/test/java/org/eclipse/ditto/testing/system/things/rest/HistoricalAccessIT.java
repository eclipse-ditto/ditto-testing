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

import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
import static org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT;

import java.util.Collections;
import java.util.List;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.Test;

/**
 * Integration test for the REST API, accessing entities at a historical revision
 * (e.g. using {@code at-historical-revision}).
 */
public final class HistoricalAccessIT extends IntegrationTest {


    @Test
    public void createAndUpdateThingRetrievingInitialHistoricalState() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing newThing = Thing.newBuilder()
                .setId(thingId)
                .setAttribute(JsonPointer.of("counter"), JsonValue.of(0))
                .build();

        putThing(TestConstants.API_V_2, newThing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        final Thing thingRev1 = newThing.toBuilder()
                .setRevision(1L)
                .setPolicyId(PolicyId.of(thingId))
                .build();

        getThing(TestConstants.API_V_2, thingId.toString())
                .withParam("fields", "thingId,policyId,attributes,_revision")
                .expectingBody(contains(thingRev1.toJson()))
                .fire();

        putAttribute(TestConstants.API_V_2, thingId.toString(), "counter", String.valueOf(1))
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        final Thing thingRev2 = thingRev1.toBuilder()
                .setRevision(2L)
                .setAttribute(JsonPointer.of("counter"), JsonValue.of(1))
                .build();

        getThing(TestConstants.API_V_2, thingId.toString())
                .withParam("fields", "thingId,policyId,attributes,_revision")
                .expectingBody(contains(thingRev2.toJson()))
                .fire();

        getThing(TestConstants.API_V_2, thingId.toString())
                .withParam(DittoHeaderDefinition.AT_HISTORICAL_REVISION.getKey(), "1")
                .expectingBody(contains(thingRev1.toJson()))
                .fire();

        deleteAttribute(TestConstants.API_V_2, thingId, "counter")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getThing(TestConstants.API_V_2, thingId.toString())
                .withParam(DittoHeaderDefinition.AT_HISTORICAL_REVISION.getKey(), "2")
                .expectingBody(contains(thingRev2.toJson()))
                .fire();
    }

    @Test
    public void createAndUpdatePolicyRetrievingHistoricalState() {
        final PolicyId policyId = PolicyId.of(idGenerator().withRandomName());
        final Policy newPolicy = Policy.newBuilder()
                .setId(policyId)
                .forLabel(Label.of("DEFAULT"))
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getSubject())
                .setResource(Resource.newInstance("policy", "/",
                        EffectedPermissions.newInstance(List.of("READ", "WRITE"), List.of())))
                .build();

        putPolicy(newPolicy)
                .expectingHttpStatus(CREATED)
                .fire();

        final Policy policyRev1 = newPolicy.toBuilder()
                .setRevision(1L)
                .build();

        getPolicy(policyId.toString())
                .withParam("fields", "policyId,imports,entries,_revision")
                .expectingBody(contains(policyRev1.toJson()))
                .fire();

        final PolicyEntry policyEntry = PolicyEntry.newInstance("ANOTHER",
                Collections.singleton(Subject.newInstance(SubjectIssuer.INTEGRATION, "fofo")),
                Collections.singleton(Resource.newInstance("thing", "/attributes",
                        EffectedPermissions.newInstance(List.of("READ"), List.of())))
        );
        putPolicyEntry(policyId.toString(), policyEntry)
                .expectingHttpStatus(CREATED)
                .fire();

        final Policy policyRev2 = policyRev1.toBuilder()
                .setRevision(2L)
                .set(policyEntry)
                .build();

        getPolicy(policyId.toString())
                .withParam("fields", "policyId,imports,entries,_revision")
                .expectingBody(contains(policyRev2.toJson()))
                .fire();

        getPolicy(policyId.toString())
                .withParam(DittoHeaderDefinition.AT_HISTORICAL_REVISION.getKey(), "1")
                .expectingBody(contains(policyRev1.toJson()))
                .fire();

        deletePolicyEntry(policyId, "ANOTHER")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getPolicy(policyId.toString())
                .withParam(DittoHeaderDefinition.AT_HISTORICAL_REVISION.getKey(), "2")
                .expectingBody(contains(policyRev2.toJson()))
                .fire();
    }

}
