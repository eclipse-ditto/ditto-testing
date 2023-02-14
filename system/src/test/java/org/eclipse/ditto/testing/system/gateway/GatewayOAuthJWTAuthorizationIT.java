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
package org.eclipse.ditto.testing.system.gateway;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.api.Permission;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.Resources;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ServiceEnvironment;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.ThingJsonProducer;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.ws.ThingsWebsocketClient;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration test for the authorization of OAuth JSON Web Tokens at the API Gateway.
 */
public final class GatewayOAuthJWTAuthorizationIT extends IntegrationTest {

    private static final String RANDOM_NAMESPACE = ServiceEnvironment.createRandomDefaultNamespace();

    @Test
    @Category({Acceptance.class})
    public void postThingWithOAuthToken() throws IllegalStateException {
        final AuthClient authClient = serviceEnv.getDefaultTestingContext().getOAuthClient();
        final String jwtToken = authClient.getAccessToken();

        final ThingId thingId = ThingId.of(idGenerator(RANDOM_NAMESPACE).withRandomName());

        putThing(TestConstants.API_V_2, Thing.newBuilder().setId(thingId).build(), JsonSchemaVersion.V_2)
                .withJWT(jwtToken)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final EffectedPermissions readWriteGranted =
                EffectedPermissions.newInstance(Arrays.asList(Permission.READ, Permission.WRITE), null);
        final Resources resources = Resources.newInstance(
                Resource.newInstance(PoliciesResourceType.policyResource("/"), readWriteGranted),
                Resource.newInstance(PoliciesResourceType.thingResource("/"), readWriteGranted));
        final PolicyEntry policyEntry = PoliciesModelFactory.newPolicyEntry(
                "O-AUTH-CLIENT", Subjects.newInstance(authClient.getSubject()), resources);

        // add permissions user for clean up process
        putPolicyEntry(thingId, policyEntry)
                .withJWT(jwtToken)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getPolicy(thingId)
                .withJWT(jwtToken)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    @Category({Acceptance.class})
    public void refreshJwtViaWebSocket() throws InterruptedException {
        final AuthClient authClient = serviceEnv.getDefaultTestingContext().getOAuthClient();
        final ThingsWebsocketClient thingsWebsocketClient =
                newTestWebsocketClient(authClient.getAccessToken(), Collections.emptyMap(),
                        TestConstants.API_V_2);

        thingsWebsocketClient.connect("refreshJwtViaWebSocket-" + UUID.randomUUID());

        thingsWebsocketClient.refresh(authClient.getAccessToken());

        Thread.sleep(1000L);

        assertThat(thingsWebsocketClient.isOpen()).isTrue();

        thingsWebsocketClient.disconnect();
    }

    @Test
    public void enrichMessagesViaWebSocketWithJwtAsQueryParameter() {
        final AuthClient authClient = serviceEnv.getDefaultTestingContext().getOAuthClient();
        final AuthClient authClient2 = TestingContext.withGeneratedMockClient(
                serviceEnv.getTestingContext2().getSolution(), TEST_CONFIG).getOAuthClient();

        final String wsJwtToken = authClient.getAccessToken();
        final String restJwtToken = authClient2.getAccessToken();
        final ThingsWebsocketClient thingsWebsocketClient = newTestWebsocketClient(wsJwtToken, Collections.emptyMap(),
                TestConstants.API_V_2, ThingsWebsocketClient.JwtAuthMethod.QUERY_PARAM);
        thingsWebsocketClient.connect("enrichMessagesViaWebSocketWithJwtAsQueryParameter-" + UUID.randomUUID());

        final List<Adaptable> adaptableList = new CopyOnWriteArrayList<>();

        thingsWebsocketClient.onAdaptable(adaptable -> {
            adaptableList.add(adaptable);
            LOGGER.info("WS event: {}", adaptable);
        });
        assertThat(thingsWebsocketClient.isOpen()).isTrue();
        final CompletableFuture<Void> ackConfirmed =
                thingsWebsocketClient.sendProtocolCommand("START-SEND-EVENTS?namespaces=" + RANDOM_NAMESPACE + "&extraFields=attributes",
                        "START-SEND-EVENTS:ACK");
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> Assertions.assertThat(ackConfirmed.isDone()).isTrue());

        final ThingId thingId = ThingId.of(idGenerator(RANDOM_NAMESPACE).withRandomName());
        final Thing thing = new ThingJsonProducer().getThing().toBuilder().setId(thingId).build();
        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .withJWT(restJwtToken)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final EffectedPermissions readWriteGranted =
                EffectedPermissions.newInstance(Arrays.asList(Permission.READ, Permission.WRITE), null);
        final Resources resources = Resources.newInstance(
                Resource.newInstance(PoliciesResourceType.policyResource("/"), readWriteGranted),
                Resource.newInstance(PoliciesResourceType.thingResource("/"), readWriteGranted));
        final PolicyEntry policyEntry = PoliciesModelFactory.newPolicyEntry(
                "O-AUTH-CLIENT", Subjects.newInstance(authClient.getSubject(), authClient2.getSubject()),
                resources);

        putPolicyEntry(thingId, policyEntry)
                .withJWT(restJwtToken)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .print();

        patchThing(ThingId.of(thingId), JsonPointer.of("features/Vehicle/properties/status/speed"), JsonValue.of(0))
                .withJWT(restJwtToken)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire()
                .print();

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(1))
                .untilAsserted(() -> Assertions.assertThat(adaptableList).hasSize(1));

        Assertions.assertThat(adaptableList.get(0).getPayload().getExtra()).isPresent();
        Assertions.assertThat(adaptableList.get(0).getPayload().getExtra()
                .map(payload -> payload.contains("attributes"))
                .orElse(false)).isTrue();

        thingsWebsocketClient.disconnect();
    }
}
