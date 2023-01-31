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
package org.eclipse.ditto.testing.system.client.retrieve;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.assertj.core.api.JUnitSoftAssertions;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.configuration.ClientCredentialsAuthenticationConfiguration;
import org.eclipse.ditto.client.messaging.AuthenticationProvider;
import org.eclipse.ditto.client.messaging.AuthenticationProviders;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonFieldDefinition;
import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonParseOptions;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.testing.system.client.util.ClientFactory;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neovisionaries.ws.client.WebSocket;

/**
 * Tests retrieve() command with CR Integration Client.
 */
public class RetrieveThingsIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetrieveThingsIT.class);

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    private ThingId thingId1;
    private ThingId thingId2;
    private ThingId thingId3;

    private static final String FIELD_ATTRIBUTE = "attributes/attr1";
    private static final String FIELD_THING_ID = "thingId";
    private static final String FIELD_FEATURES = "features";

    private static final JsonObject ATTRIBUTES = JsonFactory.newObjectBuilder()
            .set("attr1", "test1")
            .set("attr2", JsonFactory.newObjectBuilder().set("attr3", "test3").build())
            .build();
    private static final FeatureProperties FEATURE_PROPERTIES = FeatureProperties.newBuilder()
            .set("property", "object")
            .build();

    private static final JsonFieldDefinition<String> ATTR1 = JsonFieldDefinition.ofString("attr1");

    private DittoClient dittoClient;

    private AuthClient user1;
    private AuthClient user2;

    @Before
    public void setUp() throws Exception {
        LOGGER.info("-----> Running Test: {}", name.getMethodName());

        user1 = serviceEnv.getDefaultTestingContext().getOAuthClient();
        user2 = serviceEnv.getTestingContext2().getOAuthClient();

        dittoClient = newDittoClient(user1);

        thingId1 = ThingId.of(idGenerator().withRandomName());
        final Thing thing1 = ThingsModelFactory.newThingBuilder()
                .setId(thingId1)
                .setAttributes(ATTRIBUTES)
                .build();
        final Policy policy1 = newPolicy(PolicyId.of(thingId1), user1, user2);
        dittoClient.twin().create(thing1, policy1)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        thingId2 = ThingId.of(idGenerator().withRandomName());
        final Thing thing2 = ThingsModelFactory.newThingBuilder()
                .setId(thingId2)
                .setAttributes(ATTRIBUTES)
                .setFeature("first", ThingsModelFactory.newFeatureProperties(FEATURE_PROPERTIES))
                .setFeature("second", ThingsModelFactory.newFeatureProperties(FEATURE_PROPERTIES))
                .build();
        final Policy policy2 = newPolicy(PolicyId.of(thingId2), user1);
        dittoClient.twin().create(thing2, policy2)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        thingId3 = ThingId.of(idGenerator().withRandomName());
        final Thing thing3 = ThingsModelFactory.newThingBuilder()
                .setId(thingId3)
                .setAttributes(ATTRIBUTES)
                .build();
        final Policy policy3 = newPolicy(PolicyId.of(thingId3), user1);
        dittoClient.twin().create(thing3, policy3).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @After
    public void tearDown() {
        if (dittoClient != null) {
            try {
                CompletableFuture.allOf(
                        dittoClient.twin().delete(thingId1).toCompletableFuture(),
                        dittoClient.twin().delete(thingId2).toCompletableFuture(),
                        dittoClient.twin().delete(thingId3).toCompletableFuture())
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (final Exception e) {
                LOGGER.error("Error during Test", e);
            } finally {
                shutdownClient(dittoClient);
            }
        }
    }

    @Test
    public void testRetrieveThings() throws Exception {
        dittoClient.twin().retrieve(thingId1, thingId2, thingId3)
                .thenAccept(this::checkList)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void testRetrieveThingsWithFields() throws Exception {
        final JsonFieldSelector fieldSelector =
                JsonFactory.newFieldSelector(FIELD_THING_ID, FIELD_ATTRIBUTE, FIELD_FEATURES);

        dittoClient.twin()
                .retrieve(fieldSelector, thingId1, thingId2, thingId3)
                .thenAccept(this::checkList)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void testRetrieveThing() throws Exception {
        dittoClient.twin().forId(thingId2).retrieve()
                .thenAccept(thing -> checkThing(thing, thingId2))
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void testRetrieveThingWithFields() throws Exception {
        dittoClient.twin().forId(thingId2)
                .retrieve(JsonFactory.newFieldSelector(FIELD_THING_ID, FIELD_ATTRIBUTE, FIELD_FEATURES))
                .thenAccept(thing -> checkThing(thing, thingId2))
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void testRetrieveThingAttributesOnly() throws Exception {
        final JsonFieldSelector fieldSelector =
                JsonFactory.newFieldSelector(FIELD_ATTRIBUTE, JsonParseOptions.newBuilder().build());

        dittoClient.twin().forId(thingId2).retrieve(fieldSelector)
                .thenAccept(thing -> {
                    softly.assertThat(thing.getEntityId()).as("thing has no entity ID").isEmpty();
                    softly.assertThat(thing.getAttributes())
                            .as("thing has expected attributes")
                            .hasValueSatisfying(attributes -> softly.assertThat(attributes.getValue(ATTR1))
                                    .as("value of %s is expected", ATTR1.getPointer())
                                    .hasValue("test1"));
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    @RunIf(DockerEnvironment.class)
    public void testRetrieveThingsForBlockedSolution() throws Exception {
        final AuthenticationProvider<WebSocket> authenticationProvider =
                AuthenticationProviders.clientCredentials(ClientCredentialsAuthenticationConfiguration.newBuilder()
                        .clientId(user2.getClientId())
                        .clientSecret(user2.getClientSecret())
                        .scopes(scopeStringToList(user2.getScope().orElseThrow()))
                        .tokenEndpoint(user2.getTokenEndpoint())
                        .proxyConfiguration(proxyConfiguration())
                        .build());
        final DittoClient dittoClientWithBlockedSolution =
                ClientFactory.newClient(dittoWsUrl(2),
                        JsonSchemaVersion.V_2,
                        authenticationProvider,
                        proxyConfiguration(), Collections.emptyList(), Collections.emptyList(), null);

        try {
            dittoClientWithBlockedSolution.twin().forId(thingId1).retrieve()
                    .thenAccept(thing -> {
                        LOGGER.info("Thing: {}", thing);
                        assertThat(thing.getEntityId()).hasValue(thingId1);
                    })
                    .toCompletableFuture()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            shutdownClient(dittoClientWithBlockedSolution);
        }
    }

    private void checkThing(final Thing thing, final ThingId thingId) {
        softly.assertThat(thing.getEntityId()).as("thing has expected entity ID").hasValue(thingId);
        softly.assertThat(thing.getAttributes())
                .as("thing has expected attributes")
                .hasValueSatisfying(attributes -> softly.assertThat(attributes.getValue(ATTR1))
                        .as("value of %s is expected", ATTR1.getPointer())
                        .hasValue("test1"));
        softly.assertThat(thing.getFeatures())
                .as("thing has expected features")
                .hasValueSatisfying(features -> softly.assertThat(features.getFeature("first"))
                        .as("feature first is expected")
                        .hasValueSatisfying(first -> softly.assertThat(first.getProperties())
                                .as("feature has expected properties")
                                .hasValue(FEATURE_PROPERTIES)));
    }

    private void checkList(final List<Thing> thingList) {
        final List<ThingId> thingIds = thingList.stream()
                .map(Thing::getEntityId)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        softly.assertThat(thingIds).as("contains all expected thing IDs").contains(thingId1, thingId2, thingId3);
        softly.assertThat(thingList.get(1)).satisfies(thing2 -> {
            softly.assertThat(thing2.getAttributes()).as("thing has expected attributes")
                    .hasValueSatisfying(attributes -> softly.assertThat(attributes.getValue("attr1"))
                            .as("attr1 has expected value")
                            .hasValue(JsonValue.of("test1")));
            softly.assertThat(thing2.getFeatures()).as("thing has expected features")
                    .hasValueSatisfying(features -> softly.assertThat(features.getFeature("first"))
                            .as("feature first is expected")
                            .hasValueSatisfying(first -> softly.assertThat(first.getProperties())
                                    .as("feature properties are expected")
                                    .hasValue(FEATURE_PROPERTIES)));
        });
        thingList.forEach(t -> LOGGER.warn(t.toString()));
    }

}
