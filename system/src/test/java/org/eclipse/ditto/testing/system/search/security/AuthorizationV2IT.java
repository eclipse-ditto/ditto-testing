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
package org.eclipse.ditto.testing.system.search.security;

import static org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2;
import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.attribute;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.featureProperty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEmpty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.and;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.or;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.eclipse.ditto.base.model.json.FieldType;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyBuilder;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.ResourceKey;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.testing.common.SearchIntegrationTest;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests Authorization of the Search REST API with V2.
 */
public final class AuthorizationV2IT extends SearchIntegrationTest {

    private static final ConditionFactory AWAITILITY_SEARCH_CONFIG =
            Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(3, TimeUnit.SECONDS);

    private static final boolean VALUE1 = true;

    private AuthClient secondClientForDefaultSolution;

    @Before
    public void setUp() {
        final TestingContext testingContext =
                TestingContext.withGeneratedMockClient(serviceEnv.getTestingContext2().getSolution(), TEST_CONFIG);
        secondClientForDefaultSolution = testingContext.getOAuthClient();
    }
    
    @Test
    public void userCannotReadThingFromOtherUserWithoutAnyPermissions() {
        // prepare: create thing as USER_1
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thingWithDefaultPermissions = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .build();
        persistThingAndWaitTillAvailable(thingWithDefaultPermissions, V_2, serviceEnv.getDefaultTestingContext());

        // test: USER_2 has no permissions on the thing and thus is not able to see the thing
        final SearchFilter thingIdIsEqual = idFilter(thingId);
        searchThings(V_2)
                .filter(thingIdIsEqual).withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingBody(isEmpty())
                .fire();
    }
    
    @Test
    public void userCanReadThingFromOtherUserIfHeHasReadPermissionOnSearchedAttribute() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final String attributeKey = "attr" + UUID.randomUUID();

        // prepare
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonFactory.newPointer(attributeKey), JsonFactory.newValue(VALUE1))
                .build();
        final Policy policy = createPolicyForPartialReadOfAttributeAndFeatureProperty(policyId, attributeKey, null);
        persistThingWithPolicy(thing, policy);

        // test
        final SearchFilter filter = attribute(attributeKey).eq(VALUE1);
        searchThings(V_2)
                .filter(filter).withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();
    }
    
    @Test
    public void userCanReadThingFromOtherUserIfHeHasReadPermissionOnSearchedFeatureProperty() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final String featureProp = "prop" + UUID.randomUUID();

        // prepare
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setFeature(ThingsModelFactory.newFeature("f1").setProperty(featureProp, VALUE1))
                .build();
        final Policy policy = createPolicyForPartialReadOfAttributeAndFeatureProperty(policyId, null, featureProp);
        persistThingWithPolicy(thing, policy);

        // test
        final SearchFilter filter = featureProperty(featureProp).eq(VALUE1);
        searchThings(V_2)
                .filter(filter).withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();
    }
    
    @Test
    public void userCanReadThingFromOtherUserIfHeHasReadPermissionOnSearchedFeaturePropertyAndAttributeByAnd() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final String attributeKey = "attr" + UUID.randomUUID();
        final String featureProperty = "prop" + UUID.randomUUID();

        // prepare
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonFactory.newPointer(attributeKey), JsonFactory.newValue(VALUE1))
                .setFeature(ThingsModelFactory.newFeature("f1").setProperty(featureProperty, VALUE1))
                .build();
        final Policy policy =
                createPolicyForPartialReadOfAttributeAndFeatureProperty(policyId, attributeKey, featureProperty);
        persistThingWithPolicy(thing, policy);

        // test
        final SearchFilter filter = and(
                featureProperty(featureProperty).eq(VALUE1),
                attribute(attributeKey).eq(VALUE1)
        );

        searchThings(V_2)
                .filter(filter).withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();
    }
    
    @Test
    public void userCannotReadThingFromOtherUserIfHeHasReadPermissionOnSearchedFeaturePropertyOnlyButNotOnAttribute() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final String attributeKey = "attr" + UUID.randomUUID();
        final String featureProp = "prop" + UUID.randomUUID();

        // prepare
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonFactory.newPointer(attributeKey), JsonFactory.newValue(VALUE1))
                .setFeature(ThingsModelFactory.newFeature("f1").setProperty(featureProp, VALUE1))
                .build();
        final Policy policy = createPolicyForPartialReadOfAttributeAndFeatureProperty(policyId, null, featureProp);
        persistThingWithPolicy(thing, policy);

        // test
        final SearchFilter filter = and(
                featureProperty(featureProp).eq(VALUE1),
                attribute(attributeKey).eq(VALUE1)
        );

        searchThings(V_2)
                .filter(filter).withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingBody(isEmpty())
                .fire();
    }
    
    @Test
    public void userCanReadThingFromOtherUserIfHeHasReadPermissionOnSearchedFeaturePropertyAndNotAttributeByOr() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final String featureProp = "prop" + UUID.randomUUID();
        final String attributeKey = "attr" + UUID.randomUUID();

        // prepare
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonFactory.newPointer(attributeKey), JsonFactory.newValue(VALUE1))
                .setFeature(ThingsModelFactory.newFeature("f1").setProperty(featureProp, VALUE1))
                .build();
        final Policy policy =
                createPolicyForPartialReadOfAttributeAndFeatureProperty(policyId, null, featureProp);
        persistThingWithPolicy(thing, policy);

        // test
        final SearchFilter filter = or(
                featureProperty(featureProp).eq(VALUE1),
                attribute(attributeKey).eq(VALUE1)
        );

        searchThings(V_2)
                .filter(filter)
                .withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();
    }
    
    @Test
    public void userCanReadThingFromOtherUserAfterPolicyUpdate() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final String featureProp = "prop" + UUID.randomUUID();
        final String attributeKey = "attr" + UUID.randomUUID();

        // prepare
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonFactory.newPointer(attributeKey), JsonFactory.newValue(VALUE1))
                .setFeature(ThingsModelFactory.newFeature("f1").setProperty(featureProp, VALUE1))
                .build();
        final Policy policy =
                createPolicyForPartialReadOfAttributeAndFeatureProperty(policyId, null, featureProp);
        persistThingWithPolicy(thing, policy);

        putPolicy(thingId, createPolicyForPartialReadOfAttributeAndFeatureProperty(policyId, attributeKey, featureProp))
                .fire();

        // test
        final SearchFilter filter = and(
                featureProperty(featureProp).eq(VALUE1),
                attribute(attributeKey).eq(VALUE1)
        );

        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(filter).withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();
    }
    
    @Test
    public void userCannotReadThingFromOtherUserAfterPolicyUpdate() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final String featureProperty = "prop" + UUID.randomUUID();
        final String attributeKey = "attr" + UUID.randomUUID();

        // prepare
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonFactory.newPointer(attributeKey), JsonFactory.newValue(VALUE1))
                .setFeature(ThingsModelFactory.newFeature("f1").setProperty(featureProperty, VALUE1))
                .build();
        final Policy policy =
                createPolicyForPartialReadOfAttributeAndFeatureProperty(policyId, attributeKey, featureProperty);
        persistThingWithPolicy(thing, policy);

        putPolicy(thingId,
                createPolicyForPartialReadOfAttributeAndFeatureProperty(policyId, attributeKey, null)).fire();

        // test
        final SearchFilter filter = and(
                featureProperty(featureProperty).eq(VALUE1),
                attribute(attributeKey).eq(VALUE1)
        );

        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(filter).withJWT(secondClientForDefaultSolution.getAccessToken())
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void userCannotSearchOnRevokedFields() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final String grantedFeatureProperty = "granted-prop";
        final String revokedFeatureProperty = "revoked-prop";
        final String grantedAttributeKey = "granted-attr";
        final String revokedAttributeKey = "revoked-attr";

        final Subject subject = serviceEnv.getDefaultTestingContext().getOAuthClient().getSubject();
        final Policy policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("all-granted")
                .setSubject(subject)
                .setGrantedPermissions(ResourceKey.newInstance(PoliciesResourceType.THING, "/"), READ, WRITE)
                .setGrantedPermissions(ResourceKey.newInstance(PoliciesResourceType.POLICY, "/"), READ, WRITE)
                .forLabel("some-read-revoked")
                .setSubject(subject)
                .setRevokedPermissions(PoliciesResourceType.thingResource("/attributes/" + revokedAttributeKey),
                        READ)
                .setRevokedPermissions(
                        PoliciesResourceType.thingResource("/features/f1/properties/" + revokedFeatureProperty),
                        READ)
                .build();

        // prepare
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonFactory.newPointer(grantedAttributeKey), JsonFactory.newValue(VALUE1))
                .setAttribute(JsonFactory.newPointer(revokedAttributeKey), JsonFactory.newValue(VALUE1))
                .setFeature(ThingsModelFactory.newFeature("f1")
                        .setProperty(grantedFeatureProperty, VALUE1)
                        .setProperty(revokedFeatureProperty, VALUE1))
                .build();
        persistThingWithPolicy(thing, policy);

        // ensure that a search on granted fields returns the thingId:
        final SearchFilter searchGrantedFilter = and(
                featureProperty(grantedFeatureProperty).eq(VALUE1),
                attribute(grantedAttributeKey).eq(VALUE1)
        );
        searchThings(V_2)
                .filter(searchGrantedFilter)
                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        // ensure that a search with AND on 2 revoked fields does not return the thingId:
        final SearchFilter searchRevokedFilter1 = and(
                featureProperty(revokedFeatureProperty).eq(VALUE1),
                attribute(revokedAttributeKey).eq(VALUE1)
        );
        searchThings(V_2)
                .filter(searchRevokedFilter1)
                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken())
                .expectingBody(isEmpty())
                .fire();

        // ensure that a search with AND on 1 granted + 1 revoked fields does not return the thingId:
        final SearchFilter searchRevokedFilter2 = and(
                featureProperty(grantedFeatureProperty).eq(VALUE1),
                attribute(revokedAttributeKey).eq(VALUE1)
        );
        searchThings(V_2)
                .filter(searchRevokedFilter2)
                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken())
                .expectingBody(isEmpty())
                .fire();

        // ensure that a search with OR on 1 granted + 1 revoked fields does return the thingId:
        final SearchFilter searchRevokedFilter3 = or(
                featureProperty(revokedFeatureProperty).eq(VALUE1),
                attribute(grantedAttributeKey).eq(VALUE1)
        );
        searchThings(V_2)
                .filter(searchRevokedFilter3)
                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();
    }

    private Policy createPolicyForPartialReadOfAttributeAndFeatureProperty(final PolicyId id,
            final String attributeKey, final String featureProperty) {

        PolicyBuilder.LabelScoped builder = PoliciesModelFactory.newPolicyBuilder(id)
                .forLabel("l1")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(ResourceKey.newInstance(PoliciesResourceType.THING, "/"), READ,
                        WRITE)
                .setGrantedPermissions(ResourceKey.newInstance(PoliciesResourceType.POLICY, "/"), READ,
                        WRITE)
                .forLabel("l2")
                .setSubject(secondClientForDefaultSolution.getDefaultSubject());
        if (attributeKey != null) {
            builder = builder.setGrantedPermissions(
                    ResourceKey.newInstance(PoliciesResourceType.THING, "/attributes/" + attributeKey), READ);
        }
        if (featureProperty != null) {
            builder.setGrantedPermissions(ResourceKey.newInstance(PoliciesResourceType.THING,
                    JsonFactory.newPointer("/features/f1/properties/" + featureProperty)), READ);
        }

        return builder.build();
    }

    private static void persistThingWithPolicy(final Thing thing, final Policy policy) {
        final JsonObject thingJson = thing.toJson(V_2, FieldType.notHidden())
                .setAll(policy.toInlinedJson(V_2, FieldType.notHidden()));
        persistThingsAndWaitTillAvailable(thingJson.toString(), 1L, V_2, serviceEnv.getDefaultTestingContext());
    }
}
