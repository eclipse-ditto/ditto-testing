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
package org.eclipse.ditto.testing.common;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;

import javax.annotation.Nullable;

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonObjectBuilder;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.Resources;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingsModelFactory;

/**
 * Simple class to produce thing JSON strings from our reference JSON.
 */
public final class ThingJsonProducer {

    private final Thing dummyThing;
    private final String dummyThingJson;

    /**
     * Constructs a {@code ThingsJsonProducer} which can produce JSON strings from CRaaSCar.json.
     */
    public ThingJsonProducer() {
        dummyThing = Thing.newBuilder()
                .setAttributes(Attributes.newBuilder()
                        .set("manufacturer", "ACME")
                        .set("make", "Fancy Fab Car")
                        .set("model", "Environmental FourWheeler 4711")
                        .set("VIN", "0815666337")
                        .build())
                .setFeature(ThingsModelFactory.newFeature("Vehicle", FeatureProperties.newBuilder()
                        .set("configuration", JsonObject.newBuilder()
                                .set("transmission", JsonObject.newBuilder()
                                        .set("type", "manual")
                                        .set("gears", 7)
                                        .build())
                                .build())
                        .set("status", JsonObject.newBuilder()
                                .set("running", true)
                                .set("speed", 90)
                                .set("gear", 5)
                                .build())
                        .set("fault", JsonObject.newBuilder()
                                .set("flatTyre", false)
                                .build())
                        .build()))
                .setFeature(ThingsModelFactory.newFeature("EnvironmentScanner", FeatureProperties.newBuilder()
                        .set("temperature", 20.8)
                        .set("humidity", 73)
                        .set("barometricPressure", 970.7)
                        .set("location", JsonObject.newBuilder()
                                .set("longitude", 47.682170)
                                .set("latitude", 9.386372)
                                .build())
                        .set("altitude", 399)
                        .build()))
                .build();
        dummyThingJson = dummyThing.toJsonString();
    }

    public Thing getThing() {
        return dummyThing;
    }

    public String getJsonString() {
        return dummyThingJson;
    }

    /**
     * Creates a JSON object for the given Thing ID and the given Authorization Subject ID with the default policy.
     *
     * @return a JSON object for the given Thing ID.
     */
    public JsonObject getJsonForV2() {
        return getJsonForV2WithOptionalPolicy(null, null);
    }

    /**
     * Creates a JSON object for the given Thing ID and the given Authorization Subject ID.
     *
     * @param subject the ID of the Authorization Subject which gets all minimum required permissions on the
     * Thing.
     * @param policyNamespace the namespace of the policy to be created.
     * @return a JSON object for the given Thing ID with the supplied owner.
     */
    public JsonObject getJsonForV2WithInlinePolicy(final Subject subject, final String policyNamespace) {
        requireNonNull(subject);
        requireNonNull(policyNamespace);

        return getJsonForV2WithOptionalPolicy(subject, policyNamespace);
    }

    /**
     * Creates a JSON object for the given Thing ID and the given Authorization Subject ID.
     *
     * @param subject the ID of the Authorization Subject which gets all minimum required permissions on the
     * Thing. May be {@code null}: In this case no policy permissions are set.
     * @param policyNamespace the namespace of the policy to be created. May be {@code null}, if {@code
     * authSubjectId} is not specified.
     * @return a JSON object for the given Thing ID with the supplied owner.
     */
    private JsonObject getJsonForV2WithOptionalPolicy(@Nullable final Subject subject,
            @Nullable final String policyNamespace) {

        final ThingBuilder.FromCopy thingBuilder = dummyThing.toBuilder();

        final Policy policy;
        if (subject != null) {
            requireNonNull(policyNamespace);
            final String randomPolicyId = IdGenerator.fromNamespace(policyNamespace).withRandomName();
            final PolicyId policyId = PolicyId.of(randomPolicyId);
            policy = Policy.newBuilder(policyId)
                    .set(PolicyEntry.newInstance("DEFAULT",
                            Subjects.newInstance(subject),
                            Resources.newInstance(
                                    Resource.newInstance(PoliciesResourceType.policyResource("/"),
                                            EffectedPermissions.newInstance(Arrays.asList("READ", "WRITE"), null)),
                                    Resource.newInstance(PoliciesResourceType.thingResource("/"),
                                            EffectedPermissions.newInstance(Arrays.asList("READ", "WRITE"), null)))))
                    .build();
        } else {
            policy = null;
        }

        // need to select both REGULAR and SPECIAL as "policy" is a SPECIAL field:
        final Thing thing = thingBuilder.build();
        final JsonSchemaVersion schemaVersion = JsonSchemaVersion.V_2;
        final JsonObjectBuilder jsonObjectBuilder = thing.toJson(schemaVersion).toBuilder();
        if (policy != null) {
            final JsonFieldSelector fieldSelector =
                    JsonFactory.newFieldSelector(Policy.JsonFields.ID, Policy.JsonFields.ENTRIES);
            jsonObjectBuilder.setAll(policy.toInlinedJson(schemaVersion, fieldSelector));
            jsonObjectBuilder.set(Thing.JsonFields.POLICY_ID,
                    policy.getEntityId().orElseThrow(IllegalStateException::new).toString());
        }
        return jsonObjectBuilder.build();
    }
}
