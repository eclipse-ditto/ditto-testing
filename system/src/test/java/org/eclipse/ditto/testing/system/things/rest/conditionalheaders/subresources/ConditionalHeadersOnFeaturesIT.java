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

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PatchMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureDefinition;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Features;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;

public class ConditionalHeadersOnFeaturesIT extends AbstractConditionalHeadersOnThingSubResourceITBase {

    private static final String FEATURE_ID = "featureId";
    private static final String FEATURE_DEF = "ns:featureDef:1";
    private static final String FEATURE_KEY = "featureKey";
    private static final String FEATURE_VAL_1 = "featureVal1";
    private static final String FEATURE_VAL_2 = "featureVal2";
    private static final String FEATURE_VAL_3 = "featureVal3";

    public ConditionalHeadersOnFeaturesIT() {
        super(JsonSchemaVersion.V_2);
    }

    @Override
    protected boolean alwaysExists() {
        return false;
    }

    @Override
    protected PutMatcher createSubResourceMatcher(final CharSequence thingId) {
        final String featureJsonString = createFeaturesJsonString(FEATURE_VAL_1);

        return putFeatures(version.toInt(), thingId, featureJsonString);
    }

    @Override
    protected GetMatcher getSubResourceMatcher(final CharSequence thingId) {
        return getFeatures(version.toInt(), thingId);
    }

    @Override
    protected PutMatcher overwriteSubResourceMatcher(final CharSequence thingId) {
        final String featureJsonString = createFeaturesJsonString(FEATURE_VAL_2);

        return putFeatures(version.toInt(), thingId, featureJsonString);
    }

    @Override
    protected PatchMatcher patchSubResourceMatcher(final CharSequence thingId) {
        return patchThing(version.toInt(), ThingId.of(thingId), FEATURES_JSON_POINTER,
                createFeatures(FEATURE_VAL_3).toJson());
    }

    @Override
    protected DeleteMatcher deleteSubResourceMatcher(final CharSequence thingId) {
        return deleteFeatures(version.toInt(), thingId);
    }

    private Features createFeatures(final String featureVal) {
        final FeatureDefinition featureDefinition = ThingsModelFactory
                .newFeatureDefinitionBuilder(FEATURE_DEF).build();
        final FeatureProperties featureProperties = ThingsModelFactory.newFeatureProperties(JsonObject.newBuilder()
                .set(FEATURE_KEY, featureVal).build());
        final Feature feature =
                ThingsModelFactory.newFeature(FEATURE_ID, featureDefinition, featureProperties);

        final Features features = ThingsModelFactory.newFeatures(feature);
        return features;
    }

    private String createFeaturesJsonString(final String featureVal) {
        return createFeatures(featureVal).toJsonString();
    }
}
