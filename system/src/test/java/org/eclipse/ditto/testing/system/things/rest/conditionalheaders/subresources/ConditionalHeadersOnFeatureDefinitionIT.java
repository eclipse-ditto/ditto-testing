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
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PatchMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureDefinition;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;

public final class ConditionalHeadersOnFeatureDefinitionIT extends AbstractConditionalHeadersOnThingSubResourceITBase {

    private static final String FEATURE_ID = "featureId";
    private static final String FEATURE_KEY = "featureKey";
    private static final String FEATURE_DEF_1 = "ns:featureDef:1";
    private static final String FEATURE_DEF_2 = "ns:featureDef:2";
    private static final String FEATURE_DEF_3 = "ns:featureDef:3";

    public ConditionalHeadersOnFeatureDefinitionIT() {
        super(JsonSchemaVersion.V_2);
    }

    @Override
    protected boolean alwaysExists() {
        return false;
    }

    @Override
    protected PutMatcher createSubResourceMatcher(final CharSequence thingId) {
        putFeature(version.toInt(), thingId, FEATURE_ID, createFeatureJsonString()).fire();
        final String featureDefinitionJsonString = createFeatureDefinitionJsonString(FEATURE_DEF_1);
        return putDefinition(version.toInt(), thingId, FEATURE_ID, featureDefinitionJsonString);
    }

    @Override
    protected GetMatcher getSubResourceMatcher(final CharSequence thingId) {
        return getDefinition(version.toInt(), thingId, FEATURE_ID);
    }

    @Override
    protected PutMatcher overwriteSubResourceMatcher(final CharSequence thingId) {
        final String featureDefinitionJsonString = createFeatureDefinitionJsonString(FEATURE_DEF_2);
        return putDefinition(version.toInt(), thingId, FEATURE_ID, featureDefinitionJsonString);
    }

    @Override
    protected PatchMatcher patchSubResourceMatcher(final CharSequence thingId) {
        return patchThing(version.toInt(), ThingId.of(thingId),
                FEATURES_JSON_POINTER.append(JsonPointer.of(FEATURE_ID).append(DEFINITION_JSON_POINTER)),
                JsonArray.of(createFeatureDefinitionJsonString(FEATURE_DEF_3)));
    }

    @Override
    protected DeleteMatcher deleteSubResourceMatcher(final CharSequence thingId) {
        return deleteDefinition(version.toInt(), thingId, FEATURE_ID);
    }

    private String createFeatureDefinitionJsonString(final String featureDef) {
        final FeatureDefinition featureDefinition = ThingsModelFactory
                .newFeatureDefinitionBuilder(featureDef).build();

        return featureDefinition.toJsonString();
    }

    private String createFeatureJsonString() {
        final FeatureProperties featureProperties = ThingsModelFactory.newFeatureProperties(JsonObject.newBuilder()
                .set(FEATURE_KEY, 1337).build());
        final Feature feature =
                ThingsModelFactory.newFeature(FEATURE_ID, featureProperties);

        return feature.toJsonString();
    }
}
