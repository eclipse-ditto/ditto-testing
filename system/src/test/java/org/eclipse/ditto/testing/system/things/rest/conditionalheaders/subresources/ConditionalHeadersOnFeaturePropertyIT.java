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
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PatchMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureDefinition;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;

public final class ConditionalHeadersOnFeaturePropertyIT extends AbstractConditionalHeadersOnThingSubResourceITBase {

    private static final String FEATURE_ID = "featureId";
    private static final String FEATURE_DEF = "ns:featureDef:1";
    private static final String FEATURE_KEY = "featureKey";
    private static final String FEATURE_VAL_1 = JsonValue.of("featureVal1").toString();
    private static final String FEATURE_VAL_2 = JsonValue.of("featureVal2").toString();

    public ConditionalHeadersOnFeaturePropertyIT() {
        super(JsonSchemaVersion.V_2);
    }

    @Override
    protected boolean alwaysExists() {
        return false;
    }

    @Override
    protected PutMatcher createSubResourceMatcher(final CharSequence thingId) {
        putFeature(version.toInt(), thingId, FEATURE_ID, createFeatureJsonString()).fire();

        return putProperty(version.toInt(), thingId, FEATURE_ID, FEATURE_KEY, FEATURE_VAL_1);
    }

    @Override
    protected GetMatcher getSubResourceMatcher(final CharSequence thingId) {
        return getProperty(version.toInt(), thingId, FEATURE_ID, FEATURE_KEY);
    }

    @Override
    protected PutMatcher overwriteSubResourceMatcher(final CharSequence thingId) {
        return putProperty(version.toInt(), thingId, FEATURE_ID, FEATURE_KEY, FEATURE_VAL_2);
    }

    @Override
    protected PatchMatcher patchSubResourceMatcher(final CharSequence thingId) {
        return patchThing(version.toInt(), ThingId.of(thingId),
                FEATURES_JSON_POINTER.append(JsonPointer.of(FEATURE_ID).append(PROPERTIES_JSON_POINTER).append(JsonPointer.of(FEATURE_KEY))),
                JsonValue.of("newFeatureValue"));
    }

    @Override
    protected DeleteMatcher deleteSubResourceMatcher(final CharSequence thingId) {
        return deleteProperty(version.toInt(), thingId, FEATURE_ID, FEATURE_KEY);
    }

    private String createFeatureJsonString() {
        final FeatureDefinition featureDefinition = ThingsModelFactory
                .newFeatureDefinitionBuilder(FEATURE_DEF).build();
        final Feature feature =
                ThingsModelFactory.newFeature(FEATURE_ID, featureDefinition);

        return feature.toJsonString();
    }

}
