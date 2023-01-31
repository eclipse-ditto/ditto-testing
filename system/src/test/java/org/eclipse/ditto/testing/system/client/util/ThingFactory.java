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
package org.eclipse.ditto.testing.system.client.util;

import static java.util.Objects.requireNonNull;

import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.Features;
import org.eclipse.ditto.things.model.FeaturesBuilder;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;

/**
 * A factory for creating {@link org.eclipse.ditto.things.model.Thing}s.
 */
public class ThingFactory {

    private ThingFactory() {
        // no-op
    }

    /**
     * Returns a new {@code Thing} for the specified {@code thingId}.
     *
     * @param thingId the identifier of the Thing.
     * @return the Thing.
     * @throws NullPointerException if {@code thingId} is {@code null}.
     */
    public static Thing newThing(final ThingId thingId) {
        requireNonNull(thingId);

        return Thing.newBuilder()
                .setId(thingId)
                .setAttributes(ThingsModelFactory.newAttributesBuilder().set("hello", "cloud").build())
                .build();
    }

    /**
     * Returns new {@code Features} for the specified {@code featureIds}.
     *
     * @param featureIds the identifiers of the Features.
     * @return the Features.
     */
    public static Features newFeatures(final String... featureIds) {
        final FeaturesBuilder featuresBuilder = Features.newBuilder();

        for (final String featureId : featureIds) {
            featuresBuilder.set(Feature.newBuilder().properties(JsonFactory.newObject()).withId(featureId).build());
        }

        return featuresBuilder.build();
    }

}
