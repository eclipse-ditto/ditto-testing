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

import static org.assertj.core.api.Assertions.assertThat;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.UriEncoding;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.Test;

import io.restassured.response.Response;

/**
 * Integration test for URL Encoding issues.
 */
public final class GatewayUrlEncodingIT extends IntegrationTest {

    @Test
    public void createThingWithPercentInFeatureIdAndQueryIt() {
        final String unEncodedFeatureId = "feature%";
        doCreateThingWithFeatureAndQueryIt(unEncodedFeatureId);
    }

    @Test
    public void createThingWithManySpecialCharsInFeatureIdAndQueryIt() {
        final String unEncodedFeatureId = "!\\\"#$%&'()*+,:;=?@[]{|} aZ0";
        doCreateThingWithFeatureAndQueryIt(unEncodedFeatureId);
    }

    private static void doCreateThingWithFeatureAndQueryIt(final String unEncodedFeatureId) {
        final JsonObject jsonObject = ThingsModelFactory.newThingBuilder()
                .setFeature(unEncodedFeatureId)
                .build().toJson(JsonSchemaVersion.V_2);
        final Response postResponse = postThing(TestConstants.API_V_2, jsonObject)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final Thing thingFromPostBody = ThingsModelFactory.newThing(postResponse.getBody().asString());
        final ThingId thingId = thingFromPostBody.getEntityId().orElse(null);

        final String encodedFeatureId = UriEncoding.encodePathSegment(unEncodedFeatureId);
        final Response getResponse = getFeature(TestConstants.API_V_2, thingId, encodedFeatureId)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
        final JsonObject responseJson = JsonFactory.newObject(getResponse.getBody().asString());

        assertThat(responseJson).isEmpty();
    }

}
