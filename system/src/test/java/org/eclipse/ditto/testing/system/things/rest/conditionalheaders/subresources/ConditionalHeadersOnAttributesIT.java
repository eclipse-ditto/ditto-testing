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
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PatchMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.things.model.ThingId;

public class ConditionalHeadersOnAttributesIT extends AbstractConditionalHeadersOnThingSubResourceITBase {

    private static final String ATTRIBUTE_JSON_POINTER = "attrKey";
    private static final String ATTR_VAL_1 = "attrVal1";
    private static final int NEW_ATTR_VAL_1 = 123;
    private static final String ATTR_VAL_2 = "attrVal2";
    private static final int NEW_ATTR_VAL_2 = 567;

    public ConditionalHeadersOnAttributesIT() {
        super(JsonSchemaVersion.V_2);
    }

    @Override
    protected boolean alwaysExists() {
        return false;
    }

    @Override
    protected PutMatcher createSubResourceMatcher(final CharSequence thingId) {
        final String attributesJson = JsonObject.newBuilder()
                .set(ATTRIBUTE_JSON_POINTER, ATTR_VAL_1)
                .build()
                .toString();

        return putAttributes(version.toInt(), thingId, attributesJson);
    }

    @Override
    protected GetMatcher getSubResourceMatcher(final CharSequence thingId) {
        return getAttributes(version.toInt(), thingId);
    }

    @Override
    protected PutMatcher overwriteSubResourceMatcher(final CharSequence thingId) {
        final String attributesJson = JsonObject.newBuilder()
                .set(ATTRIBUTE_JSON_POINTER, ATTR_VAL_2)
                .build()
                .toString();
        return putAttributes(version.toInt(), thingId, attributesJson);
    }

    @Override
    protected PatchMatcher patchSubResourceMatcher(final CharSequence thingId) {
        return patchThing(version.toInt(), ThingId.of(thingId),
                JsonPointer.of(ATTRIBUTES_JSON_POINTER),
                JsonObject.newBuilder().set(ATTR_VAL_1, NEW_ATTR_VAL_1).set(ATTR_VAL_2, NEW_ATTR_VAL_2).build());
    }

    @Override
    protected DeleteMatcher deleteSubResourceMatcher(final CharSequence thingId) {
        return deleteAttributes(version.toInt(), thingId);
    }
}
