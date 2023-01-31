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
import org.eclipse.ditto.things.model.ThingId;

public class ConditionalHeadersOnAttributeIT extends AbstractConditionalHeadersOnThingSubResourceITBase {

    private static final String ATTRIBUTE_JSON_POINTER = "attrKey";
    private static final String ATTR_VAL_1 = "attrVal1";
    private static final String ATTR_VAL_2 = "attrVal2";
    private static final String ATTR_VAL_3 = "attrVal3";


    public ConditionalHeadersOnAttributeIT() {
        super(JsonSchemaVersion.V_2);
    }

    @Override
    protected boolean alwaysExists() {
        return false;
    }

    @Override
    protected PutMatcher createSubResourceMatcher(final CharSequence thingId) {
        return putAttribute(version.toInt(), thingId, ATTRIBUTE_JSON_POINTER, JsonValue.of(ATTR_VAL_1).toString());
    }

    @Override
    protected GetMatcher getSubResourceMatcher(final CharSequence thingId) {
        return getAttribute(version.toInt(), thingId, ATTRIBUTE_JSON_POINTER);
    }

    @Override
    protected PutMatcher overwriteSubResourceMatcher(final CharSequence thingId) {
        return putAttribute(version.toInt(), thingId, ATTRIBUTE_JSON_POINTER, JsonValue.of(ATTR_VAL_2).toString());
    }

    @Override
    protected PatchMatcher patchSubResourceMatcher(final CharSequence thingId) {
        return patchThing(version.toInt(), ThingId.of(thingId),
                JsonPointer.of(ATTRIBUTES_JSON_POINTER).append(JsonPointer.of(ATTRIBUTE_JSON_POINTER)),
                JsonValue.of(ATTR_VAL_3));
    }

    @Override
    protected DeleteMatcher deleteSubResourceMatcher(final CharSequence thingId) {
        return deleteAttribute(version.toInt(), thingId, ATTRIBUTE_JSON_POINTER);
    }
}
