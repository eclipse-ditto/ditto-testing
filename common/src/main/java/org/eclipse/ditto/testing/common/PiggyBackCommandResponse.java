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

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;

import io.restassured.response.Response;
import io.restassured.response.ResponseBody;

/**
 * A factory for parsing the response to an {@link org.eclipse.ditto.base.api.devops.signals.commands.ExecutePiggybackCommand}.
 */
@Immutable
public final class PiggyBackCommandResponse {

    private PiggyBackCommandResponse() {
        throw new AssertionError();
    }

    public static JsonObject getForString(final String jsonString) {
        if (jsonString == null || jsonString.equals("null")) {
            return JsonObject.empty();
        } else {
            return JsonFactory.newObject(jsonString);
        }
    }

    public static JsonObject getForHttpResponse(final Response httpResponse) {
        final ResponseBody<?> body = httpResponse.getBody();
        return getForString(body.asString());
    }

}
