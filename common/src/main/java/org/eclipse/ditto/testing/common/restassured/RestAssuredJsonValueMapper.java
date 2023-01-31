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
package org.eclipse.ditto.testing.common.restassured;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonValue;

import io.restassured.common.mapper.DataToDeserialize;
import io.restassured.mapper.ObjectMapper;
import io.restassured.mapper.ObjectMapperDeserializationContext;
import io.restassured.mapper.ObjectMapperSerializationContext;

/**
 * Deserializes a {@link JsonValue} from an InputStream.
 * If deserialization fails a {@link org.eclipse.ditto.json.JsonParseException} is thrown.
 * <p>
 * <em>Note: this mapper only supports deserialization.</em>
 * </p>
 */
public final class RestAssuredJsonValueMapper implements ObjectMapper {

    private RestAssuredJsonValueMapper() {
        super();
    }

    /**
     * Returns an instance of {@code RestAssuredJsonValueMapper}.
     *
     * @return the instance.
     */
    public static RestAssuredJsonValueMapper getInstance() {
        return new RestAssuredJsonValueMapper();
    }

    @Override
    public JsonValue deserialize(final ObjectMapperDeserializationContext context) {
        ConditionChecker.checkNotNull(context, "context");
        return tryToDeserializeData(context.getDataToDeserialize());
    }

    private static JsonValue tryToDeserializeData(final DataToDeserialize dataToDeserialize) {
        try {
            return deserializeInputStream(dataToDeserialize.asInputStream());
        } catch (final IOException e) {

            // This should not happen in reality!?
            throw new IllegalStateException("Failed to close InputStreamReader!", e);
        }
    }

    private static JsonValue deserializeInputStream(final InputStream inputStream) throws IOException {
        try (final var inputStreamReader = new InputStreamReader(inputStream)) {
            return JsonFactory.readFrom(inputStreamReader);
        }
    }

    @Override
    public Object serialize(final ObjectMapperSerializationContext context) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " only supports deserialization!");
    }

}
