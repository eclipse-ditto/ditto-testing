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
package org.eclipse.ditto.testing.system.things.rest.live;

import java.util.stream.Stream;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.LogEntry;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;

/**
 * Converts a {@link org.eclipse.ditto.json.JsonObject} to a stream of {@code LogEntry}s.
 */
final class LogEntriesFromJsonConverter {

    private LogEntriesFromJsonConverter() {
        throw new AssertionError();
    }

    static Stream<LogEntry> getLogEntries(final JsonObject connectionLogsJsonObject) {
        ConditionChecker.checkNotNull(connectionLogsJsonObject, "connectionLogsJsonObject");
        return connectionLogsJsonObject.getValue("connectionLogs")
                .filter(JsonValue::isArray)
                .map(JsonValue::asArray)
                .stream()
                .flatMap(JsonArray::stream)
                .filter(JsonValue::isObject)
                .map(JsonValue::asObject)
                .map(ConnectivityModelFactory::logEntryFromJson);
    }

}
