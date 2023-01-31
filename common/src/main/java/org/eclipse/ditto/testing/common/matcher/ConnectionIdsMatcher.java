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
package org.eclipse.ditto.testing.common.matcher;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public final class ConnectionIdsMatcher extends TypeSafeMatcher<String> {

    private final List<String> expectedConnectionIds;
    private final List<String> notExpectedConnectionIds;

    /**
     * Constructs a new {@code ConnectionIdsMatcher} object.
     *
     * @param expectedConnectionIds the list of the expected Connection IDs.
     * @param notExpectedConnectionIds the list of the not expected Connection IDs.
     * @throws NullPointerException if {@code expectedConnectionIds} is {@code null}.
     * @throws NullPointerException if {@code notExpectedConnectionIds} is {@code null}.
     */
    public ConnectionIdsMatcher(final List<String> expectedConnectionIds,
            final List<String> notExpectedConnectionIds) {
        this.expectedConnectionIds = requireNonNull(expectedConnectionIds);
        this.notExpectedConnectionIds = requireNonNull(notExpectedConnectionIds);
    }

    @Override
    protected boolean matchesSafely(final String solutionResponse) {
        final boolean result;
        if (solutionResponse == null || solutionResponse.isEmpty()) {
            result = false;
        } else {
            final JsonArray connectionsArray = JsonFactory.newArray(solutionResponse);
            final List<String> connectionIds = new ArrayList<>(connectionsArray.getSize());

            connectionsArray.forEach(connection ->
                connectionIds.add(connection.asObject().getValueOrThrow(Connection.JsonFields.ID))
            );

            result = calculateResult(connectionIds);
        }
        return result;
    }

    private boolean calculateResult(final List<String> connectionIds){
        int expectedConnectionsCount = 0;
        int notExpectedConnectionsCount = 0;

        for (int i = 0; i < connectionIds.size(); i++) {
            final String connectionId = connectionIds.get(i);
            if (expectedConnectionIds.contains(connectionId)) {
                expectedConnectionsCount++;
            }
            if (notExpectedConnectionIds.contains(connectionId)) {
                notExpectedConnectionsCount++;
            }
        }

        return !expectedConnectionIds.isEmpty() && expectedConnectionsCount == expectedConnectionIds.size()
                || !notExpectedConnectionIds.isEmpty() && notExpectedConnectionsCount == 0;
    }

    @Override
    public void describeTo(final Description description) {
        if (!expectedConnectionIds.isEmpty()) {
            description.appendText("The Connection ID(s) should be").appendValue(expectedConnectionIds);
        } else {
            description.appendText("The Connection ID(s) should not be").appendValue(notExpectedConnectionIds);
        }
    }
}
