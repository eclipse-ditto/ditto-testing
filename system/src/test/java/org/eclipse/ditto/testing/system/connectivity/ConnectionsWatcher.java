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
package org.eclipse.ditto.testing.system.connectivity;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Simple TestWatcher that loads the required connections for a test from the {@link Connections} annotation and
 * makes them available via {@link #getConnections()}.
 */
public class ConnectionsWatcher extends TestWatcher {

    private Map<ConnectionCategory, String> enabledConnections;

    @Override
    protected void starting(final Description description) {
        final Connections connections = description.getAnnotation(Connections.class);
        final List<UseConnection> connectionList =
                Optional.ofNullable(description.getAnnotation(UseConnection.List.class))
                        .map(UseConnection.List::value)
                        .map(List::of)
                        .or(() -> Optional.ofNullable(description.getAnnotation(UseConnection.class))
                                .map(Collections::singletonList)).orElse(Collections.emptyList());

        if (connections != null && !connectionList.isEmpty()) {
            throw new IllegalStateException("Use either @Connections or @Connection annotation.");
        }

        if (connections != null) {
            enabledConnections = List.of(connections.value()).stream().collect(Collectors.toMap(cc -> cc, cc -> ""));
        } else if (!connectionList.isEmpty()) {
            enabledConnections = getConnectionWithMods(connectionList);
        } else {
            enabledConnections = Collections.emptyMap();
        }
    }

    private Map<ConnectionCategory, String> getConnectionWithMods(final List<UseConnection> connectionList) {
        return connectionList.stream()
                .filter(mod -> {
                    if (AbstractConnectivityITBase.MODS.containsKey(mod.mod())) {
                        return true;
                    } else {
                        throw new IllegalArgumentException("The modification with id '" + mod.mod() + "' is not " +
                                "registered.");
                    }
                })
                .collect(Collectors.toMap(UseConnection::category, UseConnection::mod));
    }

    public Map<ConnectionCategory, String> getConnections() { return enabledConnections;}
}
