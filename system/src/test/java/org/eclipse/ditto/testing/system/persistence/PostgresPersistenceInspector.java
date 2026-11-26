/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
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
package org.eclipse.ditto.testing.system.persistence;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * {@link PersistenceInspector} for the PostgreSQL persistence backend. Counts journal and snapshot rows in the
 * {@code public} schema tables via plain (blocking) JDBC — which is fine for a test.
 * <p>
 * Snapshot counts use {@code count(DISTINCT sn)} instead of {@code count(*)}: the PostgreSQL snaps primary key is
 * {@code (pid, sn, written_at)}, so several rows per {@code (pid, sn)} are representable where MongoDB upserts a
 * single document; {@code DISTINCT sn} yields Mongo-equivalent semantics regardless.
 */
public final class PostgresPersistenceInspector implements PersistenceInspector {

    private static final String THING_PREFIX = "thing:";
    private static final String POLICY_PREFIX = "policy:";
    private static final String CONNECTION_PREFIX = "connection:";

    /**
     * Mirrors the Mongo {@code events.p.type} regex of {@link MongoPersistenceInspector}: the {@code event} JSONB
     * column holds the full Ditto event JSON with a top-level {@code type}; the filter excludes e.g. no-op
     * {@code EmptyEvent} entries. Mongo's regex is an unanchored substring, {@code LIKE 'x%'} is anchored —
     * equivalent for these values.
     */
    private static final String CONNECTIVITY_EVENTS_FILTER = " AND event->>'type' LIKE 'connectivity.events:%'";

    private final Connection connection;

    /**
     * Constructs a new instance connecting to the given PostgreSQL JDBC URI.
     *
     * @param jdbcUri the JDBC URI, e.g. {@code jdbc:postgresql://postgres:5432/ditto}.
     * @param user the database user.
     * @param password the database password.
     */
    public PostgresPersistenceInspector(final String jdbcUri, final String user, final String password) {
        try {
            connection = DriverManager.getConnection(jdbcUri, user, password);
        } catch (final SQLException e) {
            throw new IllegalStateException("Failed to connect to PostgreSQL at <" + jdbcUri + ">.", e);
        }
    }

    @Override
    public long countThingEvents(final List<String> thingIds) {
        return count("SELECT count(*) FROM things_journal WHERE pid = ANY(?)", THING_PREFIX, thingIds);
    }

    @Override
    public long countThingSnaps(final List<String> thingIds) {
        return count("SELECT count(DISTINCT sn) FROM things_snaps WHERE pid = ANY(?)", THING_PREFIX, thingIds);
    }

    @Override
    public long countPolicyEvents(final List<String> policyIds) {
        return count("SELECT count(*) FROM policies_journal WHERE pid = ANY(?)", POLICY_PREFIX, policyIds);
    }

    @Override
    public long countPolicySnaps(final List<String> policyIds) {
        return count("SELECT count(DISTINCT sn) FROM policies_snaps WHERE pid = ANY(?)", POLICY_PREFIX, policyIds);
    }

    @Override
    public long countConnectionEvents(final List<String> connIds, final boolean onlyConnectivityEvents) {
        String sql = "SELECT count(*) FROM connections_journal WHERE pid = ANY(?)";
        if (onlyConnectivityEvents) {
            sql += CONNECTIVITY_EVENTS_FILTER;
        }
        return count(sql, CONNECTION_PREFIX, connIds);
    }

    @Override
    public long countConnectionSnaps(final List<String> connIds) {
        return count("SELECT count(DISTINCT sn) FROM connections_snaps WHERE pid = ANY(?)", CONNECTION_PREFIX,
                connIds);
    }

    private long count(final String sql, final String prefix, final List<String> ids) {
        final Object[] pids = ids.stream()
                .map(id -> prefix + id)
                .toArray();
        try (final PreparedStatement statement = connection.prepareStatement(sql)) {
            final Array pidArray = connection.createArrayOf("text", pids);
            try {
                statement.setArray(1, pidArray);
                try (final ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        throw new IllegalStateException("Count query returned no row: <" + sql + ">.");
                    }
                    return resultSet.getLong(1);
                }
            } finally {
                pidArray.free();
            }
        } catch (final SQLException e) {
            throw new IllegalStateException("Failed to execute count query <" + sql + ">.", e);
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (final SQLException e) {
            throw new IllegalStateException("Failed to close the PostgreSQL connection.", e);
        }
    }

}
