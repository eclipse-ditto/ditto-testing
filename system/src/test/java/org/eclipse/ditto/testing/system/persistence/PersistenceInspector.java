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

import java.util.List;

/**
 * Inspects the persistence backend of Ditto (journals and snapshot stores) by counting persisted events and
 * snapshots per entity kind. Implementations exist per persistence backend (MongoDB, PostgreSQL) and are selected
 * via {@link PersistenceInspectorFactory}.
 * <p>
 * <strong>Cross-backend counting contract:</strong> MongoDB counts journal <em>documents</em> — one atomic batched
 * persist results in one document with an {@code events[]} array — while PostgreSQL counts <em>rows</em>, one per
 * event. The equality assertions of {@code CleanupIT} pass on MongoDB today, i.e. no batching occurs in practice —
 * this abstraction relies on that. If Ditto ever starts batching persists, both implementations diverge and this
 * javadoc is the breadcrumb.
 */
public interface PersistenceInspector extends AutoCloseable {

    /**
     * Counts the persisted journal events of the given things.
     *
     * @param thingIds the thing IDs (without the {@code thing:} PID prefix).
     * @return the number of persisted events.
     */
    long countThingEvents(List<String> thingIds);

    /**
     * Counts the persisted snapshots of the given things.
     *
     * @param thingIds the thing IDs (without the {@code thing:} PID prefix).
     * @return the number of persisted snapshots.
     */
    long countThingSnaps(List<String> thingIds);

    /**
     * Counts the persisted journal events of the given policies.
     *
     * @param policyIds the policy IDs (without the {@code policy:} PID prefix).
     * @return the number of persisted events.
     */
    long countPolicyEvents(List<String> policyIds);

    /**
     * Counts the persisted snapshots of the given policies.
     *
     * @param policyIds the policy IDs (without the {@code policy:} PID prefix).
     * @return the number of persisted snapshots.
     */
    long countPolicySnaps(List<String> policyIds);

    /**
     * Counts the persisted journal events of the given connections.
     *
     * @param connIds the connection IDs (without the {@code connection:} PID prefix).
     * @param onlyConnectivityEvents if {@code true}, only events of type {@code connectivity.events:*} are counted,
     * excluding e.g. no-op empty events.
     * @return the number of persisted events.
     */
    long countConnectionEvents(List<String> connIds, boolean onlyConnectivityEvents);

    /**
     * Counts the persisted snapshots of the given connections.
     *
     * @param connIds the connection IDs (without the {@code connection:} PID prefix).
     * @return the number of persisted snapshots.
     */
    long countConnectionSnaps(List<String> connIds);

    @Override
    void close();

}
