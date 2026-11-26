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

import org.eclipse.ditto.testing.common.CommonTestConfig;

/**
 * Creates the {@link PersistenceInspector} matching the persistence backend configured for the test environment
 * ({@code persistence.backend}, default {@code mongodb}).
 */
public final class PersistenceInspectorFactory {

    private static final String BACKEND_POSTGRES = "postgres";

    private PersistenceInspectorFactory() {
        throw new AssertionError();
    }

    /**
     * Creates a {@code PersistenceInspector} for the persistence backend configured in the given test config.
     *
     * @param testConfig the common test config.
     * @return a {@link PostgresPersistenceInspector} if {@code persistence.backend} is {@code postgres}, else a
     * {@link MongoPersistenceInspector}.
     */
    public static PersistenceInspector create(final CommonTestConfig testConfig) {
        if (BACKEND_POSTGRES.equalsIgnoreCase(testConfig.getPersistenceBackend())) {
            return new PostgresPersistenceInspector(testConfig.getPostgresJdbcUri(),
                    testConfig.getPostgresUser(),
                    testConfig.getPostgresPassword());
        }
        return new MongoPersistenceInspector(testConfig.getMongoDBUri());
    }

}
