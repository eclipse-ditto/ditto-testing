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
package org.eclipse.ditto.testing.system.search.sync.common;

import java.time.Duration;

import org.eclipse.ditto.testing.common.CommonTestConfig;

/**
 * Provides test configuration for the Things-Search sync tests.
 */
public class SearchSyncTestConfig extends CommonTestConfig {

    private static final String PROPERTY_THINGS_SEARCH_SYNC_TIMEOUT = "things-search-sync.wait-for-sync-timeout";

    private static final SearchSyncTestConfig INSTANCE = new SearchSyncTestConfig();

    public static SearchSyncTestConfig getInstance() {
        return INSTANCE;
    }

    /**
     * Gets the timeout to wait for sync.
     *
     * @return the timeout
     */
    public Duration getThingsSyncWaitTimeout() {
        return conf.getDuration(PROPERTY_THINGS_SEARCH_SYNC_TIMEOUT);
    }

}
