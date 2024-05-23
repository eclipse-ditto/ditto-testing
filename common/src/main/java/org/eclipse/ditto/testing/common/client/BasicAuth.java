/*
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
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
package org.eclipse.ditto.testing.common.client;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

/**
 * Utility class for Basic Auth properties.
 */
public final class BasicAuth {

    private final boolean enabled;
    private final String username;
    private final String password;

    private BasicAuth(final boolean enabled,
            final String username,
            final String password
        ) {

        this.enabled = enabled;
        this.username = username;
        this.password = password;
    }

    public static BasicAuth newInstance(final Boolean enabled,
            final String username,
            final String password
    ) {

        checkNotNull(enabled, "enabled");
        if (Boolean.TRUE.equals(enabled)) {
            checkNotNull(username, "username");
            checkNotNull(password, "password");
        }

        return new BasicAuth(enabled, username, password);
    }

    /**
     * Returns if the Basic Auth enabled or not
     *
     * @return the enabled flag.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Returns the Basic Auth client username
     *
     * @return the username.
     */
    public String getUsername() {
        return username;
    }

    /**
     * Returns the Basic Auth password.
     *
     * @return the password.
     */
    public String getPassword() {
        return password;
    }

}
