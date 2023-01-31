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

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import javax.annotation.concurrent.Immutable;

/**
 * A simple holder for authentication credentials.
 */
@Immutable
public final class SimpleUserNamePasswordCredentials implements UserNamePasswordCredentials {

    private final String userName;
    private final String password;

    /**
     * Constructs a new {@code Credentials} object.
     *
     * @param userName the user name.
     * @param password the password.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public SimpleUserNamePasswordCredentials(final CharSequence userName, final CharSequence password) {
        this.userName = requireNonNull(userName).toString();
        this.password = requireNonNull(password).toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SimpleUserNamePasswordCredentials that = (SimpleUserNamePasswordCredentials) o;
        return Objects.equals(userName, that.userName) && Objects.equals(password, that.password);
    }

    @Override
    public String getUserName() {
        return userName;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public int hashCode() {
        return Objects.hash(userName, password);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [" +
                "userName='" + userName + '\'' +
                ", password='" + "*****" + '\'' +
                ']';
    }

}
