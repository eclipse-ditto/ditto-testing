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

/**
 * An enumeration of all known resource of the Ditto HTTP API.
 */
public enum HttpResource {

    POLICIES("/policies"),

    POLICY_ENTRIES("/entries"),

    RESOURCES("/resources"),

    SUBJECTS("/subjects"),

    ACTIONS("/actions"),

    THINGS("/things"),

    SEARCH("/search/things"),

    COUNT("/count"),

    POLICY_ID("/policyId"),

    POLICY("/policy"),

    ATTRIBUTES("/attributes"),

    INBOX("/inbox"),

    OUTBOX("/outbox"),

    MESSAGES("/messages"),

    FEATURES("/features"),

    DEFINITION("/definition"),

    PROPERTIES("/properties"),

    DESIRED_PROPERTIES("/desiredProperties"),

    SOLUTIONS("/solutions"),

    NAMESPACES("/namespaces"),

    CONNECTIONS("/connections"),

    CLIENTS("/clients"),

    LOGS("/logs"),

    COMMAND("/command"),

    STATUS("/status"),

    USAGE("/usage"),

    WHOAMI("/whoami");

    private final String path;

    HttpResource(final String path) {
        this.path = path;
    }

    /**
     * Returns the path of the resource.
     *
     * @return the path.
     */
    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return getPath();
    }
}
