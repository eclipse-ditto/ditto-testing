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

/**
 * Defines the existing categories of connections used in connectivity system tests e.g. with payload mapping, with
 * extra fields. These categories are used with the annotation @{@link Connections} to specify which connections are
 * required by a test method.
 */
public enum ConnectionCategory {
    CONNECTION1,
    CONNECTION2,
    CONNECTION_WITH_PAYLOAD_MAPPING,
    CONNECTION_WITH_AUTH_PLACEHOLDER_ON_HEADER_ID,
    CONNECTION_WITH_NAMESPACE_AND_RQL_FILTER,
    CONNECTION_WITH_ENFORCEMENT_ENABLED,
    CONNECTION_WITH_HEADER_MAPPING,
    CONNECTION_WITH_MULTIPLE_PAYLOAD_MAPPINGS,
    CONNECTION_WITH_EXTRA_FIELDS,
    CONNECTION_WITH_RAW_MESSAGE_MAPPER_1,
    CONNECTION_WITH_RAW_MESSAGE_MAPPER_2,
    CONNECTION_WITH_SSH_TUNNEL,
    CONNECTION_WITH_CONNECTION_ANNOUNCEMENTS,
    CONNECTION_WITH_2_SOURCES,
    CONNECTION_HONO,
    // special category if no connection is required i.e. the test creates a connection itself
    NONE
}
