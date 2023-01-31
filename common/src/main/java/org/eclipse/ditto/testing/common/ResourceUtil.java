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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;

/**
 * Utility class providing the basic functionality of loading resource files.
 */
public interface ResourceUtil {

    default String getPayload(final String fileName) {
        return ResourceUtil.getResource(fileName);
    }

    static String getResource(final String fileName) {
        final InputStream command = ResourceUtil.class.getClassLoader().getResourceAsStream(fileName);
        try {
            return IOUtils.toString(command, StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    String getSchema();

}
