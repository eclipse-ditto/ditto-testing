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

import java.io.InputStream;
import java.util.MissingResourceException;
import java.util.Optional;
import java.util.Scanner;

public final class Resources {

    private Resources() {
        throw new AssertionError();
    }

    static final String INCOMING_SCRIPT = readAsString("incoming-script.js");
    static final String OUTGOING_SCRIPT = readAsString("outgoing-script.js");
    static final String INCOMING_SCRIPT_MERGE = readAsString("incoming-script-merge.js");
    static final String OUTGOING_SCRIPT_MERGE = readAsString("outgoing-script-merge.js");
    static final String OUTGOING_SCRIPT_CONNECTION_ANNOUNCEMENTS =
            readAsString("outgoing-script-connection-announcements.js");


    private static String readAsString(final String name) {
        final InputStream s = getResourceStream(name);
        final Scanner scanner = new Scanner(s).useDelimiter("\\A");
        final String out = scanner.hasNext() ? scanner.next() : "";
        scanner.close();
        return out;
    }

    private static InputStream getResourceStream(final String name) {
        return Optional.ofNullable(Resources.class.getClassLoader().getResourceAsStream(name))
                .orElseThrow(() -> new MissingResourceException("No resource found for name: " + name, null, name));
    }
}
