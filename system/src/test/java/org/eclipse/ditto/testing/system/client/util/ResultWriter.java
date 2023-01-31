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
package org.eclipse.ditto.testing.system.client.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.StringJoiner;

/**
 * Write result data for Jenkins plot plugin.
 */
public class ResultWriter {

    private static final String Y_VALUE_KEY = "YVALUE";
    private static final String MEASUREREMENT_PATH = "./measurements";

    public static void writeResult(String test, String label, String value) throws IOException {
        final File path = Paths.get(MEASUREREMENT_PATH, test).toFile();

        if (!path.exists()) {
            path.mkdirs();
        }

        final String builder = "# " +
                Instant.now().toString() +
                "\n" +
                Y_VALUE_KEY +
                "=" +
                value;
        Files.write(Paths.get(MEASUREREMENT_PATH, test, label + ".properties"), builder.getBytes());
    }

    public static void writeResult(String target, List<String> lines) throws IOException {
        if (Paths.get(target).getParent() != null) {
            final File file = Paths.get(target).getParent().toFile();
            if (!file.exists()) {
                file.mkdirs();
            }
        }

        try (final FileWriter writer = new FileWriter(target)) {
            writer.write(new StringJoiner("\n").add(lines.get(0)).add(lines.get(1)).toString());
        }
    }

}
