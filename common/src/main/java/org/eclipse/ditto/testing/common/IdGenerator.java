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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.RandomStringUtils;

/**
 * Generates IDs for a given namespace.
 */
public final class IdGenerator {

    private static final String AFFIX_SEPARATOR = "-";
    private final String namespace;

    private IdGenerator(final String namespace) {
        this.namespace = namespace;
    }

    public static IdGenerator fromNamespace(final String namespace) {
        return new IdGenerator(requireNonNull(namespace));
    }

    public static String createRandomNamespace(final String namespacePrefix) {
        requireNonNull(namespacePrefix);

        return escapeForNamespace(namespacePrefix) + createRandomNamespace();
    }

    public static String createRandomNamespace() {
        return RandomStringUtils.randomAlphabetic(8);
    }

    public String getNamespace() {
        return namespace;
    }

    public String withRandomName() {
        return createId(createRandomName());
    }

    public String withRandomNameAndVersion() {
        return withRandomName() + ":" + createRandomVersion();
    }

    public String withPrefixedRandomName(final String prefix, final String... furtherPrefixes) {
        requireNonNull(prefix);
        requireNonNull(furtherPrefixes);

        final String completePrefix = createName(prefix, furtherPrefixes);

        return createId(completePrefix + AFFIX_SEPARATOR + createRandomName());
    }

    public String withSuffixedRandomName(final String suffix) {
        requireNonNull(suffix);

        return createId(createRandomName() + AFFIX_SEPARATOR + suffix);
    }

    public String withName(final String name, final String... furtherNameComponents) {
        requireNonNull(name);
        requireNonNull(furtherNameComponents);

        return createId(createName(name, furtherNameComponents));
    }

    private String createId(final String name) {
        return namespace + ":" + name;
    }

    private static String createRandomVersion() {
        return RandomStringUtils.randomNumeric(1);
    }

    private static String createRandomName() {
        return RandomStringUtils.randomAlphanumeric(8);
    }

    private String createName(final String firstPart, final String... furtherParts) {
        final String escapedFirstPart = escapeForName(firstPart);
        final List<String> escapedFurtherParts = Stream.of(furtherParts)
                .map(IdGenerator::escapeForName)
                .collect(Collectors.toList());

        return joinNameParts(escapedFirstPart, escapedFurtherParts);
    }

    private static String escapeForNamespace(final String namespacePart) {
        // incomplete, feel free to add further replacements when required
        return escapeForName(namespacePart);
    }

    private static String escapeForName(final String namePart) {
        // incomplete, feel free to add further replacements when required
        return namePart.replaceAll("\\[", "_").replaceAll("]", "_");
    }

    private String joinNameParts(final String firstPart, final List<String> furtherParts) {
        final String furtherStr = String.join(AFFIX_SEPARATOR, furtherParts);
        final String completeName;
        if (furtherStr.isEmpty()) {
            completeName = firstPart;
        } else {
            completeName = String.join(AFFIX_SEPARATOR, firstPart, furtherStr);
        }
        return completeName;
    }
}
