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

public class ServiceInstanceId implements CharSequence {

    private final String id;

    public ServiceInstanceId(final String id) {this.id = id;}

    public static ServiceInstanceId of(final CharSequence id) {
        return new ServiceInstanceId(id.toString());
    }

    @Override
    public int length() {
        return id.length();
    }

    @Override
    public char charAt(final int index) {
        return id.charAt(index);
    }

    @Override
    public CharSequence subSequence(final int start, final int end) {
        return id.subSequence(start, end);
    }

    @Override
    public String toString() {
        return id;
    }
}
