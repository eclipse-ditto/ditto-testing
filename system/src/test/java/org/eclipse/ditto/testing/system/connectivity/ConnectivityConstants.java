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

public class ConnectivityConstants {

    /**
     * The header used to test placeholder substitution
     */
    public static final String HEADER_ID = "headerId";

    public static final String TARGET_SUFFIX = "_org"; // substring-before "org.eclipse.ditto" - taking the "_org" - see below

    // should match from namespace "org.eclipse.ditto.ns1.*" the "org":
    public static final String TARGET_SUFFIX_PLACEHOLDER = "_{{thing:namespace | fn:substring-before('.') | fn:default(thing:namespace)}}";


}
