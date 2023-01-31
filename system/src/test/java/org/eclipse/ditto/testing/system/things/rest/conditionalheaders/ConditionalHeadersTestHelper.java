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
package org.eclipse.ditto.testing.system.things.rest.conditionalheaders;

import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;

public class ConditionalHeadersTestHelper {

    public static final String E_TAG_HEADER_KEY = DittoHeaderDefinition.ETAG.getKey();
    public static final String IF_NONE_MATCH_HEADER_KEY = DittoHeaderDefinition.IF_NONE_MATCH.getKey();
    public static final String IF_MATCH_HEADER_KEY = DittoHeaderDefinition.IF_MATCH.getKey();
    public static final String ASTERISK = "*";
    public static final String E_TAG_1 = "\"rev:1\"";
    public static final String E_TAG_2 = "\"rev:2\"";
    public static final String VALUE_STR_1 = "valueForRev1";
    public static final String VALUE_STR_2 = "valueForRev2";

}
