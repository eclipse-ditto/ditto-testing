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

import static org.eclipse.ditto.policies.model.PoliciesModelFactory.newSubjectIssuer;

import org.eclipse.ditto.policies.model.SubjectIssuer;

/**
 * An enumeration of all known issuers for a {@code SubjectId}.
 */
public final class ThingsSubjectIssuer {

    /**
     * Subject issuer of Ditto.
     */
    public static final SubjectIssuer DITTO = newSubjectIssuer("ditto");

    private ThingsSubjectIssuer() {
        // no initialization
    }

}
