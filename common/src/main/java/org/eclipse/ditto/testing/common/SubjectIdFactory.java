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

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.policies.model.SubjectId;

/**
 * A factory for creating various instances of {@link SubjectId}.
 */
@Immutable
public final class SubjectIdFactory {

    private SubjectIdFactory() {
        throw new AssertionError();
    }

    public static SubjectId getSubjectIdForUsername(final String userName) {
        ConditionChecker.checkNotNull(userName, "userName");
        return SubjectId.newInstance(String.format("ditto:%s", userName));
    }

}
