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
package org.eclipse.ditto.testing.common.conditions;

import org.eclipse.ditto.testing.common.CommonTestConfig;

/**
 * Condition for tests which should only be run against a Docker environment (not against cloud environments).
 */
public final class DockerEnvironment implements Condition {

    @Override
    public boolean isSatisfied() {
        return CommonTestConfig.getInstance().isLocalOrDockerTestEnvironment();
    }

}
