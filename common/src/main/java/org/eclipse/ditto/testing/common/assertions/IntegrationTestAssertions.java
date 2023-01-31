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
package org.eclipse.ditto.testing.common.assertions;

import org.asynchttpclient.Response;
import org.eclipse.ditto.base.model.signals.commands.assertions.CommandAssertions;

/**
 * Assertions for integration tests.
 */
public class IntegrationTestAssertions extends CommandAssertions {

    public static AhcResponseAssert assertThat(final Response response) {
        return new AhcResponseAssert(response);
    }

}
