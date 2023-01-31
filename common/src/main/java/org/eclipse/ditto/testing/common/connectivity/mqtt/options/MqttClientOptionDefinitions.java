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
package org.eclipse.ditto.testing.common.connectivity.mqtt.options;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.testing.common.client.ditto_protocol.options.OptionDefinition;
import org.eclipse.ditto.testing.common.client.ditto_protocol.options.OptionName;

/**
 * Provides all {@link OptionDefinition}s the MQTT client is aware of.
 */
@Immutable
public final class MqttClientOptionDefinitions {

    private MqttClientOptionDefinitions() {
        throw new AssertionError();
    }

    @Immutable
    public static final class Publish {

        private Publish() {
            throw new AssertionError();
        }

        public static final OptionDefinition<String> REPLY_TO_HEADER =
                OptionDefinition.newInstance(OptionName.of("REPLY_TO_HEADER"), String.class);

    }

}
