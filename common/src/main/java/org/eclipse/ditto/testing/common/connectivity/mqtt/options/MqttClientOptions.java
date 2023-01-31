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

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.testing.common.client.ditto_protocol.options.Option;

/**
 * This utility class allows creating {@link Option}s with custom values the MQTT Client is aware of.
 */
public final class MqttClientOptions {

    private MqttClientOptions() {
        throw new AssertionError();
    }

    public static final class Publish {

        private Publish() {
            throw new AssertionError();
        }

        public static Option<String> replyToHeaderValue(final CharSequence replyToHeaderValue) {
            ConditionChecker.checkNotNull(replyToHeaderValue, "replyToHeaderValue");
            return Option.newInstance(MqttClientOptionDefinitions.Publish.REPLY_TO_HEADER, replyToHeaderValue.toString());
        }

    }

}
