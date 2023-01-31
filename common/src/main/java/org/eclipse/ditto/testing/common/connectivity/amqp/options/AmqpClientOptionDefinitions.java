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
package org.eclipse.ditto.testing.common.connectivity.amqp.options;

import java.util.Map;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.testing.common.client.ditto_protocol.options.OptionDefinition;
import org.eclipse.ditto.testing.common.client.ditto_protocol.options.OptionName;

/**
 * Provides all {@link OptionDefinition}s the AMQP client is aware of.
 */
@Immutable
public final class AmqpClientOptionDefinitions {

    private AmqpClientOptionDefinitions() {
        throw new AssertionError();
    }

    @Immutable
    public static final class Send {

        private Send() {
            throw new AssertionError();
        }

        @SuppressWarnings("rawtypes")
        public static final OptionDefinition<Map> ADDITIONAL_AMQP_HEADERS =
                OptionDefinition.newInstance(OptionName.of("ADDITIONAL_AMQP_HEADERS"), Map.class);

        public static final OptionDefinition<JmsTextMessageConsumer> TEXT_MESSAGE_CONSUMER =
                OptionDefinition.newInstance(OptionName.of("SENT_TEXT_MESSAGE_CONSUMER"),
                        JmsTextMessageConsumer.class);

        public static final OptionDefinition<MessageAnnotationHeader> MESSAGE_ANNOTATION_HEADER =
                OptionDefinition.newInstance(OptionName.of("MESSAGE_ANNOTATION_HEADER"), MessageAnnotationHeader.class);

    }

    @Immutable
    public static final class Receive {

        private Receive() {
            throw new AssertionError();
        }

        public static final OptionDefinition<JmsTextMessageConsumer> TEXT_MESSAGE_CONSUMER =
                OptionDefinition.newInstance(OptionName.of("RECEIVED_TEXT_MESSAGE_CONSUMER"),
                        JmsTextMessageConsumer.class);

    }

    public record MessageAnnotationHeader(String key, String value) {}

}
