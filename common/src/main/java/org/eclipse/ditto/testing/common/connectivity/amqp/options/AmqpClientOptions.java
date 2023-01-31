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

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.testing.common.client.ditto_protocol.options.Option;

/**
 * This utility class allows creating {@link Option}s with custom values the AMQP Client is aware of.
 */
@Immutable
public final class AmqpClientOptions {

    private AmqpClientOptions() {
        throw new AssertionError();
    }

    @Immutable
    public static final class Send {

        private Send() {
            throw new AssertionError();
        }

        @SuppressWarnings("rawtypes")
        public static Option<Map> additionalAmqpMessageHeaders(final Map<String, String> additionalAmqpMessageHeaders) {
            return Option.newInstance(AmqpClientOptionDefinitions.Send.ADDITIONAL_AMQP_HEADERS,
                    ConditionChecker.checkNotNull(additionalAmqpMessageHeaders, "additionalAmqpMessageHeaders"));
        }

        /**
         * Option for setting a consumer that accepts the sent
         * {@link org.apache.qpid.jms.message.JmsTextMessage} to the AMQP client request.
         *
         * @param jmsTextMessageConsumer the consumer of the sent text message.
         * @return the option.
         * @throws NullPointerException if {@code jmsTextMessageConsumer} is {@code null}.
         */
        public static Option<JmsTextMessageConsumer> sentJmsTextMessageConsumer(
                final JmsTextMessageConsumer jmsTextMessageConsumer
        ) {
            return Option.newInstance(AmqpClientOptionDefinitions.Send.TEXT_MESSAGE_CONSUMER,
                    ConditionChecker.checkNotNull(jmsTextMessageConsumer, "jmsTextMessageConsumer"));
        }

    }

    @Immutable
    public static final class Receive {

        private Receive() {
            throw new AssertionError();
        }

        /**
         * Option for setting a consumer that accepts the received
         * {@link org.apache.qpid.jms.message.JmsTextMessage} to the AMQP client request.
         *
         * @param jmsTextMessageConsumer the consumer of the received text messages.
         * @return the option.
         * @throws NullPointerException if {@code jmsTextMessageConsumer} is {@code null}.
         */
        public static Option<JmsTextMessageConsumer> receivedJmsTextMessageConsumer(
                final JmsTextMessageConsumer jmsTextMessageConsumer
        ) {
            return Option.newInstance(AmqpClientOptionDefinitions.Receive.TEXT_MESSAGE_CONSUMER,
                    ConditionChecker.checkNotNull(jmsTextMessageConsumer, "jmsTextMessageConsumer"));
        }

    }

}
