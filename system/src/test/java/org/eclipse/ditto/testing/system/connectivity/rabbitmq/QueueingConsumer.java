/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
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
package org.eclipse.ditto.testing.system.connectivity.rabbitmq;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * Minimal drop-in replacement for {@code com.rabbitmq.client.QueueingConsumer}, which was removed in
 * amqp-client 5.0. Buffers incoming deliveries in a blocking queue and exposes a blocking
 * {@link #nextDelivery(long)}, mirroring the subset of the removed class used by the RabbitMQ
 * connectivity tests.
 */
class QueueingConsumer extends DefaultConsumer {

    private final LinkedBlockingQueue<Delivery> deliveries = new LinkedBlockingQueue<>();

    QueueingConsumer(final Channel channel) {
        super(channel);
    }

    @Override
    public void handleDelivery(final String consumerTag, final Envelope envelope,
            final AMQP.BasicProperties properties, final byte[] body) throws IOException {
        deliveries.add(new Delivery(envelope, properties, body));
    }

    /**
     * Waits for and returns the next delivery, waiting at most {@code timeoutMillis} milliseconds.
     *
     * @param timeoutMillis how long to wait before giving up, in milliseconds.
     * @return the next delivery, or {@code null} if no delivery arrived within the timeout.
     * @throws InterruptedException if interrupted while waiting.
     */
    Delivery nextDelivery(final long timeoutMillis) throws InterruptedException {
        return deliveries.poll(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Buffered delivery, mirroring the removed {@code com.rabbitmq.client.QueueingConsumer.Delivery}.
     */
    static final class Delivery {

        private final Envelope envelope;
        private final AMQP.BasicProperties properties;
        private final byte[] body;

        Delivery(final Envelope envelope, final AMQP.BasicProperties properties, final byte[] body) {
            this.envelope = envelope;
            this.properties = properties;
            this.body = body;
        }

        Envelope getEnvelope() {
            return envelope;
        }

        AMQP.BasicProperties getProperties() {
            return properties;
        }

        byte[] getBody() {
            return body;
        }
    }
}
