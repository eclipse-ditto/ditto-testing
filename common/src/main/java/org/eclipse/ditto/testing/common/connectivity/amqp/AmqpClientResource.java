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
package org.eclipse.ditto.testing.common.connectivity.amqp;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.text.MessageFormat;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.testing.common.junit.ExternalResourcePlus;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This {@link ExternalResourcePlus} creates and connects an {@link AmqpClient} before the test is run and disconnects
 * that client after the test was run.
 * The client can be retrieved with {@link #getAmqpClient()}.
 * </p>
 * <p>
 * The connection name, source address and reply address to be used for the client could be optionally provided via the
 * {@link Config} annotation.
 * </p>
 */
@NotThreadSafe
public final class AmqpClientResource extends ExternalResourcePlus {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpClientResource.class);

    private final AmqpConfig amqpConfig;

    private String connectionName;
    private String sourceAddress;
    private String targetAddress;
    private String replyAddress;
    private AmqpClient amqpClient;
    private final Supplier<String> addressSuffixSupplier;

    private AmqpClientResource(final AmqpConfig amqpConfig, final Supplier<String> addressSuffixSupplier) {
        this.amqpConfig = amqpConfig;
        this.addressSuffixSupplier = addressSuffixSupplier;

        connectionName = null;
        sourceAddress = null;
        targetAddress = null;
        replyAddress = null;
        amqpClient = null;
    }

    /**
     * Returns a new instance of {@code AmqpClientResource}.
     *
     * @param amqpConfig provides the base configuration settings of the AMQP client the returned resource manages.
     * @return the instance.
     * @throws NullPointerException if {@code amqpConfig} is {@code null}.
     */
    public static AmqpClientResource newInstance(final AmqpConfig amqpConfig) {
        return newInstance(amqpConfig, () -> "");
    }

    public static AmqpClientResource newInstance(final AmqpConfig amqpConfig, Supplier<String> addressSuffixSupplier) {
        return new AmqpClientResource(ConditionChecker.checkNotNull(amqpConfig, "amqpConfig"), addressSuffixSupplier);
    }

    @Override
    protected void before(final Description description) throws Throwable {
        super.before(description);

        final var configAnnotationOptional = getConfigAnnotation(description);

        connectionName = configAnnotationOptional
                .map(Config::connectionName)
                .filter(Predicate.not(String::isBlank))
                .orElseGet(() -> {
                    final var testClass = description.getTestClass();
                    return testClass.getSimpleName();
                });

        sourceAddress = configAnnotationOptional
                .map(Config::sourceAddress)
                .filter(Predicate.not(String::isBlank))
                .orElseGet(() -> MessageFormat.format("source-{0}-{1}", connectionName, UUID.randomUUID())) +
                addressSuffixSupplier.get();

        targetAddress = configAnnotationOptional
                .map(Config::targetAddress)
                .filter(Predicate.not(String::isBlank))
                .orElseGet(() -> MessageFormat.format("target-{0}-{1}", connectionName, UUID.randomUUID())) +
                addressSuffixSupplier.get();

        replyAddress = configAnnotationOptional
                .map(Config::replyAddress)
                .filter(Predicate.not(String::isBlank))
                .orElseGet(() -> MessageFormat.format("reply-{0}-{1}", connectionName, UUID.randomUUID())) +
                addressSuffixSupplier.get();

        amqpClient = createAmqpClient(connectionName);
        connect(amqpClient);
    }

    private static Optional<Config> getConfigAnnotation(final Description description) {
        final var annotations = description.getAnnotations();
        return annotations.stream()
                .filter(annotation -> Config.class.isAssignableFrom(annotation.annotationType()))
                .map(Config.class::cast)
                .findFirst();
    }

    private AmqpClient createAmqpClient(final String connectionName) {
        final var result = AmqpClient.newInstance(amqpConfig.getAmqp10Uri(),
                connectionName,
                sourceAddress,
                targetAddress,
                replyAddress);
        LOGGER.info("Initialised {}.", result);
        return result;
    }

    private static void connect(final AmqpClient amqpClient) {
        try {
            amqpClient.connect();
        } finally {
            LOGGER.info("Connected {}.", amqpClient);
        }
    }

    @Override
    protected void after(final Description description) {
        try {
            amqpClient.disconnect();
        } finally {
            LOGGER.info("Disconnected {}.", amqpClient);
        }
        super.after(description);
    }

    /**
     * Returns the {@link AmqpClient} which is managed by this resource.
     *
     * @return the {@code AmqpClient}.
     * @throws IllegalStateException if this method is called before the test was run.
     */
    public AmqpClient getAmqpClient() {
        if (null == amqpClient) {
            final var pattern = "{0} gets only initialised by running the test.";
            throw new IllegalStateException(MessageFormat.format(pattern, AmqpClient.class.getSimpleName()));
        } else {
            return amqpClient;
        }
    }

    public URI getEndpointUri() {
        return amqpConfig.getAmqp10Uri();
    }

    public String getConnectionName() {
        if (null == connectionName) {
            throw new IllegalStateException("The connection name gets only initialised by running the test.");
        } else {
            return connectionName;
        }
    }

    public String getSourceAddress() {
        if (null == sourceAddress) {
            throw new IllegalStateException("The source address gets only initialised by running the test.");
        } else {
            return sourceAddress;
        }
    }

    public String getTargetAddress() {
        if (null == targetAddress) {
            throw new IllegalStateException("The target address gets only initialised by running the test.");
        } else {
            return targetAddress;
        }
    }

    public String getReplyAddress() {
        if (null == replyAddress) {
            throw new IllegalStateException("The reply address gets only initialised by running the test.");
        } else {
            return replyAddress;
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE, ElementType.METHOD})
    public @interface Config {

        String connectionName() default "";

        String sourceAddress() default "";

        String targetAddress() default "";

        String replyAddress() default "";

    }

}
