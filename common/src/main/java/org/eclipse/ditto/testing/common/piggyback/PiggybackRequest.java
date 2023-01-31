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
package org.eclipse.ditto.testing.common.piggyback;

import static org.eclipse.ditto.base.model.common.ConditionChecker.argumentNotEmpty;

import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.base.api.devops.signals.commands.ExecutePiggybackCommand;
import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.signals.commands.Command;
import org.eclipse.ditto.testing.common.ServiceInstanceId;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;

/**
 * Represents a request for a {@link ExecutePiggybackCommand}.
 */
@Immutable
public final class PiggybackRequest {

    private final String serviceName;
    private final Command<?> command;
    private final String targetActorSelection;
    private final DittoHeaders additionalHeaders;
    @Nullable private final String serviceInstanceId;

    private PiggybackRequest(final Builder builder) {
        serviceName = builder.serviceName;
        command = builder.command;
        targetActorSelection = builder.targetActorSelection;
        additionalHeaders = builder.additionalHeaders;
        serviceInstanceId = null != builder.serviceInstanceId ? builder.serviceInstanceId.toString() : null;
    }

    /**
     * Returns a new step builder for creating an instance of {@code PiggybackRequest}.
     *
     * @param serviceName the name of the service the request is sent to.
     * @return the builder.
     * @throws NullPointerException if {@code serviceName} is {@code null}.
     * @throws IllegalArgumentException if {@code serviceName} is empty.
     */
    public static PiggybackRequestStepBuilder.CommandStep newBuilder(final CharSequence serviceName) {
        return new Builder(argumentNotEmpty(serviceName, "serviceName"));
    }

    /**
     * Returns a {@code ExecutePiggybackCommand} that is created with the properties of this piggyback request.
     *
     * @return the {@code ExecutePiggybackCommand}.
     */
    public ExecutePiggybackCommand getPiggybackCommand() {
        return ExecutePiggybackCommand.of(serviceName,
                serviceInstanceId,
                targetActorSelection,
                command.toJson(),
                getHeaders());
    }

    private DittoHeaders getHeaders() {
        return additionalHeaders.toBuilder()
                .putHeader("aggregate", "false")
                .build();
    }

    /**
     * Returns the name of the service the request is sent to.
     *
     * @return the service name.
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * A mutable builder with a fluent API for creating a {@code PiggybackRequest}.
     */
    @NotThreadSafe
    public static final class Builder implements PiggybackRequestStepBuilder.CommandStep,
            PiggybackRequestStepBuilder.TargetActorSelectionStep, PiggybackRequestStepBuilder.BuildableStep {

        private final String serviceName;
        private Command<?> command;
        private String targetActorSelection;
        private DittoHeaders additionalHeaders;
        @Nullable private ServiceInstanceId serviceInstanceId;

        private Builder(final CharSequence serviceName) {
            this.serviceName = serviceName.toString();
            command = null;
            targetActorSelection = null;
            additionalHeaders = DittoHeaders.empty();
            serviceInstanceId = null;
        }

        @Override
        public PiggybackRequestStepBuilder.TargetActorSelectionStep withCommand(final Command<?> command) {
            this.command = ConditionChecker.checkNotNull(command, "command");
            return this;
        }

        @Override
        public PiggybackRequestStepBuilder.BuildableStep withTargetActorSelection(final CharSequence targetActorSelection) {
            argumentNotEmpty(targetActorSelection, "targetActorSelection");
            this.targetActorSelection = targetActorSelection.toString();
            return this;
        }

        @Override
        public PiggybackRequestStepBuilder.BuildableStep withCorrelationId(@Nullable final CorrelationId correlationId) {
            additionalHeaders = DittoHeaders.newBuilder(additionalHeaders)
                    .correlationId(correlationId)
                    .build();
            return this;
        }

        @Override
        public PiggybackRequestStepBuilder.BuildableStep withAdditionalHeaders(final DittoHeaders additionalHeaders) {
            this.additionalHeaders = Objects.requireNonNullElseGet(additionalHeaders, DittoHeaders::empty);
            return this;
        }

        @Override
        public PiggybackRequestStepBuilder.BuildableStep withServiceInstanceId(@Nullable final ServiceInstanceId serviceInstanceId) {
            this.serviceInstanceId = serviceInstanceId;
            return this;
        }

        @Override
        public PiggybackRequest build() {
            return new PiggybackRequest(this);
        }

    }

}
