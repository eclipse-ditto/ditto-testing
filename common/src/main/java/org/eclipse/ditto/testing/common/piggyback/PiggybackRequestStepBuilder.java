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

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.signals.commands.Command;
import org.eclipse.ditto.testing.common.ServiceInstanceId;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;

/**
 * A mutable builder with a fluent API to create a {@link PiggybackRequest}.
 * This builder uses object scoping to ensure that all mandatory properties are set.
 */
@NotThreadSafe
public interface PiggybackRequestStepBuilder {

    interface CommandStep {

        TargetActorSelectionStep withCommand(Command<?> command);

    }

    interface TargetActorSelectionStep {

        BuildableStep withTargetActorSelection(CharSequence targetActorSelection);

    }

    interface BuildableStep {

        BuildableStep withCorrelationId(@Nullable CorrelationId correlationId);

        BuildableStep withAdditionalHeaders(@Nullable DittoHeaders additionalHeaders);

        BuildableStep withServiceInstanceId(@Nullable ServiceInstanceId serviceInstanceId);

        PiggybackRequest build();

    }

}
