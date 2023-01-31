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
package org.eclipse.ditto.testing.system.things.rest.live;

import java.net.URI;
import java.text.MessageFormat;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.auth.AuthorizationContext;
import org.eclipse.ditto.base.model.auth.AuthorizationSubject;
import org.eclipse.ditto.base.model.auth.DittoAuthorizationContextType;
import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.ConnectivityStatus;
import org.eclipse.ditto.connectivity.model.Topic;
import org.eclipse.ditto.policies.model.SubjectIssuer;

/**
 * Factory for creating an MQTT {@link org.eclipse.ditto.connectivity.model.Connection} for test purposes.
 */
@Immutable
final class MqttConnectionFactory {

    private MqttConnectionFactory() {
        throw new AssertionError();
    }

    static Connection getMqttConnection(final URI mqttServerUri,
            final CharSequence connectionName,
            final CharSequence signalTargetTopic,
            final CharSequence signalSourceTopic,
            final AcknowledgementLabel... declaredSourceAckLabels) {

        ConditionChecker.checkNotNull(mqttServerUri, "mqttServerUri");
        ConditionChecker.argumentNotEmpty(connectionName, "connectionName");
        ConditionChecker.argumentNotEmpty(signalTargetTopic, "signalTargetTopic");
        ConditionChecker.argumentNotEmpty(signalSourceTopic, "signalSourceTopic");
        ConditionChecker.checkNotNull(declaredSourceAckLabels, "declaredSourceAckLabels");

        final var authorizationContext =
                AuthorizationContext.newInstance(DittoAuthorizationContextType.PRE_AUTHENTICATED_CONNECTION,
                        AuthorizationSubject.newInstance(MessageFormat.format("{0}:{1}",
                                SubjectIssuer.INTEGRATION,
                                connectionName)));

        // This ID will be discarded anyway by posting the connection to Solutions.
        return ConnectivityModelFactory.newConnectionBuilder(ConnectionId.generateRandom(),
                        ConnectionType.MQTT,
                        ConnectivityStatus.OPEN,
                        mqttServerUri.toString())
                .name(connectionName.toString())
                .targets(List.of(ConnectivityModelFactory.newTargetBuilder()
                        .address(signalTargetTopic.toString())
                        .authorizationContext(authorizationContext)
                        .qos(1)
                        .topics(Topic.LIVE_EVENTS, Topic.LIVE_COMMANDS, Topic.TWIN_EVENTS)
                        .build()))
                .sources(List.of(ConnectivityModelFactory.newSourceBuilder()
                        .address(signalSourceTopic + "/#")
                        .authorizationContext(authorizationContext)
                        .consumerCount(1)
                        .declaredAcknowledgementLabels(Set.of(declaredSourceAckLabels))
                        .qos(1)
                        .build()))
                .build();
    }

}
