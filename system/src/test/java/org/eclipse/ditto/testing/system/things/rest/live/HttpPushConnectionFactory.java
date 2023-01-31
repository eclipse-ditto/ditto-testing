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

import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;

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
 * Factory for creating an HTTP Push {@link org.eclipse.ditto.connectivity.model.Connection} for test purposes.
 */
@Immutable
final class HttpPushConnectionFactory {

    private HttpPushConnectionFactory() {
        throw new AssertionError();
    }

    static Connection getHttpPushConnection(final String httpPushServerName,
            final int httpPushServerPort,
            final CharSequence username,
            final CharSequence connectionName,
            final CharSequence signalTargetTopic,
            final AcknowledgementLabel... declaredSourceAckLabels) {

        ConditionChecker.checkNotNull(httpPushServerName, "httpPushServerName");
        ConditionChecker.checkNotNull(httpPushServerPort, "httpPushServerPort");
        ConditionChecker.argumentNotEmpty(username, "username");
        ConditionChecker.argumentNotEmpty(connectionName, "connectionName");
        ConditionChecker.argumentNotEmpty(signalTargetTopic, "signalTargetTopic");
        ConditionChecker.checkNotNull(declaredSourceAckLabels, "declaredSourceAckLabels");

        final var authorizationContext =
                AuthorizationContext.newInstance(DittoAuthorizationContextType.PRE_AUTHENTICATED_CONNECTION,
                        AuthorizationSubject.newInstance(MessageFormat.format("{0}:{1}:{2}",
                                SubjectIssuer.INTEGRATION,
                                username,
                                connectionName)));

        // This ID will be discarded anyway by posting the connection to Solutions.
        return ConnectivityModelFactory.newConnectionBuilder(ConnectionId.generateRandom(),
                        ConnectionType.HTTP_PUSH,
                        ConnectivityStatus.OPEN,
                        buildHttpPushServerUri(httpPushServerName, httpPushServerPort))
                .name(connectionName.toString())
                .targets(List.of(ConnectivityModelFactory.newTargetBuilder()
                        .address(signalTargetTopic.toString())
                        .authorizationContext(authorizationContext)
                        .headerMapping(ConnectivityModelFactory.newHeaderMapping(
                                Collections.singletonMap("content-type", "application/json")))
                        .topics(Topic.LIVE_EVENTS, Topic.LIVE_COMMANDS, Topic.LIVE_MESSAGES)
                        .build()))
                .build();
    }

    private static String buildHttpPushServerUri(final String hostName, final int port) {
        return "http://" + hostName + ":" + port;
    }

}
