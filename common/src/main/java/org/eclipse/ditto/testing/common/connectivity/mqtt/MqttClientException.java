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
package org.eclipse.ditto.testing.common.connectivity.mqtt;

/**
 * This exception is thrown to indicate that an error occurred within a MQTT client.
 */
public final class MqttClientException extends RuntimeException {

    private static final long serialVersionUID = 2317632725658296700L;

    public MqttClientException(final String message) {
        super(message);
    }

    public MqttClientException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
