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
package org.eclipse.ditto.testing.common.client.ditto_protocol;

import java.util.concurrent.CompletableFuture;

import org.eclipse.ditto.base.model.signals.commands.Command;
import org.eclipse.ditto.base.model.signals.commands.CommandResponse;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.testing.common.client.ditto_protocol.options.Option;

/**
 * A small, reusable, simple but powerful interface to send messages to Ditto/Things.
 * Currently, there is no implementation for every connection-type, but they could follow.
 * Creating another interface, e.g. for events or for targets, might be better than blowing this one up.
 */
public interface DittoProtocolClient {

    /**
     * Connects the client.
     */
    void connect();

    /**
     * Sends a Ditto protocol message to Ditto/Things in an async fashion.
     *
     * @param adaptable adaptable to send.
     * @return the response, wrapped in a future.
     */
    CompletableFuture<CommandResponse<?>> sendAdaptable(Adaptable adaptable, Option<?>... options);

    /**
     * Sends a Ditto protocol command to Ditto/Things in an async fashion.
     *
     * @param command the command to send.
     * @return the response, wrapped in a future.
     */
    CompletableFuture<CommandResponse<?>> send(Command<?> command, Option<?>... options);

    /**
     * Disconnects and tears downs the client.
     */
    void disconnect();

}
