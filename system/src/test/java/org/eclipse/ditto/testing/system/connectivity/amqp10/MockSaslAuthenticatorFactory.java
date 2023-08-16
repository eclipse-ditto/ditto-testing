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
package org.eclipse.ditto.testing.system.connectivity.amqp10;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Transport;
import org.eclipse.ditto.connectivity.model.UserPasswordCredentials;

import org.apache.pekko.util.ByteString;
import io.vertx.core.Handler;
import io.vertx.core.net.NetSocket;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.sasl.ProtonSaslAuthenticator;
import io.vertx.proton.sasl.ProtonSaslAuthenticatorFactory;
import io.vertx.proton.sasl.impl.ProtonSaslAnonymousImpl;
import io.vertx.proton.sasl.impl.ProtonSaslPlainImpl;

/**
 * Mock SASL authentication factory that stores incoming SASL-PLAIN credentials in a queue for verification.
 */
final class MockSaslAuthenticatorFactory implements ProtonSaslAuthenticatorFactory {

    private final Queue<UserPasswordCredentials> authAttempts = new ConcurrentLinkedQueue<>();

    @Override
    public ProtonSaslAuthenticator create() {
        return new ProtonSaslAuthenticator() {

            private Sasl sasl;
            private boolean succeeded;

            @Override
            public void init(NetSocket socket, ProtonConnection protonConnection, Transport transport) {
                this.sasl = transport.sasl();
                sasl.server();
                sasl.allowSkip(false);
                sasl.setMechanisms(ProtonSaslPlainImpl.MECH_NAME, ProtonSaslAnonymousImpl.MECH_NAME);
                succeeded = false;
            }

            @Override
            public void process(Handler<Boolean> completionHandler) {
                if (sasl == null) {
                    throw new IllegalStateException("Init was not called with the associated transport");
                }

                boolean done = false;
                String[] remoteMechanisms = sasl.getRemoteMechanisms();
                if (remoteMechanisms.length > 0) {
                    String chosen = remoteMechanisms[0];
                    if (ProtonSaslPlainImpl.MECH_NAME.equals(chosen)) {
                        final byte[] saslResponse = new byte[sasl.pending()];
                        sasl.recv(saslResponse, 0, saslResponse.length);
                        authAttempts.add(parseUserPasswordCredentials(saslResponse));
                    }
                    sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                    succeeded = true;
                    done = true;
                }

                completionHandler.handle(done);
            }

            @Override
            public boolean succeeded() {
                return succeeded;
            }
        };
    }

    Queue<UserPasswordCredentials> getAuthAttempts() {
        return authAttempts;
    }

    private static UserPasswordCredentials parseUserPasswordCredentials(final byte[] saslResponse) {
        if (saslResponse.length < 2) {
            throw badSaslResponse(saslResponse);
        }
        final byte delimiter = saslResponse[0];
        int nextDelimiter = 1;
        while (nextDelimiter < saslResponse.length && saslResponse[nextDelimiter] != delimiter) {
            ++nextDelimiter;
        }
        if (nextDelimiter >= saslResponse.length) {
            throw badSaslResponse(saslResponse);
        }
        final String username = new String(Arrays.copyOfRange(saslResponse, 1, nextDelimiter));
        final String password = new String(Arrays.copyOfRange(saslResponse, nextDelimiter + 1, saslResponse.length));
        return UserPasswordCredentials.newInstance(username, password);
    }

    private static AssertionError badSaslResponse(final byte[] saslResponse) {
        return new AssertionError("Expect SASL-PLAIN response, got " + saslResponse.length +
                "bytes: " + ByteString.fromArray(saslResponse));
    }
}
