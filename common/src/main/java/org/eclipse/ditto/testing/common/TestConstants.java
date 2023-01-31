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
package org.eclipse.ditto.testing.common;

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.Source;
import org.eclipse.ditto.connectivity.model.SshTunnel;
import org.eclipse.ditto.connectivity.model.UserPasswordCredentials;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectType;

/**
 * This class contains frequently used constants for testing.
 */
public final class TestConstants {

    /**
     * API of the Things service version 2.
     */
    public static final int API_V_2 = JsonSchemaVersion.V_2.toInt();

    public static final String CONTENT_TYPE_APPLICATION_RAW_IMAGE = "application/raw+image";
    public static final String CONTENT_TYPE_APPLICATION_XML = "application/xml";
    public static final String CONTENT_TYPE_CUSTOM = "application/my-custom-content-type";
    public static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";
    public static final String CONTENT_TYPE_APPLICATION_JSON = "application/json";
    public static final String CONTENT_TYPE_APPLICATION_MERGE_PATCH_JSON = "application/merge-patch+json";
    public static final String CONTENT_TYPE_APPLICATION_JSON_UTF8 = CONTENT_TYPE_APPLICATION_JSON + "; charset=utf-8";
    public static final String CONTENT_TYPE_APPLICATION_OCTET_STREAM = "application/octet-stream";

    public static final String REQUEST_SUBJECT_ID_PLACEHOLDER = "{{ request:subjectId }}";
    public static final Subject REQUEST_SUBJECT =
            Subject.newInstance(REQUEST_SUBJECT_ID_PLACEHOLDER, SubjectType.GENERATED);

    /**
     * Policy related test constants.
     */
    public static final class Policy {

        public static final String THING_RESOURCE_TYPE = "thing";
        public static final SubjectType ARBITRARY_SUBJECT_TYPE = SubjectType.newInstance("arbitrarySubjectType");
        public static final SubjectType DITTO_AUTH_SUBJECT_TYPE = SubjectType.newInstance("ditto-auth");
        public static final JsonObject DEFAULT_POLICY = JsonObject.of(
                "{\n" +
                        "  \"entries\": {\n" +
                        "    \"DEFAULT\": {\n" +
                        "      \"subjects\": {\n" +
                        "        \"{{ request:subjectId }}\": {\n" +
                        "          \"type\": \"the creator\"\n" +
                        "        }\n" +
                        "      },\n" +
                        "      \"resources\": {\n" +
                        "        \"policy:/\": {\n" +
                        "          \"grant\": [\n" +
                        "            \"READ\",\n" +
                        "            \"WRITE\"\n" +
                        "          ],\n" +
                        "          \"revoke\": []\n" +
                        "        },\n" +
                        "        \"thing:/\": {\n" +
                        "          \"grant\": [\n" +
                        "            \"READ\",\n" +
                        "            \"WRITE\"\n" +
                        "          ],\n" +
                        "          \"revoke\": []\n" +
                        "        },\n" +
                        "        \"message:/\": {\n" +
                        "          \"grant\": [\n" +
                        "            \"READ\",\n" +
                        "            \"WRITE\"\n" +
                        "          ],\n" +
                        "          \"revoke\": []\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}");

        private Policy() {
            throw new AssertionError();
        }

    }

    public static final class Connections {

        public static JsonObject buildConnection() {
            return buildConnection("0", "testConnection");
        }

        public static JsonObject buildConnection(final String integrationName, final String connectionName) {
            return JsonObject.newBuilder()
                    .set(Connection.JsonFields.NAME, connectionName)
                    .set(Connection.JsonFields.CONNECTION_TYPE, "amqp-091")
                    .set(Connection.JsonFields.CONNECTION_STATUS, "closed")
                    .set(Connection.JsonFields.FAILOVER_ENABLED, false)
                    .set(Connection.JsonFields.URI, "amqp://127.0.0.1:5672")
                    .set(Connection.JsonFields.VALIDATE_CERTIFICATES, false)
                    .set(Connection.JsonFields.CLIENT_COUNT, 1)
                    .set(Connection.JsonFields.SOURCES,
                            JsonArray.of(JsonObject.newBuilder()
                                    .set(Source.JsonFields.ADDRESSES, JsonArray.of(JsonValue.of("telemetry/tenant")))
                                    .set(Source.JsonFields.CONSUMER_COUNT, 1)
                                    .set(Source.JsonFields.AUTHORIZATION_CONTEXT,
                                            JsonArray.of(JsonValue.of("integration:" + integrationName + ":" +
                                                    TestingContext.DEFAULT_SCOPE)))
                                    .build()))
                    .build();
        }

        public static JsonObject buildConnectionWithSshTunnel(final String connectionName) {

            return buildConnection("0", connectionName).toBuilder()
                    .set(Connection.JsonFields.SSH_TUNNEL, sshTunnelTemplate())
                    .build();

        }

        public static JsonObject sshTunnelTemplate() {
            return JsonObject.newBuilder()
                    .set(SshTunnel.JsonFields.ENABLED, false)
                    .set(SshTunnel.JsonFields.CREDENTIALS, JsonObject.newBuilder()
                            .set(UserPasswordCredentials.JsonFields.TYPE, UserPasswordCredentials.TYPE)
                            .set(UserPasswordCredentials.JsonFields.USERNAME, "someUser")
                            .set(UserPasswordCredentials.JsonFields.PASSWORD, "somePassword")
                            .build())
                    .set(SshTunnel.JsonFields.VALIDATE_HOST, false)
                    .set(SshTunnel.JsonFields.KNOWN_HOSTS, JsonArray.of(JsonValue.of("someFingerprint")))
                    .set(SshTunnel.JsonFields.URI, "ssh://ssh:2222")
                    .build();
        }
    }

    public static final class Messages {

        private static final String MESSAGES_PATH_API_2 = "/api/" + API_V_2;

        public static final String MESSAGES_API_2_BASE_URL =
                CommonTestConfig.getInstance().getGatewayUrl(MESSAGES_PATH_API_2);
        public static final String INBOX_PATH = "/inbox";

        public static final String MESSAGES_PATH = "/messages";
    }

    public static final class Things {

        public static final String THINGS_PATH = "/things";
        public static final String FEATURES_PATH = "/features";

    }

    /**
     * Inhibit instantiation as this is a utility class.
     */
    private TestConstants() {
        throw new AssertionError();
    }

}
