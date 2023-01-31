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
package org.eclipse.ditto.testing.system.connectivity;

import org.eclipse.ditto.testing.common.CommonTestConfig;
import org.eclipse.ditto.testing.common.ResourceUtil;

public final class ConnectivityTestConfig extends CommonTestConfig {

    private static final String CONNECTIVITY_PREFIX = "connectivity.";
    private static final String PROPERTY_CONNECTIVITY_RABBITMQ_PORT = CONNECTIVITY_PREFIX + "rabbitmq.port";
    private static final String PROPERTY_CONNECTIVITY_AMQP10_HOSTNAME = CONNECTIVITY_PREFIX + "amqp10.hostname";
    private static final String PROPERTY_CONNECTIVITY_AMQP10_TUNNEL = CONNECTIVITY_PREFIX + "amqp10.tunnel";
    private static final String PROPERTY_CONNECTIVITY_AMQP10_PORT = CONNECTIVITY_PREFIX + "amqp10.port";
    private static final String PROPERTY_CONNECTIVITY_AMQP10_HONO_HOSTNAME = CONNECTIVITY_PREFIX + "amqp10.hono.hostname";
    private static final String PROPERTY_CONNECTIVITY_AMQP10_HONO_TUNNEL = CONNECTIVITY_PREFIX + "amqp10.hono.tunnel";
    private static final String PROPERTY_CONNECTIVITY_AMQP10_HONO_PORT = CONNECTIVITY_PREFIX + "amqp10.hono.port";
    private static final String PROPERTY_CONNECTIVITY_AMQP10_HONO_CONGESTION_PORT =
            CONNECTIVITY_PREFIX + "amqp10.hono.congestion-port";
    private static final String PROPERTY_CONNECTIVITY_HTTP_HOSTNAME = CONNECTIVITY_PREFIX + "http.hostname";
    private static final String PROPERTY_CONNECTIVITY_HTTP_TUNNEL = CONNECTIVITY_PREFIX + "http.tunnel";
    private static final String PROPERTY_CONNECTIVITY_MQTT_HOSTNAME = CONNECTIVITY_PREFIX + "mqtt.hostname";
    private static final String PROPERTY_CONNECTIVITY_MQTT_TUNNEL = CONNECTIVITY_PREFIX + "mqtt.tunnel";
    private static final String PROPERTY_CONNECTIVITY_MQTT_PORT_TCP = CONNECTIVITY_PREFIX + "mqtt.port.tcp";
    private static final String PROPERTY_CONNECTIVITY_MQTT_PORT_SSL = CONNECTIVITY_PREFIX + "mqtt.port.ssl";
    private static final String PROPERTY_CONNECTIVITY_KAFKA_PORT = CONNECTIVITY_PREFIX + "kafka.port";
    private static final String PROPERTY_CONNECTIVITY_KAFKA_HOSTNAME = CONNECTIVITY_PREFIX + "kafka.hostname";
    private static final String PROPERTY_CONNECTIVITY_KAFKA_USERNAME = CONNECTIVITY_PREFIX + "kafka.username";
    private static final String PROPERTY_CONNECTIVITY_KAFKA_PASSWORD = CONNECTIVITY_PREFIX + "kafka.password";
    private static final String PROPERTY_CONNECTIVITY_SSH_HOSTNAME = CONNECTIVITY_PREFIX + "ssh.hostname";
    private static final String PROPERTY_CONNECTIVITY_SSH_PORT = CONNECTIVITY_PREFIX + "ssh.port";
    private static final String PROPERTY_CONNECTIVITY_SSH_USERNAME = CONNECTIVITY_PREFIX + "ssh.username";
    private static final String PROPERTY_CONNECTIVITY_SSH_PASSWORD = CONNECTIVITY_PREFIX + "ssh.password";
    private static final String PROPERTY_CONNECTIVITY_SSH_FINGERPRINT = CONNECTIVITY_PREFIX + "ssh.fingerprint";

    private static final ConnectivityTestConfig INSTANCE = new ConnectivityTestConfig();

    private ConnectivityTestConfig() {}

    public static ConnectivityTestConfig getInstance() {
        return INSTANCE;
    }

    @Override
    public int getRabbitMqPort() {
        return conf.getInt(PROPERTY_CONNECTIVITY_RABBITMQ_PORT);
    }

    public String getAmqp10HostName() {
        return conf.getString(PROPERTY_CONNECTIVITY_AMQP10_HOSTNAME);
    }

    public String getAmqp10Tunnel() {
        return conf.getString(PROPERTY_CONNECTIVITY_AMQP10_TUNNEL);
    }

    public int getAmqp10Port() {
        return conf.getInt(PROPERTY_CONNECTIVITY_AMQP10_PORT);
    }

    public String getAmqp10HonoHostName() {
        return conf.getString(PROPERTY_CONNECTIVITY_AMQP10_HONO_HOSTNAME);
    }

    public String getAmqp10HonoTunnel() {
        return conf.getString(PROPERTY_CONNECTIVITY_AMQP10_HONO_TUNNEL);
    }

    public int getAmqp10HonoPort() {
        return conf.getInt(PROPERTY_CONNECTIVITY_AMQP10_HONO_PORT);
    }

    public int getAmqp10HonoCongestionPort() {
        return conf.getInt(PROPERTY_CONNECTIVITY_AMQP10_HONO_CONGESTION_PORT);
    }

    public String getHttpHostName() {
        return conf.getString(PROPERTY_CONNECTIVITY_HTTP_HOSTNAME);
    }

    public String getHttpTunnel() {
        return conf.getString(PROPERTY_CONNECTIVITY_HTTP_TUNNEL);
    }

    public String getMqttHostName() {
        return conf.getString(PROPERTY_CONNECTIVITY_MQTT_HOSTNAME);
    }

    public String getMqttTunnel() {
        return conf.getString(PROPERTY_CONNECTIVITY_MQTT_TUNNEL);
    }

    public int getMqttPortTcp() {
        return conf.getInt(PROPERTY_CONNECTIVITY_MQTT_PORT_TCP);
    }

    public int getMqttPortSsl() {
        return conf.getInt(PROPERTY_CONNECTIVITY_MQTT_PORT_SSL);
    }

    public String getMqttCACrt() {
        return ResourceUtil.getResource("mqtt/ca.crt");
    }

    public String getMqttClientCrt() {
        return ResourceUtil.getResource("mqtt/client.crt");
    }

    public String getMqttClientKey() {
        return ResourceUtil.getResource("mqtt/client.key");
    }

    public String getMqttUnsignedCrt() {
        return ResourceUtil.getResource("mqtt/unsigned.crt");
    }

    public String getMqttUnsignedKey() {
        return ResourceUtil.getResource("mqtt/unsigned.key");
    }

    public String getKafkaHostname() {
        return conf.getString(PROPERTY_CONNECTIVITY_KAFKA_HOSTNAME);
    }

    public String getKafkaUsername() {
        return conf.getString(PROPERTY_CONNECTIVITY_KAFKA_USERNAME);
    }

    public String getKafkaPassword() {
        return conf.getString(PROPERTY_CONNECTIVITY_KAFKA_PASSWORD);
    }

    public int getKafkaPort() {
        return conf.getInt(PROPERTY_CONNECTIVITY_KAFKA_PORT);
    }

    public String getSshHostname() {
        return conf.getString(PROPERTY_CONNECTIVITY_SSH_HOSTNAME);
    }

    public int getSshPort() {
        return conf.getInt(PROPERTY_CONNECTIVITY_SSH_PORT);
    }

    public String getSshUsername() {
        return conf.getString(PROPERTY_CONNECTIVITY_SSH_USERNAME);
    }

    public String getSshPassword() {
        return conf.getString(PROPERTY_CONNECTIVITY_SSH_PASSWORD);
    }

    public String getSshFingerprint() {
        return conf.getString(PROPERTY_CONNECTIVITY_SSH_FINGERPRINT);
    }

    public String getTunnelPrivateKey() {return ResourceUtil.getResource("tunnel/id_rsa.pk8");}

    public String getTunnelPublicKey() {return ResourceUtil.getResource("tunnel/id_rsa.pub");}
}
