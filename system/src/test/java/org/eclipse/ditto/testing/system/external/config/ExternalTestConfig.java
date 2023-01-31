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
package org.eclipse.ditto.testing.system.external.config;

import org.eclipse.ditto.testing.common.CommonTestConfig;

public final class ExternalTestConfig extends CommonTestConfig {

    // IoT Hub
    private static final String HUB_PREFIX = "hub.";
    private static final String PROPERTY_HUB_TENANT = HUB_PREFIX + "tenant";
    private static final String PROPERTY_HUB_SERVICE_INSTANCE_ID = HUB_PREFIX + "serviceInstanceId";

    private static final String HUB_DEVICE_REGISTRY_PREFIX = HUB_PREFIX + "device-registry.";
    private static final String HUB_DEVICE_REGISTRY_SUITE_AUTH_PREFIX = HUB_DEVICE_REGISTRY_PREFIX + "suite-auth.";
    private static final String PROPERTY_HUB_DEVICE_REGISTRY_SUITE_AUTH_TOKEN_ENDPOINT = HUB_DEVICE_REGISTRY_SUITE_AUTH_PREFIX + "tokenEndpoint";
    private static final String PROPERTY_HUB_DEVICE_REGISTRY_SUITE_AUTH_CLIENT_ID = HUB_DEVICE_REGISTRY_SUITE_AUTH_PREFIX + "clientId";
    private static final String PROPERTY_HUB_DEVICE_REGISTRY_SUITE_AUTH_CLIENT_SECRET = HUB_DEVICE_REGISTRY_SUITE_AUTH_PREFIX + "clientSecret";
    private static final String PROPERTY_HUB_DEVICE_REGISTRY_SUITE_AUTH_SCOPE = HUB_DEVICE_REGISTRY_SUITE_AUTH_PREFIX + "scope";

    private static final String PROPERTY_HUB_MESSAGING_PREFIX = HUB_PREFIX + "messaging.";
    private static final String PROPERTY_HUB_MESSAGING_USERNAME = PROPERTY_HUB_MESSAGING_PREFIX + "username";
    private static final String PROPERTY_HUB_MESSAGING_PASSWORD = PROPERTY_HUB_MESSAGING_PREFIX + "password";

    // Azure Service Bus
    private static final String SERVICE_BUS_PREFIX = "servicebus.";
    private static final String PROPERTY_SERVICE_BUS_USERNAME = SERVICE_BUS_PREFIX + "username";
    private static final String PROPERTY_SERVICE_BUS_PASSWORD = SERVICE_BUS_PREFIX + "password";

    private static final ExternalTestConfig INSTANCE = new ExternalTestConfig();

    private ExternalTestConfig() {
        super();
    }

    public static ExternalTestConfig getInstance() {
        return INSTANCE;
    }

    public String getHubTenantId() {
        return conf.getString(PROPERTY_HUB_TENANT);
    }

    public String getHubServiceInstanceId() {
        return conf.getString(PROPERTY_HUB_SERVICE_INSTANCE_ID);
    }

    public String getHubSuiteAuthTokenEndpoint() {
        return conf.getString(PROPERTY_HUB_DEVICE_REGISTRY_SUITE_AUTH_TOKEN_ENDPOINT);
    }

    public String getHubSuiteAuthClientId() {
        return conf.getString(PROPERTY_HUB_DEVICE_REGISTRY_SUITE_AUTH_CLIENT_ID);
    }

    public String getHubSuiteAuthClientSecret() {
        return conf.getString(PROPERTY_HUB_DEVICE_REGISTRY_SUITE_AUTH_CLIENT_SECRET);
    }

    public String getHubSuiteAuthScope() {
        return conf.getString(PROPERTY_HUB_DEVICE_REGISTRY_SUITE_AUTH_SCOPE);
    }

    public String getHubMessagingUsername() {
        return conf.getString(PROPERTY_HUB_MESSAGING_USERNAME);
    }

    public String getHubMessagingPassword() {
        return conf.getString(PROPERTY_HUB_MESSAGING_PASSWORD);
    }

    public String getServiceBusUsername() {
        return conf.getString(PROPERTY_SERVICE_BUS_USERNAME);
    }

    public String getServiceBusPassword() {
        return conf.getString(PROPERTY_SERVICE_BUS_PASSWORD);
    }
}
