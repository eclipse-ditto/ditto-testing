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
package org.eclipse.ditto.testing.system.connectivity.mqtt3;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.testing.common.categories.RequireProtocolHeaders;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * System tests for MQTT 3 connections.
 */
@RunIf(DockerEnvironment.class)
@RunWith(Categories.class)
@Suite.SuiteClasses(Mqtt3ConnectivitySuite.class)
@Categories.ExcludeCategory(RequireProtocolHeaders.class)
@NotThreadSafe
public final class Mqtt3ConnectivityIT {}
