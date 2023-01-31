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
package org.eclipse.ditto.testing.system.connectivity.httppush;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.testing.common.categories.RequireSource;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * System tests for HTTP-push connections.
 */
@RunIf(DockerEnvironment.class)
@RunWith(Categories.class)
@Categories.ExcludeCategory(RequireSource.class)
@Suite.SuiteClasses(HttpPushConnectivitySuite.class)
@NotThreadSafe
public final class HttpPushConnectivityIT {}

