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
package org.eclipse.ditto.testing.system.search.sync.common;

import org.eclipse.ditto.testing.system.search.sync.common.things.ThingConcurrentUpdatesIT;
import org.eclipse.ditto.testing.system.search.sync.common.things.ThingIndexViolationsIT;
import org.eclipse.ditto.testing.system.search.sync.common.things.ThingPolicyUpdateIT;
import org.eclipse.ditto.testing.system.search.sync.common.things.ThingUpdateIT;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Interface with common config for sync test suites.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        ThingConcurrentUpdatesIT.class,
        ThingIndexViolationsIT.class,
        ThingPolicyUpdateIT.class,
        ThingUpdateIT.class
})
@SuppressWarnings("squid:S1610") // Junit4 requires this class to be abstract, NOT an interface!
public abstract class SyncSuiteITBase {

}
