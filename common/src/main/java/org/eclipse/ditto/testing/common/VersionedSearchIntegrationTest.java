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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Abstract base class for versioned search integration tests.
 */
@RunWith(Parameterized.class)
public abstract class VersionedSearchIntegrationTest extends SearchIntegrationTest {

    @Parameterized.Parameters(name = "v{0}")
    public static List<JsonSchemaVersion> apiVersions() {
        return getConfiguredApiVersions().orElse(apiVersion2);
    }

    private static JsonSchemaVersion lastVersion = null;

    @Parameterized.Parameter
    public JsonSchemaVersion apiVersion;


    /**
     * Resets the lastVersion variable since all Tests use the same variable (JVM is started once for all tests).
     */
    @BeforeClass
    public static void resetLastSchemaVersion() {
        lastVersion = null;
    }

    @Override
    @Before
    public void before() {
        initOncePerParam();
    }

    /**
     * Initialize only once per parameter: We can *not* configure this as @BeforeClass annotation, because the
     * parameters will not yet be initialized.
     * <p>
     * Ideally, data creation and teardown should be annotated by @BeforeParam and @AfterParam. As of May 2017, these
     * annotations are only available for the snapshot version of JUnit. Consider splitting this into a @BeforeParam
     * createTestData method and an @AfterParam teardown method once the annotations are released.
     * <p>
     * Relevant JUnit issues: https://github.com/junit-team/junit4/issues/45 https://github.com/junit-team/junit4/pull/1435
     */
    private void initOncePerParam() {
        if (lastVersion != apiVersion) {
            // for better local testing, clear the data also before the test (not only after)
            if (lastVersion != null) {
                deleteAllTestData(lastVersion);
            } else {
                deleteTestDataOfAllApiVersions();
            }
            lastVersion = apiVersion;

            /*
              Make sure that per param (version 1 or 2) new oauth clients are used: this is required because
              deleting the things from the search index takes some time and the results would include things from the
              tests for the previous version otherwise.
             */
            resetClientCredentialsEnvironment();

            createTestData();
        }
    }

    protected SearchMatcher searchThings() {
        return searchThings(apiVersion);
    }

    protected Set<ThingId> persistThingsAndWaitTillAvailable(final Function<Long, Thing> thingBuilder, final long n) {
        return persistThingsAndWaitTillAvailable(thingBuilder, n, apiVersion);
    }

    protected ThingId persistThingAndWaitTillAvailable(final Thing thing) {
        return persistThingAndWaitTillAvailable(thing, apiVersion,
                serviceEnv.getDefaultTestingContext().getOAuthClient());
    }

    private static Optional<List<JsonSchemaVersion>> getConfiguredApiVersions() {
        return TEST_CONFIG.getSearchVersions();
    }

}
