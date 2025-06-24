/*
 * Copyright (c) 2025 Contributors to the Eclipse Foundation
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
package org.eclipse.ditto.testing.system.things.rest;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.api.Permission;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.testing.common.CommonTestConfig;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.client.BasicAuth;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class WotValidationConfigIT extends IntegrationTest {

    private static final String BASE_URL = "/devops/wot/config";
    private static final String DYNAMIC_CONFIG_SCOPE = "test-scope";

    private static ThingId testThingId;
    private static Policy testPolicy;
    private static final String THING_DEFINITION_URL = "https://eclipse-ditto.github.io/ditto-examples/wot/models/dimmable-colored-lamp-1.0.0.tm.jsonld";

    @BeforeClass
    public static void setup() {
        LOGGER.info("Setup thing, policy and wotValidationConfig for the test: WotValidationConfigIT");
        testThingId = ThingId.of("namespace.default:wotvalidationConfig");
        testPolicy = createPolicy();

        putThingWithPolicy(TestConstants.API_V_2, newThing(testThingId), testPolicy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
    }

    private static Thing newThing(final ThingId thingId) {
        return ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setDefinition(ThingsModelFactory.newDefinition(THING_DEFINITION_URL))
                .build();
    }

    private static Policy createPolicy() {
        final BasicAuth basicAuth = serviceEnv.getDefaultTestingContext().getBasicAuth();
        final Subjects subjects;
        if (basicAuth.isEnabled()) {
            subjects = Subjects.newInstance(Subject.newInstance(
                    SubjectIssuer.newInstance("nginx"), basicAuth.getUsername()));
        } else {
            subjects = Subjects.newInstance(
                    serviceEnv.getDefaultTestingContext().getOAuthClient().getSubject(),
                    serviceEnv.getTestingContext2().getOAuthClient().getSubject());
        }
        return Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubjects(subjects)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), Permission.WRITE, Permission.READ)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), Permission.WRITE, Permission.READ)
                .build();
    }

    private static JsonObject defaultValidationConfig() {
        return JsonFactory.newObjectBuilder()
                .set("enabled", true)
                .set("log-warning-instead-of-failing-api-calls", false)
                .set("thing", JsonFactory.newObjectBuilder()
                        .set("enforce", JsonFactory.newObjectBuilder()
                                .set("thing-description-modification", false)
                                .set("attributes", true)
                                .set("inbox-messages-input", true)
                                .set("inbox-messages-output", true)
                                .set("outbox-messages", true)
                                .build())
                        .set("forbid", JsonFactory.newObjectBuilder()
                                .set("non-modeled-inbox-messages", true)
                                .set("non-modeled-outbox-messages", true)
                                .build())
                        .build())
                .set("feature", JsonFactory.newObjectBuilder()
                        .set("enforce", JsonFactory.newObjectBuilder()
                                .set("feature-description-modification", false)
                                .set("presence-of-modeled-features", false)
                                .build())
                        .set("forbid", JsonFactory.newObjectBuilder()
                                .set("feature-description-deletion", false)
                                .set("non-modeled-outbox-messages", false)
                                .build())
                        .build())
                .build();
    }

    private static JsonObject dynamicValidationConfig() {
        return JsonFactory.newObjectBuilder()
                .set("scope-id", DYNAMIC_CONFIG_SCOPE)
                .set("validation-context", JsonFactory.newObjectBuilder()
                        .set("thing-definition-patterns", JsonFactory.newArrayBuilder()
                                .add("^https://eclipse-ditto.github.io/ditto-examples/wot/models/.*$")
                                .build())
                        .build())
                .set("config-overrides", JsonFactory.newObjectBuilder()
                        .set("enabled", true)
                        .set("log-warning-instead-of-failing-api-calls", true)
                        .build())
                .build();
    }

    @Test
    public void test01_createInitialConfig() {
        // Create initial config - should return CREATED since it doesn't exist
        put(CommonTestConfig.getInstance().getDevopsUrl(BASE_URL), defaultValidationConfig().toString())
                .withLogging(LOGGER, "wotValidationConfig")
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        // Verify the config was created
        get(CommonTestConfig.getInstance().getDevopsUrl(BASE_URL))
                .withLogging(LOGGER, "wotValidationConfig")
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(defaultValidationConfig()))
                .fire();
    }

    @Test
    public void test02_createNonCompliantThing_fail() {

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        putAttribute(TestConstants.API_V_2, testThingId, "foo", "\"test\"")
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    @Test
    public void test03_modifyExistingConfig() {
        // Modify existing config - should return NO_CONTENT since it exists
        JsonObject modifiedConfig = defaultValidationConfig().toBuilder()
                .set("log-warning-instead-of-failing-api-calls", true)
                .build();

        put(CommonTestConfig.getInstance().getDevopsUrl(BASE_URL), modifiedConfig.toString())
                .withLogging(LOGGER, "wotValidationConfig")
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // Verify the modification
        get(CommonTestConfig.getInstance().getDevopsUrl(BASE_URL))
                .withLogging(LOGGER, "wotValidationConfig")
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(JsonFactory.newKey("enabled")))
                .fire();
    }

    @Test
    public void test04_addDynamicConfig() {
        // Add dynamic config
        put(CommonTestConfig.getInstance().getDevopsUrl(BASE_URL + "/dynamicConfigs/" + DYNAMIC_CONFIG_SCOPE), dynamicValidationConfig().toString())
                .withLogging(LOGGER, "wotValidationConfig")
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // Verify dynamic config was added
        get(CommonTestConfig.getInstance().getDevopsUrl(BASE_URL + "/dynamicConfigs/" + DYNAMIC_CONFIG_SCOPE))
                .withLogging(LOGGER, "wotValidationConfig")
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(dynamicValidationConfig()))
                .fire();
    }

    @Test
    public void test07_getMergedConfig() {
        // Get the full config including dynamic configs
        get(CommonTestConfig.getInstance().getDevopsUrl(BASE_URL + "/merged"))
                .withLogging(LOGGER, "wotValidationConfig")
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(JsonFactory.newObjectBuilder()
                        .set("thing", JsonFactory.newObjectBuilder()
                                .set("enforce", JsonFactory.newObjectBuilder()
                                        .set("thing-description-modification", false)
                                        .build())
                                .build())
                        .build()))
                .expectingBody(contains(JsonFactory.newObjectBuilder()
                        .set("dynamic-config", JsonFactory.newArrayBuilder()
                                .add(JsonFactory.newObjectBuilder().set("scope-id", "ditto:static").build())
                                .add(JsonFactory.newObjectBuilder().set("scope-id", "ditto:static").build())
                                .add(JsonFactory.newObjectBuilder().set("scope-id", "test-scope").build())
                                .build())
                        .build()))
                .fire();
    }

    @Test
    public void test06_createNonCompliantThing_success() {

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        putAttribute(TestConstants.API_V_2, testThingId, "foo", "\"test\"")
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
    }
    @Test
    public void test07_getFullConfig() {
        // Get the full config including dynamic configs
        get(CommonTestConfig.getInstance().getDevopsUrl(BASE_URL))
                .withLogging(LOGGER, "wotValidationConfig")
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(JsonFactory.newKey("dynamic-config")))
                .fire();
    }

    @Test
    public void test09_deleteDynamicConfig() {
        // Delete the dynamic config
        delete(CommonTestConfig.getInstance().getDevopsUrl(BASE_URL + "/dynamicConfigs/" + DYNAMIC_CONFIG_SCOPE))
                .withLogging(LOGGER, "wotValidationConfig")
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // Verify dynamic config was deleted
        get(CommonTestConfig.getInstance().getDevopsUrl(BASE_URL + "/dynamicConfigs/" + DYNAMIC_CONFIG_SCOPE))
                .withLogging(LOGGER, "wotValidationConfig")
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void test10_deleteFullConfig() {
        // Delete the full config
        delete(CommonTestConfig.getInstance().getDevopsUrl(BASE_URL))
                .withLogging(LOGGER, "wotValidationConfig")
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // Verify full config was deleted
        get(CommonTestConfig.getInstance().getDevopsUrl(BASE_URL))
                .withLogging(LOGGER, "wotValidationConfig")
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }
}