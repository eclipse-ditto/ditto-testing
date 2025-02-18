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

import static java.util.Objects.requireNonNull;
import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.asynchttpclient.proxy.ProxyServer;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonFieldDefinition;
import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.json.JsonKey;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonObjectBuilder;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.messages.model.MessageDirection;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.PolicyImport;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.Resources;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.testing.common.client.BasicAuth;
import org.eclipse.ditto.testing.common.client.ConnectionsClient;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.conditions.ConditionalRunRule;
import org.eclipse.ditto.testing.common.matcher.BodyContainsJsonKeysMatcher;
import org.eclipse.ditto.testing.common.matcher.BodyContainsJsonValueMatcher;
import org.eclipse.ditto.testing.common.matcher.BodyContainsOnlyExpectedJsonKeysMatcher;
import org.eclipse.ditto.testing.common.matcher.BodyContainsOnlyExpectedJsonValueMatcher;
import org.eclipse.ditto.testing.common.matcher.CharSequenceContainsMatcher;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.EmptyCollectionMatcher;
import org.eclipse.ditto.testing.common.matcher.EmptyJsonMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.HttpVerbMatcher;
import org.eclipse.ditto.testing.common.matcher.JsonKeyStrictMatcher;
import org.eclipse.ditto.testing.common.matcher.PatchMatcher;
import org.eclipse.ditto.testing.common.matcher.PostMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.testing.common.matcher.RestMatcherConfigurer;
import org.eclipse.ditto.testing.common.matcher.RestMatcherFactory;
import org.eclipse.ditto.testing.common.matcher.SatisfiesMatcher;
import org.eclipse.ditto.testing.common.matcher.ThingIdMatcher;
import org.eclipse.ditto.testing.common.matcher.ThingsIdsExactlyMatcher;
import org.eclipse.ditto.testing.common.matcher.ThingsIdsMatcher;
import org.eclipse.ditto.testing.common.util.LazySupplier;
import org.eclipse.ditto.testing.common.ws.ThingsWebsocketClient;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.runners.statements.FailOnTimeout;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.RestAssured;
import io.restassured.config.DecoderConfig;
import io.restassured.config.EncoderConfig;
import io.restassured.config.RedirectConfig;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.response.ResponseOptions;

/**
 * Base class for integration tests.
 */
public abstract class IntegrationTest {

    static {
        initAwaitility();
    }

    protected static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTest.class);

    private static final long GLOBAL_TIMEOUT_MS = Optional.ofNullable(System.getProperty("test.timeoutMs"))
            .map(Long::parseLong)
            .orElse(120_000L);

    protected static final CommonTestConfig TEST_CONFIG = CommonTestConfig.getInstance();

    private static final String WS_URL_TEMPLATE = "/ws/%d";

    public static final ConditionFactory AWAITILITY_DEFAULT_CONFIG = Awaitility.await().atMost(5, TimeUnit.SECONDS);

    protected static ServiceEnvironment serviceEnv;
    protected static IdGenerator defaultIdGenerator;

    private static ConnectionsClient connectionsClient;

    protected static RestMatcherConfigurer restMatcherConfigurer;
    protected static RestMatcherFactory restMatcherFactory;

    protected static final String FIELD_NAME_THING_ID = "thingId";

    protected static final int DEFAULT_MAX_THING_SIZE = 1024 * 100;
    protected static final int DEFAULT_MAX_POLICY_SIZE = 1024 * 100;

    private static final LazySupplier<Boolean> INITIALIZATION_ONCE = LazySupplier.fromScratch();

    private static Deque<HttpVerbMatcher<?>> cleanUpMatchers;
    private static Deque<Runnable> cleanUpActions;

    @ClassRule
    public static ErrorCollector errorCollector;

    private static final ConditionalRunRule CONDITIONAL_RUN_RULE = new ConditionalRunRule();
    /**
     * For evaluating {@link org.eclipse.ditto.testing.common.conditions.RunIf} on test-class level.
     */
    @ClassRule
    public static final ConditionalRunRule CONDITIONAL_RUN_CLASS_RULE = CONDITIONAL_RUN_RULE;
    /**
     * For evaluating {@link org.eclipse.ditto.testing.common.conditions.RunIf} on test-method level.
     */
    @Rule
    public ConditionalRunRule conditionalRunTestRule = CONDITIONAL_RUN_RULE;

    /**
     * Applies the global timeout only to tests which do not explicitly disable it or set it by
     * {@code @Test (timeout=..)}.
     */
    @Rule
    public TestWatcher timeoutWatcher = new TestWatcher() {
        @Override
        public Statement apply(final Statement base, final Description description) {
            final Timeout classLevelTimeout = description.getTestClass().getAnnotation(Timeout.class);
            if (classLevelTimeout != null) {
                return FailOnTimeout.builder()
                        .withTimeout(classLevelTimeout.millis(), TimeUnit.MILLISECONDS)
                        .build(base);
            }

            final Test testAnnotation = description.getAnnotation(Test.class);
            // 0 is the default value for timeout-property, which means no timeout
            if (testAnnotation == null || testAnnotation.timeout() == 0L) {
                return FailOnTimeout.builder().withTimeout(GLOBAL_TIMEOUT_MS, TimeUnit.MILLISECONDS).build(base);
            } else {
                return base;
            }
        }
    };

    @Rule
    public final TestWatcher watchman = new TestedMethodLoggingWatcher(LOGGER);

    @BeforeClass
    public static void setUpEnvironment() {
        INITIALIZATION_ONCE.get(() -> {
            initOnce();
            return true;
        });

        setUpRestMatchers();
    }

    @BeforeClass
    public static void logUrlBeforeTests() {
        LOGGER.info("Use Ditto URL: {}", dittoUrl(TestConstants.API_V_2));
    }

    @BeforeClass
    public static void initCleanUpActions() {
        errorCollector = new ErrorCollector();
        cleanUpMatchers = new LinkedBlockingDeque<>();
        cleanUpActions = new LinkedBlockingDeque<>();
    }

    @AfterClass
    public static void performCleanUpActions() {
        LOGGER.info("Performing clean up.");
        // reverse cleanup action order so that things are deleted before their policies.
        cleanUpMatchers.forEach(cleanUpMatcher -> errorCollector.checkSucceeds(cleanUpMatcher::fire));
        cleanUpActions.forEach(Runnable::run);
        discardCleanupActions();
    }

    private static void initOnce() {
        LOGGER.info("Init Once");
        initAwaitility();
        setUpRestAssured();
        serviceEnv = ServiceEnvironment.from(TEST_CONFIG);
        defaultIdGenerator = IdGenerator.fromNamespace(serviceEnv.getDefaultNamespaceName());
        connectionsClient = ConnectionsClient.getInstance();
    }

    private static void initAwaitility() {
        /* is necessary because of the workaround in SatisfiesMatcher (AssertionErrorWrappingException instead of
           AssertionError) */
        Awaitility.ignoreExceptionsByDefault();
    }

    private static void setUpRestAssured() {
        RestAssured.config = RestAssured.config()
                .encoderConfig(EncoderConfig.encoderConfig()
                        .defaultContentCharset("UTF-8")
                        .appendDefaultContentCharsetToContentTypeIfUndefined(false)
                        .encodeContentTypeAs(TestConstants.CONTENT_TYPE_APPLICATION_XML, ContentType.TEXT)
                        .encodeContentTypeAs(TestConstants.CONTENT_TYPE_APPLICATION_RAW_IMAGE, ContentType.TEXT))
                .decoderConfig(DecoderConfig.decoderConfig().defaultContentCharset("UTF-8"))
                .redirect(RedirectConfig.redirectConfig().followRedirects(false));
        RestAssured.useRelaxedHTTPSValidation();
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

        if (TEST_CONFIG.isHttpProxyEnabled()) {
            RestAssured.proxy(TEST_CONFIG.getHttpProxyHost(), TEST_CONFIG.getHttpProxyPort());
        }
    }

    private static void setUpRestMatchers() {
        final BasicAuth basicAuth = serviceEnv.getDefaultTestingContext().getBasicAuth();
        if (basicAuth.isEnabled()) {
            restMatcherConfigurer = RestMatcherConfigurer.withBasicAuth(basicAuth);
        } else {
            final String jwt = serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken();
            restMatcherConfigurer = RestMatcherConfigurer.withJwt(jwt);
        }
        restMatcherFactory = new RestMatcherFactory(restMatcherConfigurer);
    }

    protected static void resetClientCredentialsEnvironment() {
        setUpRestMatchers();
    }

    /**
     * Discard all accumulated cleanup actions.
     * Subclasses should call this if they perform actions invalidating default cleanup actions such as deleting
     * solutions with which things and policies are created.
     */
    protected static void discardCleanupActions() {
        cleanUpMatchers.clear();
        cleanUpActions.clear();
    }

    protected static Policy newPolicy(final PolicyId policyId, final AuthClient... readAndWriteSubjects) {
        return newPolicy(policyId, Arrays.asList(readAndWriteSubjects), Arrays.asList(readAndWriteSubjects));
    }

    protected static Policy newPolicy(final PolicyId policyId, final Subject readAndWriteSubject) {
        final Subjects readAndWriteSubjects = Subjects.newInstance(readAndWriteSubject);
        return newPolicy(policyId, readAndWriteSubjects, readAndWriteSubjects);
    }

    protected static Policy newPolicy(final PolicyId policyId, final List<AuthClient> readClients,
            final List<AuthClient> writeClients) {

        final Subjects readSubjects = toSubjects(readClients);
        final Subjects writeSubjects = toSubjects(writeClients);

        return newPolicy(policyId, readSubjects, writeSubjects);
    }

    protected static Policy newPolicy(final PolicyId policyId, final Subjects readSubjects,
            final Subjects writeSubjects) {
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("read")
                .setSubjects(readSubjects)
                .setGrantedPermissions("thing", "/", "READ")
                .setGrantedPermissions("policy", "/", "READ")
                .setGrantedPermissions("message", "/", "READ")
                .forLabel("write")
                .setSubjects(writeSubjects)
                .setGrantedPermissions("thing", "/", "WRITE")
                .setGrantedPermissions("policy", "/", "WRITE")
                .setGrantedPermissions("message", "/", "WRITE")
                .build();
    }

    protected static Subjects toSubjects(final List<AuthClient> authClients) {

        return PoliciesModelFactory.newSubjects(authClients.stream()
                .map(AuthClient::getSubject)
                .collect(Collectors.toList())
        );
    }

    /**
     * Returns a URL for the Ditto HTTP API for the given {@code version} and {@code path}.
     *
     * @param version the API version.
     * @param path the resource path.
     * @return the URL.
     */
    protected static String dittoUrl(final int version, final CharSequence path) {
        return CommonTestConfig.getInstance().getGatewayApiUrl(version, path);
    }

    /**
     * Creates a new {@link ThingsWebsocketClient}.
     *
     * @param testingContext the testing context to be used for authentication.
     * @param additionalHttpHeaders additional HTTP headers to set for the initial WS request.
     * @param apiVersion the API version to use for opening the WS.
     * @return the created ThingsWebsocketClient instance.
     */
    protected static ThingsWebsocketClient newTestWebsocketClient(final TestingContext testingContext,
            final Map<String, String> additionalHttpHeaders, final int apiVersion) {

        return newTestWebsocketClient(testingContext, additionalHttpHeaders, apiVersion,
                    ThingsWebsocketClient.AuthMethod.HEADER);
    }

    /**
     * Creates a new {@link ThingsWebsocketClient}.
     *
     * @param testingContext the testing context to be used for authentication.
     * @param additionalHttpHeaders additional HTTP headers to set for the initial WS request.
     * @param apiVersion the API version to use for opening the WS.
     * @param authMethod the testingContext auth method used.
     * @return the created ThingsWebsocketClient instance.
     */
    protected static ThingsWebsocketClient newTestWebsocketClient(final TestingContext testingContext,
            final Map<String, String> additionalHttpHeaders, final int apiVersion, final
    ThingsWebsocketClient.AuthMethod authMethod) {

        final ProxyServer proxyServer;
        if (TEST_CONFIG.isHttpProxyEnabled()) {
            proxyServer = new ProxyServer.Builder(TEST_CONFIG.getHttpProxyHost(), TEST_CONFIG.getHttpProxyPort())
                    .build();
        } else {
            proxyServer = null;
        }

        return ThingsWebsocketClient.newInstance(dittoWsUrl(apiVersion), testingContext,
                additionalHttpHeaders, proxyServer,
                authMethod);
    }

    /**
     * Returns a URL for the Ditto HTTP API for the given {@code version}.
     *
     * @param version the API version.
     * @return the URL.
     */
    protected static String dittoUrl(final int version) {
        return dittoUrl(version, "");
    }

    /**
     * Returns a URL for the Ditto WS API for the given {@code version}.
     *
     * @param version the API version.
     * @return the URL.
     */
    protected static String dittoWsUrl(final int version) {
        return CommonTestConfig.getInstance().getWsUrl(String.format(WS_URL_TEMPLATE, version));
    }

    /**
     * Returns a matcher for checking if the JSON of the response body contains the given JSON value.
     * The check is not recursive, i. e. only the top level values are compared.
     *
     * @param jsonValue the JSON value expected to be contained in the response body JSON at top level.
     * @return a matcher for checking if the JSON of the response body contains {@code jsonValue}.
     * @throws NullPointerException if {@code jsonValue} is {@code null}.
     */
    public static Matcher<String> contains(final JsonValue jsonValue) {
        return new BodyContainsJsonValueMatcher(jsonValue);
    }

    /**
     * Returns a {@link Matcher} for checking if the JSON of a response expectingBody contains the expected
     * {@code jsonKey}.
     *
     * @param jsonKey the expected JSON key.
     * @param furtherJsonKeys further expected JSON keys.
     * @return the matcher.
     */
    public static Matcher<String> contains(final JsonKey jsonKey, final JsonKey... furtherJsonKeys) {
        final Set<JsonKey> expectedJsonKeys = new HashSet<>(1 + furtherJsonKeys.length);
        expectedJsonKeys.add(jsonKey);
        Collections.addAll(expectedJsonKeys, furtherJsonKeys);
        return new BodyContainsJsonKeysMatcher(expectedJsonKeys);
    }

    /**
     * Returns a {@link Matcher} for checking if the JSON of a response expectingBody contains the expected
     * {@code jsonKey}.
     *
     * @param furtherJsonKeys expected JSON keys.
     * @return the matcher.
     */
    public static Matcher<String> contains(final List<JsonKey> furtherJsonKeys) {
        return new BodyContainsJsonKeysMatcher(Set.copyOf(furtherJsonKeys));
    }

    /**
     * Returns a {@link Matcher} for checking if the JSON expectingBody of a request is an empty array.
     *
     * @return a matcher for checking if the JSON request expectingBody is an empty array.
     */
    protected static Matcher<String> containsEmptyCollection() {
        return new EmptyCollectionMatcher();
    }

    /**
     * Returns a {@link Matcher} for checking if the JSON object is empty.
     *
     * @return a matcher for checking if the JSON object is empty.
     */
    protected static Matcher<String> containsEmptyJson() {
        return new EmptyJsonMatcher();
    }

    /**
     * Returns a {@code Matcher} for checking if the JSON of a response contains the expected JSON fields.
     *
     * @param expectedJsonFieldDefinitions the expected JSON fields.
     * @return the matcher.
     */
    protected static Matcher<String> containsOnly(
            final List<? extends JsonFieldDefinition<?>> expectedJsonFieldDefinitions
    ) {
        return new BodyContainsOnlyExpectedJsonKeysMatcher(expectedJsonFieldDefinitions);
    }

    /**
     * Returns a {@code Matcher} for checking if the JSON of a response contains the expected JSON fields.
     *
     * @param expectedJsonValue the expected JSON value
     * @return the matcher.
     */
    protected static Matcher<String> containsOnly(final JsonValue expectedJsonValue) {
        return new BodyContainsOnlyExpectedJsonValueMatcher(expectedJsonValue);
    }

    /**
     * Returns a {@link Matcher} for checking if the JSON object only contains the specified key or keys.
     *
     * @return the matcher.
     */
    protected static Matcher<String> containsOnlyJsonKey(final CharSequence jsonKey,
            @Nonnull final CharSequence... furtherJsonKeys) {

        final List<String> jsonKeys = new ArrayList<>(1 + furtherJsonKeys.length);
        jsonKeys.add(jsonKey.toString());
        for (final CharSequence furtherJsonKey : furtherJsonKeys) {
            jsonKeys.add(furtherJsonKey.toString());
        }
        return new JsonKeyStrictMatcher(jsonKeys, true);
    }

    /**
     * Returns a {@link Matcher} for checking if the JSON object only contains the specified keys at the top level.
     *
     * @return a matcher for checking if the JSON object only contains the specified key at the top level.
     */
    protected static Matcher<String> containsOnlyJsonKeysAtTopLevel(final CharSequence jsonKey,
            @Nonnull final CharSequence... furtherJsonKeys) {
        final List<String> jsonKeys = new ArrayList<>(1 + furtherJsonKeys.length);
        jsonKeys.add(jsonKey.toString());
        for (final CharSequence furtherJsonKey : furtherJsonKeys) {
            jsonKeys.add(furtherJsonKey.toString());
        }

        return new JsonKeyStrictMatcher(jsonKeys, false);
    }

    /**
     * Returns a {@link Matcher} for checking if the JSON of a response expectingBody contains the expected Thing ID.
     *
     * @param thingId the mandatory expected Thing ID.
     * @return a matcher for checking if a JSON string contains the expected Thing ID.
     */
    protected static Matcher<String> containsThingId(final ThingId thingId) {
        return new ThingIdMatcher(thingId);
    }

    /**
     * Returns a {@link Matcher} for checking if the JSON of a response expectingBody contains the expected Thing IDs in
     * an array format.
     *
     * @param furtherThingIds expected Thing IDs.
     * @return a matcher for checking if a JSON string contains the expected Thing IDs.
     */
    protected static Matcher<String> containsThingIds(final ThingId... furtherThingIds) {
        final List<ThingId> expectedThingsIds = new ArrayList<>(furtherThingIds.length);
        Collections.addAll(expectedThingsIds, furtherThingIds);

        return new ThingsIdsMatcher(expectedThingsIds, Lists.newArrayList());
    }

    /**
     * Returns a {@link Matcher} for checking if the JSON of a response expectingBody contains only the expected Thing
     * IDs in an array format in the given expected order.
     *
     * @param furtherThingIds expected Thing IDs.
     * @return a matcher for checking if a JSON string contains the expected Thing IDs.
     */
    public static Matcher<String> containsThingIdsExactly(final String... furtherThingIds) {
        final List<String> expectedThingsIds = new ArrayList<>(furtherThingIds.length);
        Collections.addAll(expectedThingsIds, furtherThingIds);

        return new ThingsIdsExactlyMatcher(expectedThingsIds);
    }

    /**
     * Returns a {@link Matcher} for checking if the JSON of a response expectingBody does not contain the expected
     * Thing IDs.
     *
     * @param furtherThingIds expected Thing IDs not to be contained.
     * @return a matcher for checking if a JSON string contains the expected Thing IDs.
     */
    protected static Matcher<String> doesNotContainThingIds(final ThingId... furtherThingIds) {
        final List<ThingId> notExpectedThingsIds = new ArrayList<>(furtherThingIds.length);
        Collections.addAll(notExpectedThingsIds, furtherThingIds);

        return new ThingsIdsMatcher(Lists.newArrayList(), notExpectedThingsIds);
    }

    /**
     * <p>Check that the given {@code requirements} are fulfilled.</p>
     * <p>Inspired by assertJ's method {@code org.assertj.core.api.AbstractAssert#satisfies(Consumer)}.</p>
     *
     * @param requirements the requirements as a {@link Consumer}
     * @return the matcher
     */
    protected static <T> Matcher<T> satisfies(final Consumer<T> requirements) {
        return new SatisfiesMatcher<>(requirements);
    }

    /**
     * Returns a {@link Matcher} for checking if the JSON of a response contains the expected {@code charSequence}.
     *
     * @param charSequence the expected char sequence.
     * @return a matcher for checking if a JSON string contains the expected char sequence.
     */
    protected static Matcher<String> containsCharSequence(final CharSequence charSequence) {
        return new CharSequenceContainsMatcher(charSequence);
    }

    /**
     * Parses the thing ID from a location header.
     *
     * @param location the location header.
     * @return the thing ID.
     */
    public static String parseIdFromLocation(final String location) {
        final List<String> locationStringTokens = Arrays.asList(location.split("/"));
        return locationStringTokens.get(locationStringTokens.size() - 1);
    }

    /**
     * Parses the ID from a given {@link Response}.
     *
     * @param response the response to parse the ID from.
     * @return the ID.
     */
    protected static String parseIdFromResponse(final ResponseOptions<Response> response) {
        return parseIdFromLocation(response.header("Location"));
    }

    /**
     * Wraps a get request for all Things of the Things Service.
     *
     * @return the wrapped request.
     */
    protected static GetMatcher getThings(final int version) {
        return get(dittoUrl(version, HttpResource.THINGS.getPath())).withLogging(LOGGER, "Things");
    }

    /**
     * Wraps a post request for the JSON representation of a generated Thing.
     *
     * @param version the API version.
     * @return the wrapped request.
     */
    public static PostMatcher postThing(final int version) {
        return postThing(version, new ThingJsonProducer().getJsonString());
    }

    /**
     * Wraps a post request for the the given JSON.
     *
     * @param version the API version.
     * @param jsonObject the JSON to post.
     * @return the wrapped request.
     */
    public static PostMatcher postThing(final int version, final JsonObject jsonObject) {
        return postThing(version, jsonObject.toString());
    }

    /**
     * Wraps a post request for the the given JSON.
     *
     * @param version the API version.
     * @param jsonString the JSON to post.
     * @return the wrapped request.
     */
    public static PostMatcher postThing(final int version, final String jsonString) {
        final String path = HttpResource.THINGS.getPath();
        final String thingsServiceUrl = dittoUrl(version, path);

        final PostMatcher result = post(thingsServiceUrl, jsonString).withLogging(LOGGER, "Thing");
        result.registerResponseConsumer(response -> {
            if (null == response) { // in case of a bad request (for example) the response can be null
                return;
            }
            final String location = response.getHeader("Location");
            if (null == location) {
                return;
            }

            final String thingId = parseIdFromLocation(location);
            rememberForCleanUp(deleteThing(version, thingId));
            String policyId;
            try {
                policyId =
                        JsonObject.of(response.getBody().asByteArray()).getValue("policyId").get().asString();
            } catch (final Exception ex) {
                LOGGER.warn("Could not parse policyId of Thing {} to remember for cleanup: {}", thingId, ex.getMessage());
                policyId = thingId;
            }
            rememberForCleanUpLast(deletePolicy(policyId));
        });
        return result;
    }

    /**
     * Wraps a post request for the JSON representation of a generated Thing.
     *
     * @param version the API version.
     * @param subject the subject to grant permissions.
     * @return the wrapped request.
     */
    protected static PostMatcher postThingWithAuthSubject(final int version, final Subject subject) {
        final ThingJsonProducer thingJsonProducer = new ThingJsonProducer();

        final String jsonString = thingJsonProducer
                .getJsonForV2WithInlinePolicy(subject, serviceEnv.getDefaultNamespaceName())
                .toString();

        return postThing(version, jsonString);
    }

    /**
     * Wraps a put request for the specified {@code thing} in {@code schemaVersion}.
     *
     * @param version the API version.
     * @param thing the Thing as JSON.
     * @param schemaVersion the JSON schema version.
     * @return the wrapped request.
     */
    public static PutMatcher putThing(final int version, final JsonObject thing,
            final JsonSchemaVersion schemaVersion) {

        checkThing(thing);
        checkSchemaVersion(schemaVersion);

        final String thingId = thing.getValueOrThrow(Thing.JsonFields.ID);

        rememberForCleanUp(deleteThing(version, thingId));
        rememberForCleanUpLast(deletePolicy(thingId));

        final String path = ResourcePathBuilder.forThing(thingId).toString();
        final String jsonString = thing.toString();
        final String thingsServiceUrl = dittoUrl(version, path);

        return put(thingsServiceUrl, jsonString).withLogging(LOGGER, "Thing");
    }

    private static void checkThing(final Object thing) {
        checkNotNull(thing, "Thing");
    }

    private static void checkSchemaVersion(final JsonSchemaVersion schemaVersion) {
        checkNotNull(schemaVersion, "JSON schema version");
    }

    /**
     * Wraps a put request for the specified {@code thing} with inlined {@code policy} in {@code schemaVersion}.
     *
     * @param version the API version.
     * @param thing the Thing.
     * @param policy the inlined Policy.
     * @param schemaVersion the JSON schema version.
     * @return the wrapped request.
     */
    public static PutMatcher putThingWithPolicy(final int version,
            final Thing thing,
            @Nullable final Policy policy,
            final JsonSchemaVersion schemaVersion) {

        checkThing(thing);
        checkSchemaVersion(schemaVersion);

        final ThingId thingId = thing.getEntityId()
                .orElseThrow(() -> new IllegalArgumentException("The Thing ID must be present!"));

        final String jsonString;
        final JsonObjectBuilder jsonObjectBuilder = thing.toJson(schemaVersion).toBuilder();
        if (policy != null) {
            final JsonFieldSelector jsonFieldSelector =
                    JsonFactory.newFieldSelector(Policy.JsonFields.ID, Policy.JsonFields.ENTRIES);
            jsonObjectBuilder.setAll(policy.toInlinedJson(schemaVersion, jsonFieldSelector));
            rememberForCleanUpLast(deletePolicy(policy.getEntityId()
                    .map(id -> (CharSequence) id)
                    .orElse(thingId)));
        } else {
            rememberForCleanUpLast(deletePolicy(thingId));
        }
        jsonString = jsonObjectBuilder.build().toString();

        rememberForCleanUp(deleteThing(version, thingId));

        final String path = ResourcePathBuilder.forThing(thingId).toString();
        final String thingsServiceUrl = dittoUrl(version, path);

        return put(thingsServiceUrl, jsonString).withLogging(LOGGER, "Thing");
    }

    /**
     * Wraps a put request for the specified {@code thing} in {@code schemaVersion}.
     *
     * @param version the API version
     * @param thing the Thing.
     * @param schemaVersion the JSON schema version.
     * @return the wrapped request.
     */
    public static PutMatcher putThing(final int version, final Thing thing, final JsonSchemaVersion schemaVersion) {
        return putThingWithPolicy(version, thing, null, schemaVersion);
    }


    /**
     * Wraps a patch request for the specified {@code thing}.
     *
     * @param path the path where the patch is applied.
     * @param value the value containing the patch in Json Merge Patch format.
     * @return the wrapped request.
     */
    public static PatchMatcher patchThing(final ThingId thingId, final JsonPointer path, final JsonValue value) {
        return patchThing(TestConstants.API_V_2, thingId, path, value);
    }

    /**
     * Wraps a patch request for the specified {@code thing}.
     *
     * @param version the API version
     * @param patchPath the path where the patch is applied.
     * @param value the value containing the patch in Json Merge Patch format.
     * @return the wrapped request.
     */
    public static PatchMatcher patchThing(final int version, final ThingId thingId, final JsonPointer patchPath,
            final JsonValue value) {
        return patchThing(version, TestConstants.CONTENT_TYPE_APPLICATION_MERGE_PATCH_JSON, thingId,
                patchPath, value);
    }

    /**
     * Wraps a patch request for the specified {@code thing}.
     *
     * @param version the API version
     * @param thingId the id of the Thing that is patched
     * @param contentType the content type of the request
     * @param patchPath the path where the patch is applied.
     * @param value the value containing the patch in Json Merge Patch format.
     * @return the wrapped request.
     */
    public static PatchMatcher patchThing(final int version, final CharSequence contentType, final ThingId thingId,
            final JsonPointer patchPath, final JsonValue value) {
        checkNotNull(thingId, "thingId");
        final String path =
                JsonPointer.of(ResourcePathBuilder.forThing(thingId).toString()).append(patchPath).toString();
        final String thingsServiceUrl = dittoUrl(version, path);
        return patch(thingsServiceUrl, contentType, value.toString()).withLogging(LOGGER, "Thing");
    }

    /**
     * Wraps a get request for a given Thing ID.
     *
     * @param thingId the Thing ID.
     * @return the wrapped request.
     */
    public static GetMatcher getThing(final int version, final CharSequence thingId) {
        final String path = ResourcePathBuilder.forThing(thingId).toString();
        final String thingsServiceUrl = dittoUrl(version, path);

        return get(thingsServiceUrl).withLogging(LOGGER, "Thing");
    }

    /**
     * Wraps a delete request for a given Thing ID.
     *
     * @param version the API version.
     * @param thingId the Thing ID.
     * @return the wrapped request.
     */
    public static DeleteMatcher deleteThing(final int version, final CharSequence thingId) {
        final String path = ResourcePathBuilder.forThing(thingId).toString();
        final String thingsServiceUrl = dittoUrl(version, path);

        return delete(thingsServiceUrl).withLogging(LOGGER, "Thing");
    }

    /**
     * Puts a Thing's Policy ID.
     *
     * @param thingId the ID of the Thing.
     * @param policyId the Policy ID to put
     * @return the wrapped request.
     */
    protected static PutMatcher putPolicyId(final CharSequence thingId, final CharSequence policyId) {
        final String path = ResourcePathBuilder.forThing(thingId).policyId().toString();

        return put(dittoUrl(TestConstants.API_V_2, path), JsonValue.of(policyId.toString()).toString())
                .withLogging(LOGGER, "Thing/PolicyId");
    }

    /**
     * Gets a Thing's Policy ID.
     *
     * @param thingId the ID of the Thing.
     * @return the wrapped request.
     */
    protected static GetMatcher getPolicyId(final CharSequence thingId) {
        final String path = ResourcePathBuilder.forThing(thingId).policyId().toString();

        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "Thing/PolicyId");
    }

    /**
     * Puts a Thing's Attributes.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @param jsonString the attributes.
     * @return the wrapped request.
     */
    protected static PutMatcher putAttributes(final int version, final CharSequence thingId, final String jsonString) {
        final String path = ResourcePathBuilder.forThing(thingId).attributes().toString();
        return put(dittoUrl(version, path), jsonString).withLogging(LOGGER, "Attributes");
    }

    /**
     * Gets the JSON Representation of a Thing's Attributes.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @return the wrapped request.
     */
    protected static GetMatcher getAttributes(final int version, final CharSequence thingId) {
        final String path = ResourcePathBuilder.forThing(thingId).attributes().toString();
        return get(dittoUrl(version, path)).withLogging(LOGGER, "Attributes");
    }

    /**
     * Deletes Thing's Attributes.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @return the wrapped request.
     */
    protected static DeleteMatcher deleteAttributes(final int version, final CharSequence thingId) {
        final String path = ResourcePathBuilder.forThing(thingId).attributes().toString();
        return delete(dittoUrl(version, path)).withLogging(LOGGER, "Attributes");
    }

    /**
     * Puts a JSON Representation of a Attribute on a Thing at a given JSON Pointer.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @param attributeJsonPointer the JSON Pointer.
     * @param jsonString the attribute.
     * @return the wrapped request.
     */
    protected static PutMatcher putAttribute(final int version,
            final CharSequence thingId,
            final CharSequence attributeJsonPointer,
            final String jsonString) {

        final String path = ResourcePathBuilder.forThing(thingId).attribute(attributeJsonPointer).toString();
        return put(dittoUrl(version, path), jsonString).withLogging(LOGGER, "Attribute");
    }

    /**
     * Gets the JSON Representation of an Attribute on a Thing at a given JSON Pointer.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @param attributeJsonPointer the JSON Pointer.
     * @return the wrapped request.
     */
    protected static GetMatcher getAttribute(final int version, final CharSequence thingId,
            final CharSequence attributeJsonPointer) {

        final String path = ResourcePathBuilder.forThing(thingId).attribute(attributeJsonPointer).toString();
        return get(dittoUrl(version, path)).withLogging(LOGGER, "Attribute");
    }

    /**
     * Deletes an Attribute on a Thing at a given JSON Pointer.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @param attributeJsonPointer the JSON Pointer.
     * @return the wrapped request.
     */
    protected static DeleteMatcher deleteAttribute(final int version, final CharSequence thingId,
            final CharSequence attributeJsonPointer) {

        final String path = ResourcePathBuilder.forThing(thingId).attribute(attributeJsonPointer).toString();
        return delete(dittoUrl(version, path)).withLogging(LOGGER, "Attribute");
    }

    /**
     * Puts a Thing's definition.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @param jsonString the JSON Representation of the definition.
     * @return the wrapped request.
     */
    protected static PutMatcher putDefinition(final int version,
            final CharSequence thingId,
            final String jsonString) {

        final String path = ResourcePathBuilder.forThing(thingId).definition().toString();
        return put(dittoUrl(version, path), jsonString).withLogging(LOGGER, "Definition");
    }

    /**
     * Gets the JSON Representation of a Thing's definition.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @return the wrapped request.
     */
    protected static GetMatcher getDefinition(final int version, final CharSequence thingId) {

        final String path = ResourcePathBuilder.forThing(thingId).definition().toString();
        return get(dittoUrl(version, path)).withLogging(LOGGER, "Definition");
    }

    /**
     * Deletes a Thing's definition.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @return the wrapped request.
     */
    protected static DeleteMatcher deleteDefinition(final int version, final CharSequence thingId) {

        final String path = ResourcePathBuilder.forThing(thingId).definition().toString();
        return delete(dittoUrl(version, path)).withLogging(LOGGER, "Definition");
    }

    /**
     * Wraps a post request for a given JSON object on the features subresource.
     *
     * @param version the API version.
     * @param thingId the ID of the thing
     * @param jsonString the JSON string.
     * @return the wrapped request.
     */
    protected static PostMatcher postFeatures(final int version, final CharSequence thingId, final String jsonString) {
        final String path = ResourcePathBuilder.forThing(thingId).features().toString();
        return post(dittoUrl(version, path), jsonString).withLogging(LOGGER, "Feature");
    }

    /**
     * Puts JSON representation of features.
     *
     * @param version the API version.
     * @param thingId the ID of thing.
     * @param jsonValue the JSON value.
     * @return the wrapped request.
     */
    protected static PutMatcher putFeatures(final int version, final CharSequence thingId, final JsonValue jsonValue) {
        return putFeatures(version, thingId, jsonValue.toString()).withLogging(LOGGER, "Features");
    }

    /**
     * Puts JSON representation of features.
     *
     * @param version the API version.
     * @param thingId the ID of thing.
     * @param jsonString the jsonString string value.
     * @return the wrapped request.
     */
    protected static PutMatcher putFeatures(final int version, final CharSequence thingId, final String jsonString) {
        final String path = ResourcePathBuilder.forThing(thingId).features().toString();
        return put(dittoUrl(version, path), jsonString).withLogging(LOGGER, "Features");
    }

    /**
     * Gets the features of the given Thing ID.
     *
     * @param version the API version.
     * @param thingId the ID of a Thing.
     * @return the wrapped request.
     * @throws NullPointerException if {@code thingIdOrLocation} is {@code null}.
     */
    protected static GetMatcher getFeatures(final int version, final CharSequence thingId) {
        final String path = ResourcePathBuilder.forThing(thingId).features().toString();
        return get(dittoUrl(version, path)).withLogging(LOGGER, "Features");
    }

    /**
     * Deletes all the features of the given Thing.
     *
     * @param version the API version.
     * @param thingId the id.
     * @return the wrapped request.
     */
    protected static DeleteMatcher deleteFeatures(final int version, final CharSequence thingId) {
        final String path = ResourcePathBuilder.forThing(thingId).features().toString();
        return delete(dittoUrl(version, path)).withLogging(LOGGER, "Features");
    }

    /**
     * Puts JSON representation of feature with given featureId.
     *
     * @param version the API version.
     * @param thingId the id of thing.
     * @param featureId the id of the feature.
     * @param jsonValue the jsonValue value.
     * @return the wrapped request.
     */
    protected static PutMatcher putFeature(final int version,
            final CharSequence thingId,
            final CharSequence featureId,
            final JsonValue jsonValue) {

        return putFeature(version, thingId, featureId, jsonValue.toString()).withLogging(LOGGER, "Feature");
    }

    /**
     * Puts JSON representation of feature with given featureId.
     *
     * @param version the API version.
     * @param thingId the id of thing.
     * @param featureId the id of the feature.
     * @param jsonString the jsonString value.
     * @return the wrapped request.
     */
    protected static PutMatcher putFeature(final int version,
            final CharSequence thingId,
            final CharSequence featureId,
            final String jsonString) {

        final String path = ResourcePathBuilder.forThing(thingId).feature(featureId).toString();
        return put(dittoUrl(version, path), jsonString).withLogging(LOGGER, "Feature");
    }

    /**
     * Gets the feature with the given Feature ID of the given Thing ID.
     *
     * @param version the API version.
     * @param thingId the ID of a Thing.
     * @param featureId the ID of the Feature.
     * @return the wrapped request.
     * @throws NullPointerException if {@code thingIdOrLocation} is {@code null}.
     */
    protected static GetMatcher getFeature(final int version, final CharSequence thingId,
            final CharSequence featureId) {

        final String path = ResourcePathBuilder.forThing(thingId).feature(featureId).toString();
        return get(dittoUrl(version, path)).withLogging(LOGGER, "Feature");
    }

    /**
     * Deletes the feature with the given ID of the given Thing.
     *
     * @param version the API version.
     * @param thingId the id.
     * @param featureId the id of the feature.
     * @return the wrapped request.
     */
    protected static DeleteMatcher deleteFeature(final int version, final CharSequence thingId,
            final CharSequence featureId) {

        final String path = ResourcePathBuilder.forThing(thingId).feature(featureId).toString();
        return delete(dittoUrl(version, path)).withLogging(LOGGER, "Feature");
    }

    /**
     * Puts a Feature's definition.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @param jsonString the JSON Representation of the definition.
     * @return the wrapped request.
     */
    protected static PutMatcher putDefinition(final int version,
            final CharSequence thingId,
            final CharSequence featureId,
            final String jsonString) {

        final String path = ResourcePathBuilder.forThing(thingId).feature(featureId).definition().toString();
        return put(dittoUrl(version, path), jsonString).withLogging(LOGGER, "FeatureDefinition");
    }

    /**
     * Gets the JSON Representation of a Feature's definition.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @return the wrapped request.
     */
    protected static GetMatcher getDefinition(final int version, final CharSequence thingId,
            final CharSequence featureId) {

        final String path = ResourcePathBuilder.forThing(thingId).feature(featureId).definition().toString();
        return get(dittoUrl(version, path)).withLogging(LOGGER, "FeatureDefinition");
    }

    /**
     * Deletes Feature's definition.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @return the wrapped request.
     */
    protected static DeleteMatcher deleteDefinition(final int version, final CharSequence thingId,
            final CharSequence featureId) {

        final String path = ResourcePathBuilder.forThing(thingId).feature(featureId).definition().toString();
        return delete(dittoUrl(version, path)).withLogging(LOGGER, "FeatureDefinition");
    }

    /**
     * Sends a POST request to the /migrateDefinition endpoint with the specified JSON payload.
     *
     * @param payload the JSON payload to post.
     * @return the wrapped request.
     */
    protected static PostMatcher postMigrateDefinition(final CharSequence thingId, @Nullable final String payload, final boolean dryRun) {
        final String path = ResourcePathBuilder.forThing(thingId).migrateDefinition().toString();
        return post(dittoUrl(TestConstants.API_V_2, path), payload)
                .withParam("dry-run", String.valueOf(dryRun))
                .withLogging(LOGGER, "MigrateDefinition");
    }

    /**
     * Puts a Feature's Properties.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @param jsonString the JSON Representation.
     * @return the wrapped request.
     */
    protected static PutMatcher putProperties(final int version,
            final CharSequence thingId,
            final CharSequence featureId,
            final String jsonString) {

        final String path = ResourcePathBuilder.forThing(thingId).feature(featureId).properties().toString();
        return put(dittoUrl(version, path), jsonString).withLogging(LOGGER, "Properties");
    }

    /**
     * Gets the JSON Representation of a Feature's Properties.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @return the wrapped request.
     */
    protected static GetMatcher getProperties(final int version, final CharSequence thingId,
            final CharSequence featureId) {

        final String path = ResourcePathBuilder.forThing(thingId).feature(featureId).properties().toString();
        return get(dittoUrl(version, path)).withLogging(LOGGER, "Properties");
    }

    /**
     * Deletes Feature's Properties.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @return the wrapped request.
     */
    protected static DeleteMatcher deleteProperties(final int version, final CharSequence thingId,
            final CharSequence featureId) {

        final String path = ResourcePathBuilder.forThing(thingId).feature(featureId).properties().toString();
        return delete(dittoUrl(version, path)).withLogging(LOGGER, "Properties");
    }

    /**
     * Puts a JSON Representation of a Property on a Thing's Feature at a given JSON Pointer.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @param propertyJsonPointer the JSON Pointer.
     * @param jsonString the JSON Representation.
     * @return the wrapped request.
     */
    protected static PutMatcher putProperty(final int version,
            final CharSequence thingId,
            final CharSequence featureId,
            final CharSequence propertyJsonPointer,
            final String jsonString) {

        final String path = ResourcePathBuilder.forThing(thingId)
                .feature(featureId)
                .property(propertyJsonPointer)
                .toString();
        return put(dittoUrl(version, path), jsonString).withLogging(LOGGER, "Property");
    }

    /**
     * Gets the JSON Representation of a Property on a Thing's Feature at a given JSON Pointer.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @param propertyJsonPointer the JSON Pointer.
     * @return the wrapped request.
     */
    protected static GetMatcher getProperty(final int version,
            final CharSequence thingId,
            final CharSequence featureId,
            final CharSequence propertyJsonPointer) {

        final String path = ResourcePathBuilder.forThing(thingId)
                .feature(featureId)
                .property(propertyJsonPointer)
                .toString();
        return get(dittoUrl(version, path)).withLogging(LOGGER, "Property");
    }

    /**
     * Deletes the JSON Representation of a Property on a Thing's Feature at a given JSON Pointer.
     *
     * @param version the API version.
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @param propertyJsonPointer the JSON Pointer.
     * @return the wrapped request.
     */
    protected static DeleteMatcher deleteProperty(final int version,
            final CharSequence thingId,
            final CharSequence featureId,
            final CharSequence propertyJsonPointer) {

        final String path = ResourcePathBuilder.forThing(thingId)
                .feature(featureId)
                .property(propertyJsonPointer)
                .toString();
        return delete(dittoUrl(version, path)).withLogging(LOGGER, "Property");
    }

    /**
     * Puts a Feature's desired properties.
     *
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @param jsonString the JSON Representation.
     * @return the wrapped request.
     */
    protected static PutMatcher putDesiredProperties(final CharSequence thingId,
            final CharSequence featureId,
            final String jsonString) {

        final String path = ResourcePathBuilder.forThing(thingId).feature(featureId).desiredProperties().toString();
        return put(dittoUrl(TestConstants.API_V_2, path), jsonString).withLogging(LOGGER, "DesiredProperties");
    }

    /**
     * Gets the JSON Representation of a Feature's desired properties.
     *
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @return the wrapped request.
     */
    protected static GetMatcher getDesiredProperties(final CharSequence thingId,
            final CharSequence featureId) {

        final String path = ResourcePathBuilder.forThing(thingId).feature(featureId).desiredProperties().toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "DesiredProperties");
    }

    /**
     * Deletes Feature's desired properties.
     *
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @return the wrapped request.
     */
    protected static DeleteMatcher deleteDesiredProperties(final CharSequence thingId,
            final CharSequence featureId) {

        final String path = ResourcePathBuilder.forThing(thingId).feature(featureId).desiredProperties().toString();
        return delete(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "DesiredProperties");
    }

    /**
     * Puts a JSON Representation of a desired property on a Thing's Feature at a given JSON Pointer.
     *
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @param propertyJsonPointer the JSON Pointer.
     * @param jsonString the JSON Representation.
     * @return the wrapped request.
     */
    protected static PutMatcher putDesiredProperty(final CharSequence thingId,
            final CharSequence featureId,
            final CharSequence propertyJsonPointer,
            final String jsonString) {

        final String path = ResourcePathBuilder.forThing(thingId)
                .feature(featureId)
                .desiredProperty(propertyJsonPointer)
                .toString();
        return put(dittoUrl(TestConstants.API_V_2, path), jsonString).withLogging(LOGGER, "DesiredProperty");
    }

    /**
     * Gets the JSON Representation of a desired property on a Thing's Feature at a given JSON Pointer.
     *
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @param propertyJsonPointer the JSON Pointer.
     * @return the wrapped request.
     */
    protected static GetMatcher getDesiredProperty(final CharSequence thingId,
            final CharSequence featureId,
            final CharSequence propertyJsonPointer) {

        final String path = ResourcePathBuilder.forThing(thingId)
                .feature(featureId)
                .desiredProperty(propertyJsonPointer)
                .toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "DesiredProperty");
    }

    /**
     * Deletes the JSON Representation of a desired property on a Thing's Feature at a given JSON Pointer.
     *
     * @param thingId the ID of the Thing.
     * @param featureId the ID of the Feature.
     * @param propertyJsonPointer the JSON Pointer.
     * @return the wrapped request.
     */
    protected static DeleteMatcher deleteDesiredProperty(final CharSequence thingId,
            final CharSequence featureId,
            final CharSequence propertyJsonPointer) {

        final String path = ResourcePathBuilder.forThing(thingId)
                .feature(featureId)
                .desiredProperty(propertyJsonPointer)
                .toString();
        return delete(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "DesiredProperty");
    }

    /**
     * Puts the specified Policy.
     *
     * @param policyId the identifier of the Policy.
     * @param policy the Policy.
     * @return the wrapped request.
     */
    protected static PutMatcher putPolicy(final CharSequence policyId, final Policy policy) {
        return putPolicy(policyId, policy.toJson());
    }

    /**
     * Puts the specified Policy.
     *
     * @param policyId the identifier of the Policy.
     * @param policyJson the Policy as a Json.
     * @return the wrapped request.
     */
    protected static PutMatcher putPolicy(final CharSequence policyId, final JsonObject policyJson) {
        final String path = ResourcePathBuilder.forPolicy(policyId).toString();
        final String thingsServiceUrl = dittoUrl(TestConstants.API_V_2, path);
        final String jsonString = policyJson.toString();

        rememberForCleanUpLast(deletePolicy(policyId));

        LOGGER.debug("PUTing Policy JSON to URL '{}': {}", thingsServiceUrl, jsonString);
        return put(thingsServiceUrl, jsonString).withLogging(LOGGER, "Policy");
    }

    /**
     * Puts the specified Policy.
     *
     * @param policy the Policy.
     * @return the wrapped request.
     */
    protected static PutMatcher putPolicy(final Policy policy) {
        final PolicyId policyId = policy.getEntityId().orElseThrow(IllegalArgumentException::new);
        return putPolicy(policyId, policy).withLogging(LOGGER, "Policy");
    }

    /**
     * Puts the specified PolicyEntry.
     *
     * @param policyId the identifier of the Policy.
     * @param policyEntry the PolicyEntry.
     * @return the wrapped request.
     */
    protected static PutMatcher putPolicyEntry(final CharSequence policyId, final PolicyEntry policyEntry) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyEntry(policyEntry.getLabel()).toString();
        return put(dittoUrl(TestConstants.API_V_2, path), policyEntry.toJsonString())
                .withLogging(LOGGER, "PolicyEntry");
    }

    protected static PutMatcher putPolicyImport(final CharSequence policyId, final PolicyImport policyImport) {
        final String path =
                ResourcePathBuilder.forPolicy(policyId).policyImport(policyImport.getImportedPolicyId()).toString();
        final String thingsServiceUrl = dittoUrl(TestConstants.API_V_2, path);
        final String jsonString = policyImport.toJsonString();

        LOGGER.debug("PUTing Policy Imports JSON to URL '{}': {}", thingsServiceUrl, jsonString);
        return put(thingsServiceUrl, jsonString).withLogging(LOGGER, "Policy");
    }

    /**
     * Puts the specified policy entries.
     *
     * @param policyId the identifier of the Policy.
     * @param policyEntries the policy entries.
     * @return the wrapped request.
     */
    protected static PutMatcher putPolicyEntries(final CharSequence policyId, final JsonObject policyEntries) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyEntries().toString();
        return put(dittoUrl(TestConstants.API_V_2, path), policyEntries.toString())
                .withLogging(LOGGER, "PolicyEntries");
    }

    /**
     * Gets the Policy entries with the specified {@code policyId}.
     *
     * @param policyId the identifier of the Policy to get.
     * @return the wrapped request.
     */
    protected static GetMatcher getPolicyEntries(final CharSequence policyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyEntries().toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "PolicyEntries");
    }

    /**
     * Gets the Policy entry subjects with the specified {@code policyId} and {@code label}.
     *
     * @param policyId the identifier of the Policy to get.
     * @param label the label of the policy entry.
     * @return the wrapped request.
     */
    protected static GetMatcher getPolicyEntrySubjects(final CharSequence policyId, final String label) {
        final String path = ResourcePathBuilder.forPolicy(policyId).subjects(label).toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "PolicyEntrySubjects");
    }

    /**
     * Puts the Policy entry subjects with the specified {@code policyId} and {@code label}.
     *
     * @param policyId the identifier of the Policy to get.
     * @param label the label of the policy entry.
     * @param subjects the subjects to put.
     * @return the wrapped request.
     */
    protected static PutMatcher putPolicyEntrySubjects(final CharSequence policyId, final String label,
            final Subjects subjects) {
        final String path = ResourcePathBuilder.forPolicy(policyId).subjects(label).toString();
        return put(dittoUrl(TestConstants.API_V_2, path), subjects.toJsonString()).withLogging(LOGGER,
                "PolicyEntrySubjects");
    }

    /**
     * Gets the Policy entry subject with the specified {@code policyId} and {@code label}.
     *
     * @param policyId the identifier of the Policy to get.
     * @param label the label of the policy entry.
     * @param subjectId the id of the subject.
     * @return the wrapped request.
     */
    protected static GetMatcher getPolicyEntrySubject(final CharSequence policyId, final String label,
            final String subjectId) {
        final String path = ResourcePathBuilder.forPolicy(policyId).subject(label, subjectId).toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "PolicyEntrySubject");
    }

    /**
     * Puts the Policy entry subject with the specified {@code policyId}, {@code label} and {@code subjectId}.
     *
     * @param policyId the identifier of the Policy to get.
     * @param label the label of the policy entry.
     * @param subjectId the id of the subject.
     * @param subject The subject to put.
     * @return the wrapped request.
     */
    protected static PutMatcher putPolicyEntrySubject(final CharSequence policyId, final String label,
            final String subjectId, final Subject subject) {
        final String path = ResourcePathBuilder.forPolicy(policyId).subject(label, subjectId).toString();
        return put(dittoUrl(TestConstants.API_V_2, path), subject.toJsonString()).withLogging(LOGGER,
                "PolicyEntrySubject");
    }

    /**
     * Deletes the Policy entry subject with the specified {@code policyId}, {@code label} and {@code subjectId}.
     *
     * @param policyId the identifier of the Policy to get.
     * @param label the label of the policy entry.
     * @param subjectId the id of the subject.
     * @return the wrapped request.
     */
    protected static DeleteMatcher deletePolicyEntrySubject(final CharSequence policyId, final String label,
            final String subjectId) {
        final String path = ResourcePathBuilder.forPolicy(policyId).subject(label, subjectId).toString();
        return delete(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER,
                "PolicyEntrySubject");
    }

    /**
     * Gets the Policy entry resources with the specified {@code policyId} and {@code label}.
     *
     * @param policyId the identifier of the Policy to get.
     * @param label the label of the policy entry.
     * @return the wrapped request.
     */
    protected static GetMatcher getPolicyEntryResources(final CharSequence policyId, final String label) {
        final String path = ResourcePathBuilder.forPolicy(policyId).resources(label).toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "PolicyEntryResources");
    }

    /**
     * Puts the Policy entry resources with the specified {@code policyId} and {@code label} and {@code resourcePath}.
     *
     * @param policyId the identifier of the Policy to get.
     * @param label the label of the policy entry.
     * @param resources the resources to put.
     * @return the wrapped request.
     */
    protected static PutMatcher putPolicyEntryResources(final CharSequence policyId, final String label,
            final Resources resources) {
        final String path = ResourcePathBuilder.forPolicy(policyId).resources(label).toString();
        return put(dittoUrl(TestConstants.API_V_2, path), resources.toJsonString()).withLogging(LOGGER,
                "PolicyEntryResources");
    }

    /**
     * Gets the Policy entry resource with the specified {@code policyId} and {@code label} and {@code resourcePath}.
     *
     * @param policyId the identifier of the Policy to get.
     * @param label the label of the policy entry.
     * @param resourcePath the path of the resource.
     * @return the wrapped request.
     */
    protected static GetMatcher getPolicyEntryResource(final CharSequence policyId, final String label,
            final String resourcePath) {
        final String path =
                ResourcePathBuilder.forPolicy(policyId).resource(label, resourcePath).toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "PolicyEntryResource");
    }

    /**
     * Puts the Policy entry resource with the specified {@code policyId} and {@code label} and {@code resourcePath}.
     *
     * @param policyId the identifier of the Policy to get.
     * @param label the label of the policy entry.
     * @param resourcePath the path of the resource.
     * @param resource the resource to put.
     * @return the wrapped request.
     */
    protected static PutMatcher putPolicyEntryResource(final CharSequence policyId, final String label,
            final String resourcePath, final Resource resource) {
        final String path =
                ResourcePathBuilder.forPolicy(policyId).resource(label, resourcePath).toString();
        return put(dittoUrl(TestConstants.API_V_2, path), resource.toJsonString()).withLogging(LOGGER,
                "PolicyEntryResource");
    }

    /**
     * Deletes the Policy entry resource with the specified {@code policyId} and {@code label} and {@code resourcePath}.
     *
     * @param policyId the identifier of the Policy to get.
     * @param label the label of the policy entry.
     * @param resourcePath the path of the resource.
     * @return the wrapped request.
     */
    protected static DeleteMatcher deletePolicyEntryResource(final CharSequence policyId, final String label,
            final String resourcePath) {
        final String path =
                ResourcePathBuilder.forPolicy(policyId).resource(label, resourcePath).toString();
        return delete(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER,
                "PolicyEntryResource");
    }

    /**
     * Gets the Policy with the specified {@code policyId}.
     *
     * @param policyId the identifier of the Policy to get.
     * @return the wrapped request.
     */
    protected static GetMatcher getPolicy(final CharSequence policyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId).toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "Policy");
    }

    /**
     * Deletes the Policy with the specified {@code policyId}.
     *
     * @param policyId the identifier of the Policy to delete.
     * @return the wrapped request.
     */
    protected static DeleteMatcher deletePolicy(final CharSequence policyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId).toString();
        final String thingsServiceUrl = dittoUrl(TestConstants.API_V_2, path);

        return delete(thingsServiceUrl).withLogging(LOGGER, "Policy");
    }

    /**
     * Gets the Policy via field selector of the specified {@code thingId}.
     *
     * @param thingId the identifier of the Thing to get the Policy for.
     * @return the wrapped request.
     */
    protected static GetMatcher getThingPolicy(final CharSequence thingId) {
        final String path = ResourcePathBuilder.forThing(thingId).toString();
        return get(dittoUrl(TestConstants.API_V_2, path))
                .withParam("fields", "_policy")
                .withLogging(LOGGER, "ThingPolicy");
    }

    /**
     * Gets the specified PolicyEntry.
     *
     * @param policyId the identifier of the Policy.
     * @param policyLabel the label of the PolicyEntry.
     * @return the wrapped request.
     */
    protected static GetMatcher getPolicyEntry(final CharSequence policyId, final CharSequence policyLabel) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyEntry(policyLabel).toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "PolicyEntry");
    }

    /**
     * Deletes the specified PolicyEntry.
     *
     * @param policyId the identifier of the Policy.
     * @param policyLabel the label of the PolicyEntry.
     * @return the wrapped request.
     */
    protected static DeleteMatcher deletePolicyEntry(final CharSequence policyId, final CharSequence policyLabel) {
        final String path = ResourcePathBuilder.forPolicy(policyId).policyEntry(policyLabel).toString();
        return delete(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "PolicyEntry");
    }

    /**
     * Executes the specified policy "action" with the given payload.
     *
     * @param policyId the identifier of the Policy.
     * @param actionName the action name to invoke "toplevel" on the Policy.
     * @param payload the optional payload to send along.
     * @return the wrapped request.
     */
    protected static PostMatcher postPolicyAction(final CharSequence policyId, final String actionName,
            @Nullable final String payload) {
        final String path = ResourcePathBuilder.forPolicy(policyId).action(actionName).toString();
        return post(dittoUrl(TestConstants.API_V_2, path), payload)
                .withLogging(LOGGER, "PolicyAction");
    }

    /**
     * Sends a POST request to the /checkPermissions endpoint with the specified JSON payload.
     *
     * @param payload the JSON payload to post.
     * @return the wrapped request.
     */
    protected static PostMatcher postCheckPermissions(@Nullable final String payload) {
        final String path = ResourcePathBuilder.forCheckPermissions().toString();
        return post(dittoUrl(TestConstants.API_V_2, path), payload)
                .withLogging(LOGGER, "CheckPermissions");
    }

    /**
     * Wraps a POST message request for the specified {@code thingId}, {@code direction} and {@code messageSubject}
     * combination.
     *
     * @param version the API version
     * @param thingId the Thing ID to do the request on
     * @param direction the message direction (from/to)
     * @param messageSubject the message subject
     * @return the wrapped request.
     */
    public static PostMatcher postMessage(final int version,
            final CharSequence thingId,
            final MessageDirection direction,
            final String messageSubject,
            final ContentType contentType,
            @Nullable final String payload,
            @Nullable final String timeoutInSeconds) {

        return postMessage(version, thingId, direction, messageSubject, contentType.toString(), payload,
                timeoutInSeconds);
    }

    /**
     * Wraps a POST message request for the specified {@code thingId}, {@code direction} and {@code messageSubject}
     * combination.
     *
     * @param version the API version
     * @param thingId the Thing ID to do the request on
     * @param direction the message direction (from/to)
     * @param messageSubject the message subject
     * @return the wrapped request.
     */
    public static PostMatcher postMessage(final int version,
            final CharSequence thingId,
            final MessageDirection direction,
            final String messageSubject,
            final String contentType,
            @Nullable final String payload,
            @Nullable final String timeoutInSeconds) {

        final byte[] payloadBytes = payload != null ? payload.getBytes() : null;
        return postMessage(version, thingId, direction, messageSubject, contentType, payloadBytes,
                timeoutInSeconds);
    }

    /**
     * Overload postMessage to implement default value for timeout
     */
    public static PostMatcher postMessage(final int version,
            final CharSequence thingId,
            final MessageDirection direction,
            final String messageSubject,
            final String contentType,
            final byte[] payload) {

        return postMessage(version, thingId, direction, messageSubject, contentType, payload, null);
    }

    public static PostMatcher postMessage(final int version,
            final CharSequence thingId,
            final MessageDirection direction,
            final String messageSubject,
            final String contentType,
            @Nullable final byte[] payload,
            final String timeoutInSeconds) {

        checkNotNull(thingId, "thing ID");
        checkNotNull(direction, "direction");
        checkNotNull(messageSubject, "message subject");

        final String thingPath = ResourcePathBuilder.forThing(thingId).toString();
        // add 0-timeoutInSeconds for immediate response
        final String path = thingPath + (direction == MessageDirection.TO ? HttpResource.INBOX : HttpResource.OUTBOX) +
                HttpResource.MESSAGES + '/' + messageSubject;
        final String thingsServiceUrl = dittoUrl(version, path);

        return post(thingsServiceUrl, contentType, payload)
                .withParam("timeout", (timeoutInSeconds != null ? timeoutInSeconds : "0"))
                .withLogging(LOGGER, "Message");
    }

    /**
     * Overload postMessage to implement default value for timeout
     */
    public static PostMatcher postMessage(final int version,
            final CharSequence thingId,
            final String featureId,
            final MessageDirection direction,
            final String messageSubject,
            final String contentType,
            final byte[] payload) {

        return postMessage(version, thingId, featureId, direction, messageSubject, contentType, payload, null);
    }

    /**
     * Wraps a POST feature message request for the specified {@code thingId}, {@code direction} and {@code
     * messageSubject} combination.
     *
     * @param version the API version
     * @param thingId the Thing ID to do the request on
     * @param direction the message direction (from/to)
     * @param messageSubject the message subject
     * @return the wrapped request.
     */
    public static PostMatcher postMessage(final int version,
            final CharSequence thingId,
            final String featureId,
            final MessageDirection direction,
            final String messageSubject,
            final ContentType contentType,
            final String payload,
            @Nullable final String timeoutInSeconds) {

        return postMessage(version, thingId, featureId, direction, messageSubject, contentType.toString(), payload,
                timeoutInSeconds);
    }

    /**
     * Wraps a POST feature message request for the specified {@code thingId}, {@code direction} and {@code
     * messageSubject} combination.
     *
     * @param version the API version
     * @param thingId the Thing ID to do the request on
     * @param direction the message direction (from/to)
     * @param messageSubject the message subject
     * @return the wrapped request.
     */
    public static PostMatcher postMessage(final int version,
            final CharSequence thingId,
            final String featureId,
            final MessageDirection direction,
            final String messageSubject,
            final String contentType,
            @Nullable final String payload,
            @Nullable final String timeoutInSeconds) {
        final byte[] payloadBytes = payload != null ? payload.getBytes() : null;
        return postMessage(version, thingId, featureId, direction, messageSubject, contentType, payloadBytes,
                timeoutInSeconds);
    }

    public static PostMatcher postMessage(final int version,
            final CharSequence thingId,
            final String featureId,
            final MessageDirection direction,
            final String messageSubject,
            final String contentType,
            @Nullable final byte[] payload,
            @Nullable final String timeoutInSeconds) {
        checkNotNull(thingId, "thing ID");
        checkNotNull(featureId, "feature ID");
        checkNotNull(direction, "direction");
        checkNotNull(messageSubject, "message subject");

        final String featurePath = ResourcePathBuilder.forThing(thingId).feature(featureId).toString();
        // add 0-timeout for immediate response
        final String path =
                featurePath + (direction == MessageDirection.TO ? HttpResource.INBOX : HttpResource.OUTBOX) +
                        HttpResource.MESSAGES + '/' + messageSubject;
        final String thingsServiceUrl = dittoUrl(version, path);

        return post(thingsServiceUrl, contentType, payload).withLogging(LOGGER, "Message")
                .withParam("timeout", timeoutInSeconds != null ? timeoutInSeconds : "0");
    }

    /**
     * Executes the given {@code restPostOperation}. If the response's code is not {@code expectedHttpStatus}, this
     * method executes the operation again after the given {@code timeout}.
     *
     * @param restOperation the REST operation to getAndExtractResult.
     * @param expectedHttpStatus the expected HTTP status.
     * @param timeout the timeout to await if the first execution of the operation yields a conflict.
     */
    protected static void retryAfterTimeout(final Supplier<Response> restOperation,
            final HttpStatus expectedHttpStatus, final long timeout) {

        Response response = restOperation.get();
        if (response.getStatusCode() != expectedHttpStatus.getCode()) {
            // try again after some time if the post operation failed
            try {
                Thread.sleep(timeout);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            response = restOperation.get();

            assertThat(response.getStatusCode(), HttpVerbMatcher.is(expectedHttpStatus));
        }
    }

    /**
     * Gets from the given location.
     *
     * @return the wrapped request.
     * @throws NullPointerException if any of the arguments are {@code null}.
     */
    protected static GetMatcher getByLocation(final String location) {
        return get(location).withLogging(LOGGER, "<unknown>");
    }

    protected static GetMatcher get(final String path) {
        return restMatcherFactory.get(path);
    }

    protected static PostMatcher post(final String path, @Nullable final String jsonString) {
        return restMatcherFactory.post(path, jsonString);
    }

    protected static PostMatcher post(final String path, final String contentType, @Nullable final byte[] payload) {
        return restMatcherFactory.post(path, contentType, payload);
    }

    protected static PutMatcher put(final String path, final String jsonString) {
        return restMatcherFactory.put(path, jsonString);
    }

    protected static PatchMatcher patch(final String path, final CharSequence contentType, final String jsonString) {
        return restMatcherFactory.patch(path, contentType, jsonString);
    }

    protected static DeleteMatcher delete(final String path) {
        return restMatcherFactory.delete(path);
    }

    protected static void rememberForCleanUp(final HttpVerbMatcher<?> action) {
        cleanUpMatchers.addFirst(action);
    }

    protected static void rememberForCleanUpLast(final HttpVerbMatcher<?> action) {
        cleanUpMatchers.addLast(action);
    }

    public static void rememberForCleanUp(final Runnable action) {
        cleanUpActions.addFirst(action);
    }

    protected static IdGenerator idGenerator() {
        return defaultIdGenerator;
    }

    protected static IdGenerator idGenerator(final String namespace) {
        requireNonNull(namespace);
        return IdGenerator.fromNamespace(namespace);
    }

    /**
     * This TestWatcher logs the name of each performed test method.
     */
    private static final class TestedMethodLoggingWatcher extends TestWatcher {

        private final Logger logger;

        public TestedMethodLoggingWatcher(final Logger logger) {
            this.logger = logger;
        }

        @Override
        protected void starting(final org.junit.runner.Description description) {
            logger.info("Testing: {}#{}()", description.getTestClass().getSimpleName(), description.getMethodName());
        }

    }

    protected static ConnectionsClient connectionsClient() {
        return connectionsClient;
    }

    protected static void expectStatusOkWithDelay(final HttpVerbMatcher<?> httpVerbMatcher) {
        final ConditionFactory conditionFactory = Awaitility.await()
                // do not create more than 50 entities per second
                .pollDelay(20, TimeUnit.MILLISECONDS)
                // back off more if there is a problem
                .pollInterval(250, TimeUnit.MILLISECONDS)
                // give up the test if Ditto fails to respond in 60 seconds
                .atMost(Duration.ofMinutes(1));
        final Consumer<Integer> statusCodeIsCreatedOrNoContent = statusCode ->
                Assertions.assertThat(statusCode).isIn(HttpStatus.CREATED.getCode(),
                        HttpStatus.NO_CONTENT.getCode(),
                        HttpStatus.OK.getCode());
        httpVerbMatcher.useAwaitility(conditionFactory)
                .expectingStatusCode(satisfies(statusCodeIsCreatedOrNoContent))
                .fire();
    }

}
