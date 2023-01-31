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

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.base.api.devops.signals.commands.ExecutePiggybackCommand;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.signals.commands.Command;
import org.eclipse.ditto.base.model.signals.commands.CommandResponse;
import org.eclipse.ditto.json.JsonField;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.matcher.PostMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.response.Response;

/**
 * This class is a means for sending {@link ExecutePiggybackCommand}s.
 */
public final class PiggyBackCommander {

    private static final String PIGGY_BACK_URL = "/devops/piggyback";

    private static final Logger LOGGER = LoggerFactory.getLogger(PiggyBackCommander.class);
    private static final String DEFAULT_SUFFIX = "/.default"; // This is required by azure

    private final String serviceName;
    @Nullable private final String instance;
    private final String piggyBackCommandUrl;

    private PiggyBackCommander(final String serviceName) {
        this(serviceName, null);
    }

    private PiggyBackCommander(final String serviceName, @Nullable final String instance) {
        this.serviceName = checkNotNull(serviceName, "service name");
        this.instance = instance;
        if (instance != null) {
            piggyBackCommandUrl = PIGGY_BACK_URL + "/" + serviceName + "/" + instance;
        } else {
            piggyBackCommandUrl = PIGGY_BACK_URL + "/" + serviceName;
        }
    }

    /**
     * Returns an instance of PiggybackCommander.
     *
     * @param serviceName the name of the service to send the piggy back command to.
     * This name determines the command target URL.
     * @return the instance.
     */
    public static PiggyBackCommander getInstance(final String serviceName) {
        return new PiggyBackCommander(serviceName);
    }

    public static HttpStatus getPiggybackStatus(final Response response) {
        return PiggyBackCommandResponse.getForHttpResponse(response)
                .getValue(CommandResponse.JsonFields.STATUS)
                .flatMap(HttpStatus::tryGetInstance)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Did not find piggyback status in: " + response.body().asString()));
    }

    /**
     * Returns an instance of PiggybackCommander.
     *
     * @param serviceName the name of the service to send the piggy back command to.
     * @param instance the instance ID to send the piggy back command to.
     * This name determines the command target URL.
     * @return the instance.
     */
    public static PiggyBackCommander getInstance(final String serviceName, final String instance) {
        return new PiggyBackCommander(serviceName, instance);
    }

    public PostMatcherBuilder preparePost(final Command<?> command, final String targetActorSelector) {
        checkNotNull(command, "command");
        return new PostMatcherBuilder(serviceName, instance, piggyBackCommandUrl, command.toJson(),
                targetActorSelector);
    }

    public PostMatcherBuilder preparePost(final Command<?> command,
            final Predicate<JsonField> predicate, final String targetActorSelector) {

        checkNotNull(command, "command");
        checkNotNull(predicate, "predicate");
        return new PostMatcherBuilder(serviceName, instance, piggyBackCommandUrl, command.toJson(predicate),
                targetActorSelector);
    }

    public PostMatcherBuilder preparePost(final JsonObject payloadJson, final String targetActorSelector) {
        return new PostMatcherBuilder(serviceName, instance, piggyBackCommandUrl, payloadJson, targetActorSelector);
    }

    @NotThreadSafe
    public static final class PostMatcherBuilder {

        private final String serviceName;
        private final String piggyBackCommandUrl;
        private final JsonObject payloadJson;
        private final String targetActorSelector;
        private final CommonTestConfig testConfig;
        private final Set<Integer> expectedStatusCodes;
        private String instanceId;
        private DittoHeaders additionalHeaders;
        private Matcher<String> responseBodyMatcher;
        private String responseBodyKey;

        private PostMatcherBuilder(final String serviceName,
                @Nullable final String instanceId,
                final String piggyBackCommandUrl,
                final JsonObject payloadJson,
                final String targetActorSelector) {

            this.serviceName = serviceName;
            this.instanceId = instanceId;
            this.piggyBackCommandUrl = piggyBackCommandUrl;
            this.targetActorSelector = checkNotNull(targetActorSelector, "target actor selector");
            this.payloadJson = checkNotNull(payloadJson, "payload JSON");
            testConfig = CommonTestConfig.getInstance();
            expectedStatusCodes = new HashSet<>(2);
            additionalHeaders = DittoHeaders.empty();
            responseBodyMatcher = null;
            responseBodyKey = "";
        }

        /**
         * @throws NullPointerException if {@code expectedStatus} is {@code null}.
         */
        public PostMatcherBuilder withExpectedStatus(final HttpStatus expectedStatus) {
            expectedStatusCodes.add(checkNotNull(expectedStatus, "expectedStatus").getCode());
            return this;
        }

        /**
         * @throws NullPointerException if {@code bodyMatcher}  or {@code responseBodyKey} is {@code null}.
         */
        public PostMatcherBuilder withExpectedBodyForKey(final Matcher<String> bodyMatcher,
                final String responseBodyKey) {

            responseBodyMatcher = checkNotNull(bodyMatcher, "expected response body matcher");
            this.responseBodyKey = checkNotNull(responseBodyKey, "responseBodyKey");
            return this;
        }

        public PostMatcherBuilder serviceInstance(final String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        /**
         * @throws NullPointerException if {@code additionalHeaders} is {@code null}.
         */
        public PostMatcherBuilder withAdditionalHeaders(final DittoHeaders additionalHeaders) {
            this.additionalHeaders = checkNotNull(additionalHeaders, "additional headers");
            return this;
        }

        public PostMatcher build() {
            final String piggyBackUrl = getTargetUrl();
            final String payload = getPayloadForPost(getPiggyBackCommand());
            final Matcher<String> responseStatusCodeMatcher;
            final Matcher<String> responseBodyMatcher;
            if (expectedStatusCodes.isEmpty()) {
                responseStatusCodeMatcher = null;
            } else {
                responseStatusCodeMatcher = getResponseStatusCodeMatcher();
            }

            if (this.responseBodyMatcher == null) {
                responseBodyMatcher = null;
            } else {
                responseBodyMatcher = getResponseBodyMatcher();
            }

            return getPostMatcher(piggyBackUrl, payload, responseStatusCodeMatcher, responseBodyMatcher);
        }

        private String getTargetUrl() {
            final CommonTestConfig commonTestConfig = CommonTestConfig.getInstance();
            return commonTestConfig.getDevopsUrl(piggyBackCommandUrl);
        }

        private ExecutePiggybackCommand getPiggyBackCommand() {
            return ExecutePiggybackCommand.of(serviceName, instanceId, targetActorSelector, payloadJson, getHeaders());
        }

        private DittoHeaders getHeaders() {
            return additionalHeaders.toBuilder()
                    .putHeader("aggregate", "false")
                    .build();
        }

        private static String getPayloadForPost(final ExecutePiggybackCommand piggyBackCommand) {
            JsonObject commandJson = piggyBackCommand.toJson();
            final DittoHeaders commandHeaders = piggyBackCommand.getDittoHeaders();
            commandJson = commandJson.setValue("headers", commandHeaders.toJson());

            return commandJson.toString();
        }

        private Matcher<String> getResponseStatusCodeMatcher() {
            final Matcher<Integer> statusCodeMatcher;
            if (expectedStatusCodes.isEmpty()) {
                statusCodeMatcher = Matchers.any(Integer.class);
            } else {
                statusCodeMatcher = Matchers.isIn(expectedStatusCodes);
            }

            return new PiggyCommandByResponseStatusMatcher(statusCodeMatcher);
        }

        private Matcher<String> getResponseBodyMatcher() {
            return new PiggyCommandByResponseBodyMatcher(responseBodyMatcher, responseBodyKey);
        }

        private PostMatcher getPostMatcher(final String piggyBackUrl, final String payload,
                @Nullable final Matcher<String> responseStatusCodeMatcher,
                @Nullable final Matcher<String> responseBodyMatcher) {

            final PostMatcher result = PostMatcher.newInstance(piggyBackUrl, payload);
            result.withLogging(LOGGER, "Piggyback");
            result.withParam("timeout", getConfiguredCommandTimeout());
            result.expectingHttpStatus(HttpStatus.OK);
            if (responseStatusCodeMatcher != null) {
                result.expectingBody(responseStatusCodeMatcher);
            }

            if (responseBodyMatcher != null) {
                result.expectingBody(responseBodyMatcher);
            }

            if (testConfig.isDevopsAuthEnabled()) {
                result.withBasicAuth(testConfig.getDevopsAuthUser(), testConfig.getDevopsAuthPassword());
            }

            return result;
        }

        private String getConfiguredCommandTimeout() {
            final Duration devOpsCommandTimeout = testConfig.getDevopsCommandTimeout();
            return devOpsCommandTimeout.toMillis() + "ms";
        }

    }

    private static final class PiggyCommandByResponseStatusMatcher extends TypeSafeMatcher<String> {

        private static final String STATUS_KEY = "status";

        private final Matcher<Integer> responseStatusMatcher;

        private PiggyCommandByResponseStatusMatcher(final Matcher<Integer> responseStatusMatcher) {
            this.responseStatusMatcher = checkNotNull(responseStatusMatcher);
        }

        @Override
        protected boolean matchesSafely(final String jsonString) {
            final JsonValue piggyCommandResponse = extractPiggyBackCommandResponse(jsonString);
            if (piggyCommandResponse.isObject()) {
                return piggyCommandResponse.asObject().getValue(STATUS_KEY)
                        .filter(JsonValue::isInt)
                        .map(JsonValue::asInt)
                        .filter(responseStatusMatcher::matches)
                        .isPresent();
            } else {
                // For other JsonValue types than JsonObject the validation on the status-code should be executed
                // on the received response manually, rather than using the HttpVerbMatcher.
                LOGGER.error("Received a non-JsonObject response, use direct response-status validation instead");
                return false;
            }
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("field '" + STATUS_KEY + "' to match ");
            responseStatusMatcher.describeTo(description);
        }

    }

    private static final class PiggyCommandByResponseBodyMatcher extends TypeSafeMatcher<String> {

        private final Matcher<String> responseBodyMatcher;
        private final String responseBodyKey;

        private PiggyCommandByResponseBodyMatcher(final Matcher<String> responseBodyMatcher,
                final String responseBodyKey) {

            this.responseBodyMatcher = checkNotNull(responseBodyMatcher, "responseBodyMatcher");
            this.responseBodyKey = checkNotNull(responseBodyKey, "responseBodyKey");
        }

        @Override
        protected boolean matchesSafely(final String jsonString) {
            final JsonValue piggyCommandResponse = extractPiggyBackCommandResponse(jsonString);
            if (piggyCommandResponse.isObject()) {
                return piggyCommandResponse.asObject().getValue(responseBodyKey)
                        .filter(JsonValue::isObject)
                        .map(JsonValue::toString)
                        .filter(responseBodyMatcher::matches)
                        .isPresent();
            } else {
                // For other JsonValue types than JsonObject the validation on the response-body should be executed
                // on the received response manually, rather than using the HttpVerbMatcher.
                LOGGER.error("Received a non-JsonObject response, use direct response-body validation instead");
                return false;
            }
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("field '" + responseBodyKey + "' to match ");
            responseBodyMatcher.describeTo(description);
        }

    }

    private static JsonValue extractPiggyBackCommandResponse(final String jsonString) {
        return PiggyBackCommandResponse.getForString(jsonString);
    }

}
