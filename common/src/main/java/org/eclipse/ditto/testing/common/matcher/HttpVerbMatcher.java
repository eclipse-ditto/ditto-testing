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
package org.eclipse.ditto.testing.common.matcher;

import static io.restassured.RestAssured.expect;
import static java.util.Objects.requireNonNull;
import static org.eclipse.ditto.base.model.common.ConditionChecker.argumentNotEmpty;
import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.net.SocketException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.awaitility.core.ConditionFactory;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.HttpParameter;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.client.BasicAuth;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import io.restassured.specification.ResponseSpecification;

/**
 * Abstract base class for performing REST calls and checking if everything works as expected.
 */
@NotThreadSafe
public abstract class HttpVerbMatcher<T extends HttpVerbMatcher> {

    private static final Consumer<RequestSpecification> NO_AUTHENTICATION = requestSpecification -> {
        // Nothing to do here.
    };

    protected final Map<String, List<String>> parameters;
    protected final Map<String, Object> headers;
    protected final String path;
    private final ResponseSpecification responseSpecification;
    private final List<Matcher<String>> bodyMatchers;
    private final Collection<Consumer<Response>> responseConsumers;

    private Consumer<RequestSpecification> authenticationProvider = NO_AUTHENTICATION;

    @Nullable private ConditionFactory conditionFactory = null;
    private Logger logger = LoggerFactory.getLogger(getClass());
    @Nullable private String entityType;

    /**
     * Constructs a new {@code HttpVerbMatcher} object.
     *
     * @param path path of the HTTP resource.
     * @throws NullPointerException if {@code path} is {@code null}.
     * @throws IllegalArgumentException if {@code path} is empty.
     */
    protected HttpVerbMatcher(final String path) {
        this.path = argumentNotEmpty(path, "path");
        parameters = new HashMap<>();
        responseSpecification = createNewResponseSpec();
        headers = new HashMap<>();
        bodyMatchers = new ArrayList<>();
        responseConsumers = new HashSet<>(1);
    }

    private static ResponseSpecification createNewResponseSpec() {
        return expect();
    }

    /**
     * Makes it possible to use inheritance with fluent api. (See http://egalluzzo.blogspot.de/2010/06/using-inheritance-with-fluent.html)
     *
     * @return the concrete subclass instance
     */
    protected T getThis() {
        @SuppressWarnings("unchecked") final T thisAsSubType = (T) this;
        return thisAsSubType;
    }

    /**
     * Sets request parameters to be used for the REST request.
     *
     * @param parameter the name of the request parameter.
     * @param paramValues the value(s) of the request parameter. This values are concatenated appropriately by this
     * method.
     * @return this instance to allow Method Chaining.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public T withParam(final HttpParameter parameter, final CharSequence... paramValues) {
        checkNotNull(parameter, "parameter name");

        return withParam(parameter.toString(), paramValues);
    }

    /**
     * Sets request parameters to be used for the REST request.
     *
     * @param parameter the name of the request parameter.
     * @param paramValues the value(s) of the request parameter. This values are concatenated appropriately by this
     * method.
     * @return this instance to allow Method Chaining.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public T withParam(final String parameter, final CharSequence... paramValues) {
        checkNotNull(parameter, "parameter name");
        requireNonNull(paramValues, "The parameter values must not be null!"
                + " If none are required, please use an empty array instead of null or"
                + " do not specify parameter values at all.");

        parameters.put(parameter, Collections.singletonList(String.join(",", Arrays.asList(paramValues))));
        return getThis();
    }

    /**
     * Returns a {@link Matcher} for checking if the actual {@link HttpStatus} is equal to the expected HTTP status.
     *
     * @param expectedHttpStatus the HTTP status which is expected.
     * @return a matcher for checking the equality of the actual and expected status.
     */
    public static Matcher<Integer> is(final HttpStatus expectedHttpStatus) {
        return new CustomTypeSafeMatcher<>(MessageFormat.format("<{0}>", expectedHttpStatus.getCode())) {
            @Override
            protected boolean matchesSafely(final Integer actualStatusCode) {
                return Objects.equals(actualStatusCode, expectedHttpStatus.getCode());
            }
        };
    }

    /**
     * Returns a {@link Matcher} for checking if the actual {@link HttpStatus} is equal to one of the allowed HTTP
     * statuses.
     *
     * @param first the first allowed HTTP status.
     * @param furtherStatuses further allowed HTTP statuses.
     * @return the matcher.
     */
    public static Matcher<Integer> isOneOf(final HttpStatus first, final HttpStatus... furtherStatuses) {
        checkNotNull(first, "first");
        checkNotNull(furtherStatuses, "furtherStatuses");

        final List<Integer> allowedStatuses = new ArrayList<>(1 + furtherStatuses.length);
        allowedStatuses.add(first.getCode());
        allowedStatuses.addAll(Arrays.stream(furtherStatuses)
                .map(HttpStatus::getCode)
                .collect(Collectors.toList()));

        return new CustomTypeSafeMatcher<Integer>(MessageFormat.format("<{0}>", allowedStatuses)) {
            @Override
            protected boolean matchesSafely(final Integer actualStatus) {
                return allowedStatuses.contains(actualStatus);
            }
        };
    }

    public static Matcher<ContentType> is(final ContentType expectedContentType) {
        return new CustomTypeSafeMatcher<ContentType>(MessageFormat.format("<{0}>", expectedContentType)) {
            @Override
            protected boolean matchesSafely(final ContentType actualContentType) {
                return Objects.equals(actualContentType, expectedContentType);
            }
        };
    }


    /**
     * Sets Basic Auth credentials.
     *
     * @param user the user.
     * @param password the password.
     * @return this instance to allow Method Chaining.
     */
    public T withBasicAuth(final String user, final String password) {
        return withBasicAuth(user, password, false);
    }

    /**
     * Sets Basic Auth credentials.
     *
     * @param user the user.
     * @param password the password.
     * @param preemptive whether preemptive authentication is enabled: this means that the authentication details are
     * sent in the request header regardless if the server has challenged for authentication or not.
     * @return this instance to allow Method Chaining.
     */
    public T withBasicAuth(final String user, final String password, final boolean preemptive) {
        authenticationProvider = requestSpec -> {
            if (preemptive) {
                requestSpec.auth().preemptive().basic(user, password);
            } else {
                requestSpec.auth().basic(user, password);
            }
        };
        return getThis();
    }

    /**
     * Sets a Dummy Authorization Header for simple testing of different authorization scenarios.
     *
     * @param preAuthenticatedAuth the {@link PreAuthenticatedAuth}.
     * @return this instance to allow Method Chaining.
     * @see #withPreAuthenticatedAuth(String)
     */
    public T withPreAuthenticatedAuth(final PreAuthenticatedAuth preAuthenticatedAuth) {
        return withPreAuthenticatedAuth(requireNonNull(preAuthenticatedAuth).toHeaderValue());
    }

    /**
     * Sets a Dummy Authorization Header for simple testing of different authorization scenarios.
     *
     * @param authenticationSubjectsWithPrefix the token to authorize.
     * @return this instance to allow Method Chaining.
     * @see #withPreAuthenticatedAuth(PreAuthenticatedAuth)
     */
    public T withPreAuthenticatedAuth(final String authenticationSubjectsWithPrefix) {
        authenticationProvider = requestSpec ->
                requestSpec.header(HttpHeader.X_DITTO_PRE_AUTH.getName(), authenticationSubjectsWithPrefix);

        return getThis();
    }

    /**
     * Sets the JWT Authorization Header.
     *
     * @param token the token to authorize.
     * @return this instance to allow Method Chaining.
     */
    public T withJWT(final String token) {
        return withJWT(token, false);
    }

    /**
     * Sets the JWT Authorization Header.
     *
     * @param token the token to authorize.
     * @param preemptive whether preemptive authentication is enabled: this means that the authentication details are
     * sent in the request header regardless if the server has challenged for authentication or not.
     * @return this instance to allow Method Chaining.
     */
    public T withJWT(final String token, final boolean preemptive) {
        authenticationProvider = requestSpec -> {
            final String authorization = "Bearer " + token;
            requestSpec.header("Authorization", authorization);
            if (preemptive) {
                requestSpec.auth().preemptive();
            }
        };

        return getThis();
    }

    /**
     * Sets Authorization Header according to configured authentication method.
     * If Basic Auth is enabled, Basic auth is used, otherwise OAuth method.
     * @param testingContext the testing context
     *
     * @return this instance to allow Method Chaining.
     */
    public T withConfiguredAuth(final TestingContext testingContext) {
        return withConfiguredAuth(testingContext, false);
    }

    /**
     * Sets Authorization Header according to configured authentication method.
     * If Basic Auth is enabled, Basic auth is used, otherwise OAuth method.
     * @param testingContext the testing context
     * @param preemptive whether preemptive authentication is enabled: this means that the authentication details are
     * sent in the request header regardless if the server has challenged for authentication or not.
     *
     * @return this instance to allow Method Chaining.
     */
    public T withConfiguredAuth(final TestingContext testingContext, final boolean preemptive) {
        final BasicAuth basicAuth = testingContext.getBasicAuth();
        if (basicAuth.isEnabled()) {
            return this.withBasicAuth(basicAuth.getUsername(), basicAuth.getPassword(), preemptive);
        } else {
            return this.withJWT(testingContext.getOAuthClient().getAccessToken(), preemptive);
        }
    }

    /**
     * Clears any authentication, if set.
     *
     * @return this instance to allow Method Chaining.
     */
    public T clearAuthentication() {
        authenticationProvider = NO_AUTHENTICATION;
        return getThis();
    }

    /**
     * Adds a HTTP header field to the request.
     *
     * @param header the header field.
     * @param value the value of the header field.
     * @return this instance to allow Method Chaining.
     * @throws NullPointerException if {@code header} is {@code null}.
     */
    public T withHeader(final HttpHeader header, final Object value) {
        requireNonNull(header, "The key of the header field must not be null!");
        headers.put(header.toString(), value);
        return getThis();
    }

    /**
     * Adds a HTTP header field to the request.
     *
     * @param headerKey the key of the header field.
     * @param headerValue the value of the header field.
     * @return this instance to allow Method Chaining.
     * @throws NullPointerException if {@code headerKey} is {@code null}.
     */
    public T withHeader(final String headerKey, final Object headerValue) {
        requireNonNull(headerKey, "The key of the header field must not be null!");
        headers.put(headerKey, headerValue);
        return getThis();
    }

    public T withDevopsAuth() {
        return withBasicAuth("devops", "foobar", true);
    }

    /**
     * Enables logging for this request. The actual logging is done in {@code doLog} method which is called before the
     * request is fired.
     *
     * @param logger the logger used to log
     * @param entityType the type that is CRUDed with the request e.g. Thing, Policy, Acl,...
     * @return this instance to allow Method Chaining.
     */
    public T withLogging(final Logger logger, @Nullable final String entityType) {
        this.logger = logger;
        this.entityType = entityType;
        return getThis();
    }

    /**
     * Removes the correlationId and replaces it with explicit correlationId.
     *
     * @return this instance to allow Method Chaining.
     */
    public T withCorrelationId(final CharSequence correlationId) {
        headers.remove(HttpHeader.X_CORRELATION_ID.toString());
        headers.put(HttpHeader.X_CORRELATION_ID.toString(), correlationId.toString());
        return getThis();
    }

    /**
     * Matches the expected HTTP status against a list of allowed HTTP statuses.
     *
     * @param first the first allowed HTTP status.
     * @param furtherStatusCodes further allowed HTTP statuses.
     * @return this instance to allow Method Chaining.
     */
    public T expectingHttpStatus(final HttpStatus first, final HttpStatus... furtherStatusCodes) {
        return expectingStatusCode(isOneOf(first, furtherStatusCodes));
    }

    /**
     * Matches the expected HTTP status based on the given {@code matcher}.
     *
     * @param matcher the expected status matcher.
     * @return this instance to allow Method Chaining.
     */
    public T expectingHttpStatus(final Matcher<Integer> matcher) {
        return expectingStatusCode(matcher);
    }

    /**
     * Matches the expected HTTP status code based on the given {@code matcher}.
     *
     * @param matcher The expected status code matcher.
     * @return this instance to allow Method Chaining.
     */
    public T expectingStatusCode(final Matcher<Integer> matcher) {
        responseSpecification.statusCode(matcher);
        return getThis();
    }

    /**
     * Checks whether the actual HTTP status code is in the OK range (at least 200, at most 299).
     *
     * @return this instance to allow Method Chaining.
     */
    public T expectingStatusCodeSuccessful() {
        responseSpecification.statusCode(StatusCodeSuccessfulMatcher.getInstance());
        return getThis();
    }

    /**
     * Sets bodyMatchers which are used to check the response body of the request. Each matcher is expected to evaluate
     * to {@code true}; otherwise this unit test case will fail.
     *
     * @param bodyMatcher a matcher for checking the response body of the request.
     * @param additionalMatchers additional matcher for the response body.
     * @return this instance to allow Method Chaining.
     * @throws NullPointerException if any argument is {@code null}.
     */
    @SafeVarargs
    public final T expectingBody(final Matcher<String> bodyMatcher, final Matcher<String>... additionalMatchers) {
        checkNotNull(bodyMatcher, "body matcher");
        requireNonNull(additionalMatchers, "The additional body bodyMatchers must not be null!"
                + " If none are required, please use an empty array instead of null or"
                + " do not specify the additional body bodyMatchers at all.");

        bodyMatchers.add(bodyMatcher);
        Collections.addAll(bodyMatchers, additionalMatchers);
        return getThis();
    }

    public final T expectingErrorCode(final String expectedErrorCode) {
        checkNotNull(expectedErrorCode, "expected error code");
        bodyMatchers.add(new BodyContainsErrorCodeMatcher(expectedErrorCode));
        return getThis();
    }

    public T expectingHeaderIsNotPresent(final String headerName) {
        final Matcher matcher = Matchers.not(headerName);
        responseSpecification.header(headerName, matcher);
        return getThis();
    }

    public T expectingHeader(final String name, final String value) {
        responseSpecification.header(name, value);
        return getThis();
    }

    public T expectingHeader(final String name, final Matcher matcher) {
        responseSpecification.header(name, matcher);
        return getThis();
    }

    /**
     * Enables URL-Encoding. If enabled, RestAssured will not only encode the params but also the path which is
     * sometimes not desired.
     *
     * @return this instance to allow Method Chaining.
     */
    public T enableUrlEncoding() {
        responseSpecification.given().urlEncodingEnabled(true);
        return getThis();
    }

    /**
     * Disables URL-Encoding.
     *
     * @return this instance to allow Method Chaining.
     * @see #enableUrlEncoding()
     */
    public T disableUrlEncoding() {
        responseSpecification.given().urlEncodingEnabled(false);
        return getThis();
    }

    /**
     * Provides the possibility to use Awaitility for wait conditions.
     *
     * @param conditionFactory the Awaitility {@link ConditionFactory}
     * @return this instance to allow Method Chaining.
     */
    public T useAwaitility(final ConditionFactory conditionFactory) {
        this.conditionFactory = conditionFactory;
        return getThis();
    }

    /**
     * Adds the specified consumer. It gets notified when {@link #fire()} was called and receives the same Response like
     * {@code fire()} returns.
     *
     * @param responseConsumer the consumer to be added.
     */
    public T registerResponseConsumer(@Nullable final Consumer<Response> responseConsumer) {
        if (null != responseConsumer) {
            responseConsumers.add(responseConsumer);
        }
        return getThis();
    }

    protected long getHeaderBytes() {
        return headers.values().stream().map(Object::toString).mapToLong(HttpVerbMatcher::getBytes).sum();
    }

    protected static long getBytes(final String s) {
        final byte[] bytes = s.getBytes();
        return bytes.length;
    }

    /**
     * Sends the REST request.
     *
     * @return the response of the REST request.
     */
    public Response fire() {
        if (conditionFactory == null) {
            return tryToFire();
        }

        final AtomicReference<Response> responseRef = new AtomicReference<>();
        conditionFactory.untilAsserted(() -> responseRef.set(tryToFire()));
        return responseRef.get();
    }

    private Response tryToFire() {
        // set the headers/parameters on a copy and fire the copy (for multiple firing of same HttpVerbMatcher instance)
        final RequestSpecification originalRequestSpec = responseSpecification.given();
        final ResponseSpecification responseSpecificationCopy = createNewResponseSpec()
                .spec(responseSpecification);

        final RequestSpecification requestSpecCopy = responseSpecificationCopy.request().spec(originalRequestSpec);

        authenticationProvider.accept(requestSpecCopy);

        if (!headers.isEmpty()) {
            requestSpecCopy.headers(headers);
        }

        parameters.forEach(requestSpecCopy::queryParam);

        if (logger != null) {
            doLog(logger, path, entityType);
        }

        final Response result = retry(2, () -> doFire(path, responseSpecificationCopy, bodyMatchers));

        responseConsumers.forEach(responseConsumer -> responseConsumer.accept(result));

        return result;
    }

    private <R> R retry(final int maxRetries, Supplier<R> supplier) {
        if (maxRetries < 1) {
            throw new IllegalArgumentException("maxRetries must be > 0");
        }

        for (int i = 0; i < (maxRetries + 1); i++) {
            if (i != 0) {
                logger.info("Retrying request: Retry #{}", i);
            }
            try {
                return supplier.get();
            } catch (final Exception e) {
                if (!isRetryableException(e)) {
                    throw e;
                } else if (i == maxRetries) {
                    logger.info("Maximum Retries reached, propagating exception", e);
                    throw e;
                } else {
                    logger.info("Catched retryable exception", e);
                }
            }
        }

        throw new IllegalStateException();
    }

    private boolean isRetryableException(final Exception e) {
        if (e instanceof SocketException) {
            return e.getMessage() != null && e.getMessage().startsWith("Connection reset");
        }

        return false;
    }

    /**
     * Subclasses implement this method to perform the actual REST request.
     *
     * @param path path of the HTTP resource.
     * @param responseSpecification the (pre-configured) response specification to use for creating the REST request.
     * @param bodyMatchers matchers for checking the response body of the request.
     * @return the response of the REST request.
     */
    abstract Response doFire(String path, ResponseSpecification responseSpecification,
            List<Matcher<String>> bodyMatchers);

    /**
     * Subclasses implement this method to log some meaningful message before the request is fired.
     *
     * @param logger the logger.
     * @param path the path of the HTTP resource.
     * @param entityType the entity type of the request which can be used in the log message e.g. Thing, Policy.
     */
    protected abstract void doLog(Logger logger, String path, @Nullable String entityType);

}
