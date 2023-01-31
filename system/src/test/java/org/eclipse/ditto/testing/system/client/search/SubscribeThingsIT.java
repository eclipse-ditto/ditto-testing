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
package org.eclipse.ditto.testing.system.client.search;


import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.api.Permission;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.SubjectType;
import org.eclipse.ditto.testing.common.ThingsSubjectIssuer;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

/**
 * Integration test for Thing search protocol via ditto-client.
 */
public class SubscribeThingsIT extends AbstractClientIT {

    private static final Policy policy =
            Policy.newBuilder(PolicyId.of(serviceEnv.getDefaultNamespaceName() + ":user.policy"))
                    .forLabel("specialLabel")
                    .setSubject(ThingsSubjectIssuer.DITTO,
                            serviceEnv.getDefaultTestingContext().getOAuthClient().getClientId(),
                            SubjectType.newInstance("arbitrarySubjectType"))
                    .setGrantedPermissions(PoliciesResourceType.policyResource("/"), Permission.WRITE, Permission.READ)
                    .setGrantedPermissions(PoliciesResourceType.thingResource("/"), Permission.WRITE, Permission.READ)
                    .build();

    private static final Thing thing1 = ThingsModelFactory.newThingBuilder()
            .setId(ThingId.of(idGenerator().withRandomName()))
            .setPolicyId(policy.getEntityId().orElseThrow())
            .setAttribute(JsonPointer.of("/test"), JsonValue.of("attribute"))
            .build();

    private static final Thing thing2 = ThingsModelFactory.newThingBuilder()
            .setId(ThingId.of(idGenerator().withRandomName()))
            .setPolicyId(policy.getEntityId().orElseThrow())
            .setAttribute(JsonPointer.of("/test"), JsonValue.of("attribute"))
            .build();

    private static final Thing thing3 = ThingsModelFactory.newThingBuilder()
            .setId(ThingId.of(idGenerator().withRandomName()))
            .setPolicyId(policy.getEntityId().get())
            .setAttribute(JsonPointer.of("/noTest"), JsonValue.of("noAttribute"))
            .build();

    private static final long THREAD_SLEEP_TIME = 5000L;

    private static DittoClient dittoClient;
    private TestSubscriber<List<Thing>> testSubscriber;

    @After
    public void cancelTestSubscription() throws Exception {
        if (testSubscriber != null) {
            final Subscription s = testSubscriber.subscriptions.poll(THREAD_SLEEP_TIME, TimeUnit.MILLISECONDS);
            if (s != null) {
                s.cancel();
            }
        }
        testSubscriber = null;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        final AuthClient user = serviceEnv.getDefaultTestingContext().getOAuthClient();
        dittoClient = newDittoClientV2(user);

        putPolicy(policy)
                .expectingHttpStatus(HttpStatus.CREATED).fire();

        final var acksHeaderKey = "requested-acks";
        final var acksHeaderVal = "[\"search-persisted\"]";
        putThing(2, thing1, JsonSchemaVersion.V_2)
                .withHeader(acksHeaderKey, acksHeaderVal)
                .expectingStatusCodeSuccessful()
                .fire();

        putThing(2, thing2, JsonSchemaVersion.V_2)
                .withHeader(acksHeaderKey, acksHeaderVal)
                .expectingStatusCodeSuccessful()
                .fire();

        putThing(2, thing3, JsonSchemaVersion.V_2)
                .withHeader(acksHeaderKey, acksHeaderVal)
                .expectingStatusCodeSuccessful()
                .fire();
    }

    @AfterClass
    public static void tearDown() {
        if (dittoClient != null) {
            try {
                deleteThing(2, thing1.getEntityId().orElseThrow()).fire();
                deleteThing(2, thing2.getEntityId().orElseThrow()).fire();
                deleteThing(2, thing3.getEntityId().orElseThrow()).fire();
                deletePolicy(policy.getEntityId().orElseThrow()).fire();
            } finally {
                shutdownClient(dittoClient);
            }
        }
    }

    @Test
    public void subscribeViaPublisher() throws InterruptedException {
        final Publisher<List<Thing>> listPublisher = dittoClient.twin().search().publisher(search ->
                search.filter("exists(attributes/test)")
                        .options("size(3)")
                        .initialDemand(55)
                        .demand(22));
        testSubscriber = new TestSubscriber<>();
        listPublisher.subscribe(testSubscriber);
        final Subscription subscription =
                checkNotNull(testSubscriber.subscriptions.poll(THREAD_SLEEP_TIME, TimeUnit.MILLISECONDS));
        subscription.request(1L);
        assertThat(testSubscriber.elements.poll(THREAD_SLEEP_TIME, TimeUnit.MILLISECONDS))
                .contains(thing1).contains(thing2).doesNotContain(thing3);
    }

    @Test
    public void subscribeViaStream() {

        final Stream<Thing> searchResultStream = dittoClient.twin().search().stream(search ->
                search.filter("exists(attributes/test)")
                        .options("size(3)")
                        .initialDemand(55)
                        .demand(22));
        assertThat(searchResultStream.map(thing ->
                thing.getEntityId()
                        .orElseThrow(() -> new AssertionError("No thing ID in search result: " + thing))))
                .contains(thing1.getEntityId().orElseThrow())
                .contains(thing2.getEntityId().orElseThrow())
                .doesNotContain(thing3.getEntityId().orElseThrow());
    }

    @Test
    public void subscribeViaStreamWithoutOptions() {

        final Stream<Thing> searchResultStream = dittoClient.twin().search().stream(search ->
                search.filter("exists(attributes/test)")
                        .initialDemand(55)
                        .demand(22));
        assertThat(searchResultStream.map(thing -> thing.getEntityId().orElseThrow(AssertionError::new)))
                .contains(thing1.getEntityId().orElseThrow())
                .contains(thing2.getEntityId().orElseThrow())
                .doesNotContain(thing3.getEntityId().orElseThrow());
    }

    @Test
    public void subscribeViaStreamWithoutPageOptions() {

        final Stream<Thing> searchResultStream = dittoClient.twin().search().stream(search ->
                search.filter(null));
        assertThat(searchResultStream.map(thing -> thing.getEntityId().orElseThrow(AssertionError::new)))
                .contains(thing1.getEntityId().orElseThrow())
                .contains(thing2.getEntityId().orElseThrow())
                .contains(thing3.getEntityId().orElseThrow());
    }

    @Test
    public void subscribeViaAkkaStream() {
        final String filter = "or(exists(attributes/test))";
        final ActorSystem system = ActorSystem.create("thing-search");
        try {
            Source<List<Thing>, NotUsed> things = Source.fromPublisher(
                    dittoClient.twin().search().publisher(searchQueryBuilder -> searchQueryBuilder.filter(filter)));
            things.flatMapConcat(Source::from)
                    .toMat(Sink.seq(), Keep.right())
                    .run(ActorMaterializer.create(system))
                    .thenAccept(t -> {
                        System.out.println(t);
                        assertThat(t).contains(thing1, thing2).doesNotContain(thing3);
                        assertThat(t.size()).isEqualTo(2);
                    })
                    .toCompletableFuture()
                    .join();
        } finally {
            system.terminate();
        }
    }

    private static final class TestSubscriber<T> implements Subscriber<T> {

        private final BlockingQueue<Subscription> subscriptions = new LinkedBlockingQueue<>();
        private final BlockingQueue<T> elements = new LinkedBlockingQueue<>();
        private final BlockingQueue<Throwable> errors = new LinkedBlockingQueue<>();
        private final AtomicInteger completeCounter = new AtomicInteger(0);

        @Override
        public void onSubscribe(final Subscription s) {
            subscriptions.add(s);
        }

        @Override
        public void onNext(final T t) {
            elements.add(t);
        }

        @Override
        public void onError(final Throwable t) {
            errors.add(t);
        }

        @Override
        public void onComplete() {
            completeCounter.incrementAndGet();
        }
    }
}
