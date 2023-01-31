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
package org.eclipse.ditto.testing.common.things;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.things.model.Thing;
import org.junit.rules.ExternalResource;

/**
 * Creates multiple things before the test and deletes them afterwards.
 */
@NotThreadSafe
public final class CreateThingsResource extends ExternalResource {

    private final Supplier<ThingsHttpClient> thingsHttpClientSupplier;
    private final Supplier<Stream<Thing>> thingsToCreateSupplier;

    private List<Thing> createdThings;

    private CreateThingsResource(final Supplier<ThingsHttpClient> thingsHttpClientSupplier,
            final Supplier<Stream<Thing>> thingsToCreateSupplier) {

        this.thingsHttpClientSupplier = thingsHttpClientSupplier;
        this.thingsToCreateSupplier = thingsToCreateSupplier;

        createdThings = null;
    }

    /**
     * Returns a new instance of {@code CreateThingsResource} for the specified arguments.
     *
     * @param thingsHttpClientSupplier supplies the {@link ThingsHttpClient} that is used to create and delete the
     * things.
     * @param thingsToCreateSupplier supplies the things to be created.
     * @return the instance.
     * @throws NullPointerException if any argument is {@code null}.
     */
     public static CreateThingsResource newInstance(final Supplier<ThingsHttpClient> thingsHttpClientSupplier,
             final Supplier<Stream<Thing>> thingsToCreateSupplier) {

        return new CreateThingsResource(checkNotNull(thingsHttpClientSupplier, "thingsHttpClientSupplier"),
                checkNotNull(thingsToCreateSupplier, "thingsToCreateSupplier"));
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        final var thingsToCreate = thingsToCreateSupplier.get();
        createdThings = thingsToCreate.parallel().map(this::createThing).collect(Collectors.toUnmodifiableList());
    }

    private Thing createThing(final Thing thingToCreate) {
        final Thing result;
        if (hasThingId(thingToCreate)) {
            result = putThing(thingToCreate);
        } else {
            result = postThing(thingToCreate);
        }
        return result;
    }

    private static boolean hasThingId(final Thing thingToCreate) {
        final var thingIdOptional = thingToCreate.getEntityId();
        return thingIdOptional.isPresent();
    }

    private Thing putThing(final Thing thing) {
        final var thingsHttpClient = thingsHttpClientSupplier.get();
        final var thingOptional = thingsHttpClient.putThing(thing.getEntityId().orElseThrow(),
                thing,
                CorrelationId.random());
        return thingOptional.orElse(thing);
    }

    private Thing postThing(final Thing thing) {
        final var thingsClient = thingsHttpClientSupplier.get();
        return thingsClient.postThing(thing, CorrelationId.random());
    }

    @Override
    protected void after() {
        final var thingsHttpClient = thingsHttpClientSupplier.get();
        createdThings()
                .parallel()
                .forEach(thing -> thing.getEntityId()
                        .ifPresent(thingId -> thingsHttpClient.deleteThing(thingId, CorrelationId.random())));

        createdThings = null;

        super.after();
    }

    /**
     * Returns the created things.
     *
     * @return a stream of the created things.
     * @throws IllegalArgumentException if this method is called before running a test.
     */
    public Stream<Thing> createdThings() {
        if (null == createdThings) {
            throw new IllegalStateException("The things get only created by running a test.");
        }
        return createdThings.stream();
    }

}
