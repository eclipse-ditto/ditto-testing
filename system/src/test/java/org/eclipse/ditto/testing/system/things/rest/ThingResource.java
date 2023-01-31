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
package org.eclipse.ditto.testing.system.things.rest;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.testing.common.ThingJsonProducer;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.things.ThingsHttpClientResource;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.rules.ExternalResource;

@NotThreadSafe
public final class ThingResource extends ExternalResource {

    private final ThingsHttpClientResource thingsHttpClientResource;
    private final Supplier<Thing> thingSupplier;
    private final CorrelationId correlationId;

    private Thing thing;

    private ThingResource(final ThingsHttpClientResource thingsHttpClientResource, final Supplier<Thing> thingSupplier) {
        this.thingsHttpClientResource = checkNotNull(thingsHttpClientResource, "thingsHttpClientResource");
        this.thingSupplier = thingSupplier;
        correlationId = CorrelationId.random().withSuffix("-", ThingResource.class.getSimpleName());
    }

    /**
     * Returns a new instance of {@code ThingResource}.
     *
     * @param thingsHttpClientResource provides the {@link org.eclipse.ditto.testing.common.things.ThingsHttpClient} to
     * be used for creating the thing.
     * @param thingSupplier supplies the thing that will be created by this resource.
     * @return the instance.
     */
    public static ThingResource forThingSupplier(final ThingsHttpClientResource thingsHttpClientResource,
            final Supplier<Thing> thingSupplier) {

        return new ThingResource(thingsHttpClientResource, checkNotNull(thingSupplier, "thingSupplier"));
    }

    /**
     * Returns a new instance of {@code ThingResource}.
     *
     * @param thingsHttpClientResource provides the {@link org.eclipse.ditto.testing.common.things.ThingsHttpClient} to
     * be used for creating the thing.
     * @param thing the thing that will be created by this resource.
     * @return the instance.
     */
    public static ThingResource forThing(final ThingsHttpClientResource thingsHttpClientResource, final Thing thing) {
        checkNotNull(thing, "thing");
        return new ThingResource(thingsHttpClientResource, () -> thing);
    }

    /**
     * Returns a new instance of {@code ThingResource}.
     * The returned instance uses {@link org.eclipse.ditto.testing.common.ThingJsonProducer} for getting the thing to be created.
     *
     * @param thingsHttpClientResource provides the {@link org.eclipse.ditto.testing.common.things.ThingsHttpClient} to
     * be used for creating the thing.
     * @return the instance.
     */
    public static ThingResource fromThingJsonProducer(final ThingsHttpClientResource thingsHttpClientResource) {
        final var thingJsonProducer = new ThingJsonProducer();
        return ThingResource.forThingSupplier(thingsHttpClientResource, thingJsonProducer::getThing);
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        thing = postThing(thingSupplier.get(), correlationId);
    }

    private Thing postThing(final Thing thing, final CorrelationId correlationId) {
        final var thingsHttpClient = thingsHttpClientResource.getThingsClient();
        return thingsHttpClient.postThing(thing, correlationId.withSuffix(".postNewThing"));
    }

    public Thing getThing() {
        if (null == thing) {
            throw new IllegalStateException("The thing becomes only available after running the test.");
        } else {
            return thing;
        }
    }

    public ThingId getThingId() {
        if (null == thing) {
            throw new IllegalStateException("The thing becomes only available after running the test.");
        } else {
            return thing.getEntityId().orElseThrow();
        }
    }

}
