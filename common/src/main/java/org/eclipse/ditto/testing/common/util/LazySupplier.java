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
package org.eclipse.ditto.testing.common.util;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import javax.annotation.Nullable;

/**
 * Supplier which supports lazy loading a value.
 * 
 * @param <T> the type of the value returned by the supplier.
 */
public final class LazySupplier<T> implements Supplier<T> {

    @Nullable
    private final Supplier<T> loader;
    private final ConcurrentMap<Class<?>, T> valueHolder = new ConcurrentHashMap<>(1);

    private LazySupplier(@Nullable final Supplier<T> loader) {
        this.loader = loader;
    }

    @Override
    public T get() {
        if (loader == null) {
            // maybe initialization was done by an externalLoader, see #get(Supplier), thus we do NOT just return null
            return valueHolder.get(LazySupplier.class);
        }

        return valueHolder.computeIfAbsent(LazySupplier.class, k -> loader.get());
    }

    public T get(final Supplier<T> externalLoader) {
        requireNonNull(externalLoader);

        return valueHolder.computeIfAbsent(LazySupplier.class, k -> externalLoader.get());
    }

    public static <T> Supplier<T> fromLoader(final Supplier<T> loader) {
        requireNonNull(loader);

        return new LazySupplier<>(loader);
    }

    public static <T> LazySupplier<T> fromScratch() {
        return new LazySupplier<>(null);
    }
}
