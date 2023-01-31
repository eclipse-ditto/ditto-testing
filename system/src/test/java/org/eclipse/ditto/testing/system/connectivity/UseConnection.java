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
package org.eclipse.ditto.testing.system.connectivity;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Allows to define a ConnectionCategory and a reference to a "modification" that is applied to the default connection
 * category template before the connection is created. The "modification" has to be registered in the map
 * {@link AbstractConnectivityITBase#MODS}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(UseConnection.List.class)
public @interface UseConnection {

    /**
     * @return the identifier of the modification that is applied before the connection is created
     */
    String mod();

    /**
     * @return specifies for which connection category the modification is applied
     */
    ConnectionCategory category();

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface List {

        UseConnection[] value();

    }
}
