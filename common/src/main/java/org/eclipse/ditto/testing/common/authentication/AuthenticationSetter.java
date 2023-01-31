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
package org.eclipse.ditto.testing.common.authentication;

import io.restassured.specification.AuthenticationSpecification;
import io.restassured.specification.RequestSpecification;

/**
 * Specifies an authentication scheme when performing a REST assured request.
 */
@FunctionalInterface
public interface AuthenticationSetter {

    /**
     * Specifies an authentication scheme when performing a REST assured request.
     *
     * @param authenticationSpec provides the means to apply an authentication scheme.
     * @return the request specification to further specify the REST assured request.
     */
    RequestSpecification applyAuthenticationSetting(AuthenticationSpecification authenticationSpec);

}
