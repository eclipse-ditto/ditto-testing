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

/**
 * Returns the original message with an additional header new-header-from-payload-mapping.
 */
function mapFromDittoProtocolMsg(
    namespace,
    name,
    group,
    channel,
    criterion,
    subject,
    path,
    dittoHeaders,
    value
) {

    const mappedHeaders = {
        'new-header-from-payload-mapping': name
    };
    const originalMessage = Ditto.buildDittoProtocolMsg(namespace, name, group, channel, criterion, subject, path, dittoHeaders, value);
    return Ditto.buildExternalMsg(
        mappedHeaders,
        JSON.stringify(originalMessage),
        null,
        'application/json'
    );
}
