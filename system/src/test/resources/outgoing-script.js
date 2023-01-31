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
 * Produces messages in JSON format:
 * <pre> {@code {
 *  "namespace": "org.eclipse.ditto",
     *  "deviceid": "test-device",
     *  "action": "modified",
     *  "path": "/attributes/foo",
     *  "value": 42
     * }}
 * </pre>
 */
function mapFromDittoProtocolMsg(
    namespace,
    name,
    group,
    channel,
    criterion,
    action,
    path,
    dittoHeaders,
    value
) {

    let headers = {};
    let extMessage = {};
    headers['correlation-id'] = dittoHeaders['correlation-id'];
    headers['reply-to'] = dittoHeaders['reply-to'];
    headers['new-header'] = name;
    extMessage.namespace = namespace;
    extMessage.deviceid = name;
    extMessage.action = action;
    extMessage.path = path;
    extMessage.value = value;
    let contentType = 'application/json';

    let multiplier = dittoHeaders['multiplier'] ? parseInt(dittoHeaders['multiplier']) : 1;
    if (multiplier === 0) {
        return null;
    } else if (multiplier === 1) {
        // single result mode
        return Ditto.buildExternalMsg(
            headers,
            JSON.stringify(extMessage),
            null,
            contentType
        );
    } else {
        // array result mode
        let arr = [];
        for (let i = 0; i < multiplier; i++) {
            let newHeaders = Object.assign({}, headers);
            newHeaders['index'] = i.toString();
            arr.push(Ditto.buildExternalMsg(
                newHeaders,
                JSON.stringify(extMessage),
                null,
                contentType
            ));
        }
        return arr;
    }
}
