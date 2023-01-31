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
 * Process messages in JSON format:
 * <pre> {@code {
 *  "namespace": "org.eclipse.ditto",
     *  "deviceid": "test-device",
     *  "action": "modify",
     *  "path": "/attributes/foo",
     *  "value": 42
     * }}
 * </pre>
 */
function mapToDittoProtocolMsg(
    headers,
    textPayload,
    bytePayload,
    contentType
) {

    let extMsg;
    if (textPayload) {
      extMsg = JSON.parse(textPayload);
    } else {
      // for MQTT messages, content type is lost because there is no header. Parse the binary payload instead.
      // interpret array as ascii.
      let bytes = Array.prototype.slice.call(new Int8Array(bytePayload));
      let text = "";
      for (let i = 0; i < bytes.length; ++i) {
        text += String.fromCharCode(bytes[i]);
      }
      extMsg = JSON.parse(text);
    }

  let keepAsIs = extMsg.headers && extMsg.headers["keep-as-is"];
  if(keepAsIs === "true") {
    Object.assign(extMsg.headers, headers);
    return extMsg;
  }

    let namespace = extMsg.namespace;
    let name = extMsg.deviceid;
    let group = 'things';
    let channel = 'twin';
    let criterion = 'commands';
    let action = extMsg.action;
    let path = extMsg.path;
    let dittoHeaders = headers;
    let value = extMsg.value;

    if (extMsg['headers']) {
      // set headers for protocols without headers (e. g., MQTT)
      let extMsgHeaders = extMsg['headers'];
      let replyTo = 'reply-to';
      let correlationId = 'correlation-id';
      if (!headers[replyTo]) {
        headers[replyTo] = extMsgHeaders[replyTo];
      }
      if (!headers[correlationId]) {
        headers[correlationId] = extMsgHeaders[correlationId];
      }
    }
    let multiplier = resolveMultiplier(headers, extMsg['headers']);
    if (multiplier === 0) {
        return null;
    } else if (multiplier === 1) {
        // single result mode
        return Ditto.buildDittoProtocolMsg(
            namespace,
            name,
            group,
            channel,
            criterion,
            action,
            path,
            dittoHeaders,
            value
        );
    } else {
        // array result mode
        let arr = [];
        for (let i = 0; i < multiplier; i++) {
            let theName = name + "-" + i;
            let newValue = Object.assign({}, extMsg.value);
            newValue['thingId'] = namespace +':' + theName;
            arr.push(Ditto.buildDittoProtocolMsg(
                namespace,
                theName,
                group,
                channel,
                criterion,
                action,
                path,
                dittoHeaders,
                newValue
            ));
        }
        return arr;
    }
}

function resolveMultiplier(headers, extMsgHeaders) {
    if (headers['multiplier']) {
        return parseInt(headers['multiplier']);
    } else if (extMsgHeaders && extMsgHeaders['multiplier']) {
        return parseInt(extMsgHeaders['multiplier']);
    }
    return 1;
}
