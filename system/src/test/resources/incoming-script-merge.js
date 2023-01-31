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
 * Processes JSON message in the format
 * <pre>
 *   {
 *    "correlation-id" : <correlation-id>
 *    "reply-to" : <reply-to>
 *    "namespace" : <namespace>
 *    "name" : <name>
 *    "v1" : <value1>
 *    "v2" : <value2>
 *    "v3" : <value3>
 *    }
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

  let correlationId = extMsg['correlation-id'];
  let replyTo = extMsg['reply-to'];
  let namespace = extMsg['namespace'];
  let name = extMsg['name'];
  let group = 'things';
  let channel = 'twin';
  let criterion = 'commands';
  let action = 'merge';
  let path = '/';
  let dittoHeaders = headers;

  let value = {
    features: {
      feature1: {
        properties: {
          value1: parseInt(extMsg['v1'])
        }
      },
      feature2: {
        properties: {
          value2: parseInt(extMsg['v2'])
        }
      },
      feature3: {
        properties: {
          value3: parseInt(extMsg['v3'])
        }
      }
    }
  };

  // set headers for protocols without headers (e.g. MQTT)
  if (!headers['reply-to']) {
    headers['reply-to'] = replyTo;
  }
  if (!headers['correlation-id']) {
    headers['correlation-id'] = correlationId;
  }
  headers['content-type'] = 'application/merge-patch+json';

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

}
