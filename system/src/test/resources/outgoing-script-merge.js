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
 * Produces messages in text format {@code <correlation-id>:<namespace>:<name>:<status>}
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
  value,
  status
) {

  let correlationId = dittoHeaders['correlation-id'];
  let headers = {};
  headers['correlation-id'] = correlationId;
  headers['reply-to'] = dittoHeaders['reply-to'];
  headers['new-header'] = name;
  let extMessage = {
    'correlationId': correlationId,
    'namespace': namespace,
    'id': name,
    'status': status
  };

  let contentType = 'application/json';
  return Ditto.buildExternalMsg(
    headers,
    JSON.stringify(extMessage),
    null,
    contentType
  );
}
