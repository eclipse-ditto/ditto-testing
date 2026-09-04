#!/usr/bin/env bash

# Copyright (c) 2023 Contributors to the Eclipse Foundation
#
# See the NOTICE file(s) distributed with this work for additional
# information regarding copyright ownership.
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# http://www.eclipse.org/legal/epl-2.0
#
# SPDX-License-Identifier: EPL-2.0

# Tears down the environment started by start.sh. Honours the same DITTO_DB selector (mongodb default |
# postgres) so the SAME compose file stack is downed as was started: `down` on the explicit four-file stack
# removes the overlay-declared resources deterministically (--remove-orphans would also catch a stray
# `postgres` container, but only by label matching). Always stop with the DITTO_DB you started with.

set -e

# Resolve the compose files relative to docker/ (see start.sh).
cd "$(dirname "$0")"

DITTO_DB="${DITTO_DB:-mongodb}"

# Spelled without the literal "docker-compose" on purpose (CI's `sed 's/docker-compose/docker compose/g'`
# must not mangle the file names): see start.sh.
COMPOSE_STEM='docker-''compose'
COMPOSE_FILES=()

case "$DITTO_DB" in
  mongodb)
    ;;
  postgres)
    COMPOSE_FILES=(-f "$COMPOSE_STEM.yml")
    if [ -f "$COMPOSE_STEM.override.yml" ]; then
      COMPOSE_FILES+=(-f "$COMPOSE_STEM.override.yml")
    fi
    # Always include the search overlay regardless of the SEARCH_BACKEND used at start time: downing a
    # service that was never started is a no-op, so one stop tears down either variant.
    COMPOSE_FILES+=(-f "$COMPOSE_STEM-postgres.yml" -f "$COMPOSE_STEM-postgres-search.yml")
    ;;
  *)
    printf "ERROR: DITTO_DB=<%s> is invalid; expected 'mongodb' or 'postgres'.\n" "$DITTO_DB" >&2
    exit 1
    ;;
esac

# can be used for debugging purpose
# docker-compose "${COMPOSE_FILES[@]}" logs --no-color &> docker.log

docker-compose "${COMPOSE_FILES[@]}" down --volumes --remove-orphans
