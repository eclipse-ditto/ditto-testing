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

# Starts the system-test docker environment (run from docker/). The database Ditto persists to is
# selected by DITTO_DB:
#   DITTO_DB=mongodb   (default) the stack exactly as before: plain `docker-compose` on docker-compose.yml
#                      (+ the auto-loaded docker-compose.override.yml when present), everything on MongoDB.
#   DITTO_DB=postgres  policies/things/connectivity persist to PostgreSQL (docker-compose-postgres.yml);
#                      things-search runs on PostgreSQL too unless SEARCH_BACKEND=mongodb
#                      (docker-compose-postgres-search.yml). See README-postgres.md.
# Postgres-mode environment:
#   SEARCH_BACKEND   "postgres" (default) | "mongodb": backend of things-search.
#   BUILD_IMAGES     set to 1 to (re)build the Ditto service images with the Postgres extension JARs
#                    baked in (`BAKE_POSTGRES_EXTENSIONS=true ./build-images.sh` in DITTO_REPO_DIR) before
#                    starting. Off by default: CI builds the images before calling this script.
#   DITTO_REPO_DIR   ditto worktree used by BUILD_IMAGES=1 (default: ./../../ditto).

set -e

# Resolve everything (compose files, overlays, log files, the DITTO_REPO_DIR default) relative to docker/, so the
# script also works as `docker/start.sh` from the repository root. No-op for CI, which already runs from docker/.
cd "$(dirname "$0")"

DITTO_DB="${DITTO_DB:-mongodb}"

# The compose file basename is assembled WITHOUT the literal "docker-compose" on purpose: ditto's CI
# rewrites this script with `sed 's/docker-compose/docker compose/g'` (compose v1 -> v2 CLI) and would
# otherwise mangle the -f file names below together with the command.
COMPOSE_STEM='docker-''compose'

COMPOSE_FILES=()     # -f flags; stays empty in mongodb mode (plain docker-compose, auto-override, as before)
PULL_SERVICES=()     # empty = pull every image (mongodb mode, as before)
DB_SERVICES=(mongodb)
DB_LABEL="MongoDB"

case "$DITTO_DB" in
  mongodb)
    ;;
  postgres)
    # Explicit -f flags disable compose's auto-loading of docker-compose.override.yml (host ports, Kafka
    # advertised listeners; CI deletes it on purpose), so build the stack explicitly and keep the override
    # only when it is present.
    COMPOSE_FILES=(-f "$COMPOSE_STEM.yml")
    if [ -f "$COMPOSE_STEM.override.yml" ]; then
      COMPOSE_FILES+=(-f "$COMPOSE_STEM.override.yml")
    fi
    COMPOSE_FILES+=(-f "$COMPOSE_STEM-postgres.yml")

    SEARCH_BACKEND="${SEARCH_BACKEND:-postgres}"
    case "$SEARCH_BACKEND" in
      postgres)
        COMPOSE_FILES+=(-f "$COMPOSE_STEM-postgres-search.yml")
        ;;
      mongodb)
        ;;
      *)
        printf "ERROR: SEARCH_BACKEND=<%s> is invalid; expected 'postgres' or 'mongodb'.\n" "$SEARCH_BACKEND" >&2
        exit 1
        ;;
    esac
    printf "Using DITTO_DB=postgres (SEARCH_BACKEND=%s for things-search).\n" "$SEARCH_BACKEND"

    # The Postgres-capable service images are local builds (ditto/build-images.sh tags
    # eclipse/ditto-<svc>:$IMAGE_VERSION, 0-SNAPSHOT by default), so never pull them: only the third-party images.
    export DITTO_VERSION="${DITTO_VERSION:-0-SNAPSHOT}"
    export DOCKER_REGISTRY_NAMESPACE="${DOCKER_REGISTRY_NAMESPACE:-eclipse}"
    PULL_SERVICES=(postgres mongodb oauth ssh mqtt kafka rabbitmq artemis fluentbit)
    DB_SERVICES=(postgres mongodb)
    DB_LABEL="PostgreSQL and MongoDB"
    ;;
  *)
    printf "ERROR: DITTO_DB=<%s> is invalid; expected 'mongodb' or 'postgres'.\n" "$DITTO_DB" >&2
    exit 1
    ;;
esac

function compose {
  docker-compose "${COMPOSE_FILES[@]}" "$@"
}

function assert_success {
  local status=$?
  if [ "$status" -ne 0 ] ; then
    echo "Unsuccessful exit code <$status>. Downing docker-compose."
    compose down
    exit "$status"
  fi
}

function cleanup() {
  printf "Cleanup ...\n\n"
  compose down
}
trap cleanup SIGHUP SIGINT SIGQUIT SIGABRT SIGALRM SIGTERM

# Opt-in local convenience (BUILD_IMAGES=1, postgres mode only): bake the Postgres extension JARs into
# the Ditto service images. CI compiles ditto and runs build-images.sh itself before calling this script.
function build_images {
  local ditto_repo_dir="${DITTO_REPO_DIR:-./../../ditto}"
  local marker_dir="$ditto_repo_dir/internal/utils/postgres-persistence-extension"
  # Fail fast on a wrong ditto worktree: several ditto checkouts/worktrees exist side-by-side and a
  # Mongo-era one lacks this marker directory, so it would silently build images without the extensions.
  if [ ! -d "$marker_dir" ]; then
    printf "ERROR: <%s> does not exist.\n" "$marker_dir" >&2
    printf "DITTO_REPO_DIR=<%s> does not point at a ditto worktree with the Postgres extensions; point it at the feat/postgres-persistance-search worktree.\n" "$ditto_repo_dir" >&2
    return 1
  fi
  # build-images.sh only wraps already-built JARs into images: require a built worktree.
  if [ ! -f "$marker_dir/target/ditto-postgres-persistence-extension-0-SNAPSHOT.jar" ]; then
    printf "ERROR: no built extension JAR in <%s/target>; run (cd %s && mvn install -DskipTests) first.\n" "$marker_dir" "$ditto_repo_dir" >&2
    return 1
  fi
  printf "\nBuilding Ditto service images with baked Postgres extensions from <%s> ...\n\n" "$ditto_repo_dir"
  # IMAGE_VERSION: tag the images with the version compose will look for (DITTO_VERSION, default 0-SNAPSHOT);
  # build-images.sh would otherwise always tag 0-SNAPSHOT.
  (cd "$ditto_repo_dir" && IMAGE_VERSION="$DITTO_VERSION" BAKE_POSTGRES_EXTENSIONS=true ./build-images.sh)
}

function wait_for_postgres {
  local health
  for _ in $(seq 1 30); do
    health="$(docker inspect -f '{{.State.Health.Status}}' "$(compose ps -q postgres)" 2>/dev/null || true)"
    if [ "$health" = "healthy" ]; then
      return 0
    fi
    sleep 2
  done
  echo "PostgreSQL did not become healthy in time." >&2
  return 1
}

# Postgres mode needs service images built with BAKE_POSTGRES_EXTENSIONS=true (the two extension JARs live in
# /opt/ditto/extensions/ inside the image). Check that BEFORE starting anything: with an unbaked image the
# overlays' `include required(classpath(...))` only surfaces in the container log once the stack is up.
# Only `docker image` / `docker run` are used here (CI's sed rewrites the compose command name only).
function check_service_images {
  local svc image
  for svc in policies things things-search connectivity; do
    image="${DOCKER_REGISTRY:-docker.io}/${DOCKER_REGISTRY_NAMESPACE}/${DITTO_SERVICE_PREFIX-ditto-}${svc}:${DITTO_VERSION}"
    if ! docker image inspect "$image" >/dev/null 2>&1; then
      printf "ERROR: image <%s> not found locally.\n" "$image" >&2
      printf "Build it with BAKE_POSTGRES_EXTENSIONS=true ./build-images.sh in the ditto worktree (or BUILD_IMAGES=1 here), see README-postgres.md.\n" >&2
      return 1
    fi
    if ! docker run --rm --entrypoint sh "$image" -c 'ls /opt/ditto/extensions/ditto-postgres-client-extension-*.jar >/dev/null 2>&1'; then
      printf "ERROR: image <%s> has no Postgres extension JARs in /opt/ditto/extensions/ (built without BAKE_POSTGRES_EXTENSIONS=true).\n" "$image" >&2
      printf "Rebuild it with BAKE_POSTGRES_EXTENSIONS=true ./build-images.sh (or BUILD_IMAGES=1 here), see README-postgres.md.\n" >&2
      return 1
    fi
  done
}

# shellcheck disable=SC2317  # the trailing `} || {` handler is kept as in upstream start.sh
{
  (cleanup)
  assert_success $?

  if [ "$DITTO_DB" = "postgres" ] && [ "${BUILD_IMAGES:-0}" = "1" ]; then
    (build_images)
    assert_success $?
  fi

  if [ "$DITTO_DB" = "postgres" ]; then
    (check_service_images)
    assert_success $?
  fi

  printf "\nPulling newest versions of images ...\n\n"
  (compose pull "${PULL_SERVICES[@]}")
  assert_success $?

  printf "\n"
  read -r -p "Waiting for 10 seconds ..." -t 10
  printf "\n"

  printf "\nStarting OAuth Mock ...\n\n"
  (compose up -d oauth)
  assert_success $?

  printf "\nStarting %s ...\n\n" "$DB_LABEL"
  (compose up -d "${DB_SERVICES[@]}")
  assert_success $?

  printf "\nStarting OpenSSH ...\n\n"
  (compose up -d ssh)
  assert_success $?

  printf "\nStarting Message Brokers ...\n\n"
  (compose up -d mqtt kafka rabbitmq artemis)
  assert_success $?

  printf "\nWaiting for 10 seconds ...\n\n"
  sleep 10

  printf "\nStarting Fluent Bit ...\n\n"
  (compose up -d fluentbit)
  assert_success $?

  if [ "$DITTO_DB" = "postgres" ]; then
    printf "\nWaiting for PostgreSQL to become healthy ...\n\n"
    (wait_for_postgres)
    assert_success $?
  fi

  printf "\nStarting Ditto ...\n\n"
  (compose up -d policies &&
  compose up -d things &&
  compose up -d things-search &&
  compose up -d connectivity &&
  compose up -d gateway)
  assert_success $?

  TAG="${TAG:-$(date -Iseconds)}"
  printf "\nAppend container logs to files...\n\n"
  compose logs -f oauth &> "oauth-$TAG.log" &
  if [ "$DITTO_DB" = "postgres" ]; then
    compose logs -f postgres &> "postgres-$TAG.log" &
  fi
  compose logs -f mongodb &> "mongodb-$TAG.log" &
  compose logs -f ssh &> "ssh-$TAG.log" &
  compose logs -f mqtt &> "mqtt-$TAG.log" &
  compose logs -f kafka &> "kafka-$TAG.log" &
  compose logs -f rabbitmq &> "rabbitmq-$TAG.log" &
  compose logs -f artemis &> "artemis-$TAG.log" &
  compose logs -f fluentbit &> "fluentbit-$TAG.log" &
  compose logs -f policies &> "policies-$TAG.log" &
  compose logs -f things &> "things-$TAG.log" &
  compose logs -f things-search &> "things-search-$TAG.log" &
  compose logs -f connectivity &> "connectivity-$TAG.log" &
  compose logs -f gateway &> "gateway-$TAG.log" &

  printf "\n"
  read -r -p "Waiting for 20 seconds ..." -t 20
  printf "\n"

  EXPECTED_CONTAINERS="${DB_SERVICES[*]} mqtt kafka rabbitmq artemis policies things \
  things-search connectivity gateway"
  for CONTAINER in $EXPECTED_CONTAINERS
  do
    # check all expected containers exist, or break build.
    printf "Checking %s ...\n" "$CONTAINER"
    compose exec -T "$CONTAINER" echo -n || exit 1
  done

  # check if fluentbit is running, or break build.
  # fluentbit container can not run any commands inside the container
  compose ps fluentbit
  assert_success $?

  printf "Done."

  exit 0
} || {
  export RETURN_VALUE=$? && echo "Cleanup after error $RETURN_VALUE" && compose down && exit "$RETURN_VALUE"
}
