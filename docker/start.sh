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
#                      (docker-compose-postgres-search.yml). Only the database(s) actually used are
#                      started: postgres alone, plus mongodb only with SEARCH_BACKEND=mongodb.
#                      See README-postgres.md.
# Postgres-mode environment:
#   SEARCH_BACKEND   "postgres" (default) | "mongodb": backend of things-search.
#   BUILD_IMAGES     set to 1 to (re)build the Ditto service images with the Postgres extension JARs
#                    baked in (`BAKE_POSTGRES_EXTENSIONS=true ./build-images.sh` in DITTO_REPO_DIR) before
#                    starting. Off by default: CI builds the images before calling this script.
#   DITTO_REPO_DIR   ditto worktree used by BUILD_IMAGES=1 (default: ./../../ditto).
#   DITTO_START_GRACE_SECONDS
#                    seconds (default 10) the Ditto services get after `up` before every Ditto container must be
#                    `running`; one that exited meanwhile aborts the start with its last log lines.

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

    # Start ONLY the database(s) in use: postgres always, mongodb solely for things-search-on-Mongo.
    # (The overlays replace the services' `depends_on: [mongodb]` with `!override`, so `compose up
    # <service>` does not pull mongodb in behind our back either.)
    DB_SERVICES=(postgres)
    DB_LABEL="PostgreSQL"
    SEARCH_BACKEND="${SEARCH_BACKEND:-postgres}"
    case "$SEARCH_BACKEND" in
      postgres)
        COMPOSE_FILES+=(-f "$COMPOSE_STEM-postgres-search.yml")
        ;;
      mongodb)
        DB_SERVICES+=(mongodb)
        DB_LABEL="PostgreSQL and MongoDB (things-search)"
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
    PULL_SERVICES=("${DB_SERVICES[@]}" oauth ssh mqtt kafka rabbitmq artemis fluentbit)
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
  # build-images.sh only wraps already-built JARs into images: require a built worktree. It then refuses to bake
  # a shaded extension JAR whose classes differ from its module JAR (stale after an incremental build), so this
  # path inherits that guard; the fix it names is the same `clean install`.
  if [ ! -f "$marker_dir/target/ditto-postgres-persistence-extension-0-SNAPSHOT.jar" ]; then
    printf "ERROR: no built extension JAR in <%s/target>; run (cd %s && mvn clean install -DskipTests -DskipITs) first.\n" "$marker_dir" "$ditto_repo_dir" >&2
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

# The Ditto services `compose up` starts below, in start order.
DITTO_SERVICES=(policies things things-search connectivity gateway)

# A Ditto service that dies right after `up` (a config or extension error at boot) leaves the stack looking
# started: the container check at the end only proves that a container exists at that instant, and the
# `read -t` waits return at once without a TTY (CI, scripted runs). Observed on stale extension JARs:
# connectivity exited six seconds after start and the run went on. So give the JVMs DITTO_START_GRACE_SECONDS
# (default 10) to fail, then require every Ditto container to be `running`; a dead one is reported with its
# last 40 log lines and the caller downs the stack (assert_success). `ps -a` also lists exited containers.
function check_ditto_services_running {
  local svc id status
  local dead=()
  local grace="${DITTO_START_GRACE_SECONDS:-10}"
  sleep "$grace"
  for svc in "${DITTO_SERVICES[@]}"; do
    id="$(compose ps -q -a "$svc" 2>/dev/null | head -n 1)"
    if [ -z "$id" ]; then
      status="no container"
    else
      status="$(docker inspect -f '{{.State.Status}} (exit code {{.State.ExitCode}})' "$id" 2>/dev/null || echo "unknown")"
    fi
    case "$status" in
      running*)
        printf "%s is running.\n" "$svc"
        ;;
      *)
        dead+=("$svc")
        {
          printf "\nERROR: Ditto service <%s> is not running %s s after start: %s. Last 40 log lines:\n" \
            "$svc" "$grace" "$status"
          if [ -n "$id" ]; then
            docker logs --tail 40 "$id" 2>&1 | sed 's/^/  | /'
          fi
        } >&2
        ;;
    esac
  done
  if [ "${#dead[@]}" -gt 0 ]; then
    printf "\nERROR: Ditto service(s) died right after start: %s. Full logs: <service>-%s.log in docker/.\n" \
      "${dead[*]}" "$TAG" >&2
    return 1
  fi
  return 0
}

# Postgres mode needs service images built with BAKE_POSTGRES_EXTENSIONS=true (the two extension JARs live in
# /opt/ditto/extensions/ inside the image). Check that BEFORE starting anything: with an unbaked image the
# overlays' `include required(classpath(...))` only surfaces in the container log once the stack is up.
# Only `docker image` / `docker run` are used here (CI's sed rewrites the compose command name only).
# On failure the full explanation is printed LAST (after the compose warnings, which cannot be silenced
# from here) so it is the first thing the reader sees when scrolling up from the prompt.
function check_service_images {
  local svc image missing=() unbaked=()
  for svc in policies things things-search connectivity; do
    image="${DOCKER_REGISTRY:-docker.io}/${DOCKER_REGISTRY_NAMESPACE}/${DITTO_SERVICE_PREFIX-ditto-}${svc}:${DITTO_VERSION}"
    if ! docker image inspect "$image" >/dev/null 2>&1; then
      missing+=("$image")
    elif ! docker run --rm --entrypoint sh "$image" -c 'ls /opt/ditto/extensions/ditto-postgres-client-extension-*.jar >/dev/null 2>&1'; then
      unbaked+=("$image")
    fi
  done
  if [ "${#missing[@]}" -eq 0 ] && [ "${#unbaked[@]}" -eq 0 ]; then
    return 0
  fi

  {
    printf "\n"
    printf "=====================================================================================\n"
    printf " start.sh ABORTED before starting anything: DITTO_DB=postgres needs local Ditto images\n"
    printf "=====================================================================================\n"
    printf "\n"
    printf "In postgres mode the Ditto service images are NEVER pulled from a registry: they must be\n"
    printf "built locally from a ditto worktree that contains the PostgreSQL extensions, with the\n"
    printf "extension JARs baked into /opt/ditto/extensions/ (BAKE_POSTGRES_EXTENSIONS=true).\n"
    printf "Only the third-party images (postgres, mongodb, brokers, ...) are pulled.\n"
    printf "\n"
    printf "Expected image names (DOCKER_REGISTRY=%s, DOCKER_REGISTRY_NAMESPACE=%s, DITTO_VERSION=%s):\n" \
      "${DOCKER_REGISTRY:-docker.io}" "$DOCKER_REGISTRY_NAMESPACE" "$DITTO_VERSION"
    if [ "${#missing[@]}" -gt 0 ]; then
      printf "\n  NOT FOUND locally (was it never built, or removed by a docker prune / cleanup?):\n"
      printf "    - %s\n" "${missing[@]}"
    fi
    if [ "${#unbaked[@]}" -gt 0 ]; then
      printf "\n  FOUND, but built WITHOUT the Postgres extension JARs (no /opt/ditto/extensions/*.jar):\n"
      printf "    - %s\n" "${unbaked[@]}"
    fi
    printf "\n"
    printf "How to fix (pick one):\n"
    printf "\n"
    printf "  a) Build the images in a ditto worktree on the feat/postgres-persistance-search branch\n"
    printf "     (or any branch containing internal/utils/postgres-persistence-extension):\n"
    printf "\n"
    printf "       cd /path/to/ditto\n"
    printf "       mvn clean install -DskipTests -DskipITs       # 'clean' matters: an incremental install can leave\n"
    printf "       BAKE_POSTGRES_EXTENSIONS=true ./build-images.sh  # the shaded extension JARs stale (the script refuses them)\n"
    printf "\n"
    printf "     Prefix IMAGE_VERSION=%s if you overrode DITTO_VERSION.\n" "$DITTO_VERSION"
    printf "\n"
    printf "  b) Let this script build them from an already-compiled ditto worktree:\n"
    printf "\n"
    printf "       DITTO_DB=postgres BUILD_IMAGES=1 DITTO_REPO_DIR=/path/to/ditto ./start.sh\n"
    printf "\n"
    printf "  c) If the images exist under another tag or namespace, point the script at them:\n"
    printf "\n"
    printf "       DITTO_DB=postgres DITTO_VERSION=<tag> DOCKER_REGISTRY_NAMESPACE=<ns> ./start.sh\n"
    printf "\n"
    printf "Details: docker/README-postgres.md (section \"Prerequisites\").\n"
    printf "\n"
  } >&2
  return 1
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
    # Nothing has been started yet (cleanup ran above), so a plain exit is enough: assert_success's
    # extra `compose down` would only bury the explanation under another page of compose warnings.
    (check_service_images) || exit 1
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
  for DB_SERVICE in "${DB_SERVICES[@]}"; do
    compose logs -f "$DB_SERVICE" &> "$DB_SERVICE-$TAG.log" &
  done
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

  if [ "$DITTO_DB" = "postgres" ]; then
    printf "\nWaiting %s seconds, then checking that every Ditto service is still running ...\n\n" \
      "${DITTO_START_GRACE_SECONDS:-10}"
    (check_ditto_services_running)
    assert_success $?
  fi

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
