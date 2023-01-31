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

set -e

function assert_success {
  local status=$?
  if [ $status -ne 0 ] ; then
    echo "Unsuccessful exit code <$status>. Downing $DOCKER_COMPOSE."
    $DOCKER_COMPOSE down
    exit $status
  fi
}

function cleanup() {
  printf "Cleanup ...\n\n"
  $DOCKER_COMPOSE down
}
trap cleanup SIGHUP SIGINT SIGQUIT SIGABRT SIGALRM SIGTERM

{
  export DOCKER_COMPOSE_COMMAND=${DOCKER_COMPOSE_COMMAND:-docker-compose}
  export DOCKER_COMPOSE="$DOCKER_COMPOSE_COMMAND -f docker-compose.yml"

  (cleanup)
  assert_success $?

  printf "\nPulling newest versions of images ...\n\n"
  ($DOCKER_COMPOSE pull)
  assert_success $?

  printf "\nStarting OAuth Mock ...\n\n"
  ($DOCKER_COMPOSE up -d oauth)
  assert_success $?

  printf "\nStarting MongoDB ...\n\n"
  ($DOCKER_COMPOSE up -d mongodb)
  assert_success $?

  printf "\nWaiting for 10 seconds ...\n\n"
  sleep 10

  printf "\nStarting Ditto ...\n\n"
  ($DOCKER_COMPOSE up -d policies &&
  $DOCKER_COMPOSE up -d things &&
  $DOCKER_COMPOSE up -d things-search &&
  $DOCKER_COMPOSE up -d connectivity &&
  $DOCKER_COMPOSE up -d gateway)
  assert_success $?

  TAG="${TAG:-$(date -Iseconds)}"
  printf "\nAppend container logs to files...\n\n"
  $DOCKER_COMPOSE logs -f oauth &> "oauth-$TAG.log" &
  $DOCKER_COMPOSE logs -f mongodb &> "mongodb-$TAG.log" &
  $DOCKER_COMPOSE logs -f policies &> "policies-$TAG.log" &
  $DOCKER_COMPOSE logs -f things &> "things-$TAG.log" &
  $DOCKER_COMPOSE logs -f things-search &> "things-search-$TAG.log" &
  $DOCKER_COMPOSE logs -f connectivity &> "connectivity-$TAG.log" &
  $DOCKER_COMPOSE logs -f gateway &> "gateway-$TAG.log" &

  printf "\n"
  read -r -p "Waiting for 20 seconds ..." -t 20
  printf "\n"

  EXPECTED_CONTAINERS="mongodb policies things things-search connectivity gateway"
  for CONTAINER in $EXPECTED_CONTAINERS
  do
    # check all expected containers exist, or break build.
    printf "Checking %s ...\n" $CONTAINER
    $DOCKER_COMPOSE exec -T $CONTAINER echo -n || exit 1
  done

  printf "Done."

  exit 0
} || {
  export RETURN_VALUE=$? && echo "Cleanup after error $RETURN_VALUE" && $DOCKER_COMPOSE down && exit $RETURN_VALUE
}
