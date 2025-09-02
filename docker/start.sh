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
    echo "Unsuccessful exit code <$status>. Downing docker-compose."
    docker-compose down
    exit $status
  fi
}

function cleanup() {
  printf "Cleanup ...\n\n"
  docker-compose down
}
trap cleanup SIGHUP SIGINT SIGQUIT SIGABRT SIGALRM SIGTERM

{
  (cleanup)
  assert_success $?

  printf "\nPulling newest versions of images ...\n\n"
  (docker-compose pull)
  assert_success $?

  printf "\n"
  read -r -p "Waiting for 10 seconds ..." -t 10
  printf "\n"

  printf "\nStarting OAuth Mock ...\n\n"
  (docker-compose up -d oauth)
  assert_success $?

  printf "\nStarting MongoDB ...\n\n"
  (docker-compose up -d mongodb)
  assert_success $?

  printf "\nStarting OpenSSH ...\n\n"
  (docker-compose up -d ssh)
  assert_success $?

  printf "\nStarting Message Brokers ...\n\n"
  (docker-compose up -d mqtt kafka rabbitmq artemis)
  assert_success $?

  printf "\nWaiting for 10 seconds ...\n\n"
  sleep 10

  printf "\nStarting Fluent Bit ...\n\n"
  (docker-compose up -d fluentbit)
  assert_success $?

  printf "\nStarting Ditto ...\n\n"
  (docker-compose up -d policies &&
  docker-compose up -d things &&
  docker-compose up -d things-search &&
  docker-compose up -d connectivity &&
  docker-compose up -d gateway)
  assert_success $?

  TAG="${TAG:-$(date -Iseconds)}"
  printf "\nAppend container logs to files...\n\n"
  docker-compose logs -f oauth &> "oauth-$TAG.log" &
  docker-compose logs -f mongodb &> "mongodb-$TAG.log" &
  docker-compose logs -f ssh &> "ssh-$TAG.log" &
  docker-compose logs -f mqtt &> "mqtt-$TAG.log" &
  docker-compose logs -f kafka &> "kafka-$TAG.log" &
  docker-compose logs -f rabbitmq &> "rabbitmq-$TAG.log" &
  docker-compose logs -f artemis &> "artemis-$TAG.log" &
  docker-compose logs -f fluentbit &> "fluentbit-$TAG.log" &
  docker-compose logs -f policies &> "policies-$TAG.log" &
  docker-compose logs -f things &> "things-$TAG.log" &
  docker-compose logs -f things-search &> "things-search-$TAG.log" &
  docker-compose logs -f connectivity &> "connectivity-$TAG.log" &
  docker-compose logs -f gateway &> "gateway-$TAG.log" &

  printf "\n"
  read -r -p "Waiting for 20 seconds ..." -t 20
  printf "\n"

  EXPECTED_CONTAINERS="mongodb mqtt kafka rabbitmq artemis policies things \
  things-search connectivity gateway"
  for CONTAINER in $EXPECTED_CONTAINERS
  do
    # check all expected containers exist, or break build.
    printf "Checking %s ...\n" $CONTAINER
    docker-compose exec -T $CONTAINER echo -n || exit 1
  done

  # check if fluentbit is running, or break build.
  # fluentbit container can not run any commands inside the container
  docker-compose ps fluentbit
  assert_success $?

  printf "Done."

  exit 0
} || {
  export RETURN_VALUE=$? && echo "Cleanup after error $RETURN_VALUE" && docker-compose down && exit $RETURN_VALUE
}
