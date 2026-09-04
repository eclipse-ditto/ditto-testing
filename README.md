# Eclipse Ditto™ Testing

This repository contains the "system tests" of Eclipse Ditto.  
They ensure that on API level no regressions happen, that APIs stay stable and that added functionality is tested 
from a user perspective.

## How to run tests in IntelliJ

System tests require the following components to run.
1. Containers required by Ditto stack (MongoDB) and by connectivity tests (Mosquitto, Artemis etc.).
2. A working Ditto stack, either as docker stack or launched from IntelliJ.
3. A local [authorization server mock](#authorization-server-mock).

### Start MongoDB and message broker containers

If you want to launch Ditto from IntelliJ, start the MongoDB container:
```bash
cd docker
docker-compose up -d mongodb
```
If you want to run Ditto as a docker stack, simply run `docker/start.sh`. MongoDB is included.

Connectivity tests require message brokers. Start them thus:
```bash
cd docker

# AMQP broker
docker-compose up -d artemis

# MQTT broker
docker-compose up -d mqtt

# Kafka broker
docker-compose up -d kafka

# RabbitMQ broker
docker-compose up -d rabbitmq
```
The brokers do consume system resources. Start them on a by-need basis.

### Run Ditto

To run Ditto as a docker stack, simply execute `docker/start.sh`. MongoDB is included.

To launch Ditto from IntelliJ, in debug mode for instance, run 
[`Policies`](intelliJRunConfigurations/Policies%20for%20test.run.xml) first and the other required
services after. A launch sequence could look like this:
1. [`Policies`](intelliJRunConfigurations/Policies%20for%20test.run.xml)
2. [`Things`](intelliJRunConfigurations/Things%20for%20test.run.xml)
3. [`Search`](intelliJRunConfigurations/ThingsSearch%20for%20test.run.xml)
4. [`Connectivity`](intelliJRunConfigurations/Connectivity%20for%20test.run.xml)
5. [`Gateway`](intelliJRunConfigurations/Gateway%20for%20test.run.xml)

Or, simply make use of the [multirun plugin](https://plugins.jetbrains.com/plugin/7248-multirun) for which a 
[run configuration](intelliJRunConfigurations/Ditto4test.run.xml) also is available.

### Run Ditto on PostgreSQL persistence

Ditto's persistence layer is pluggable, so the system tests can run against a Ditto stack that persists
things/policies/connectivity to PostgreSQL. things-search's backend is **selectable** (PostgreSQL by
default, or MongoDB). The Postgres extension JARs are baked into the service images (built from a ditto
worktree on the `feat/postgres-persistance-search` branch with `BAKE_POSTGRES_EXTENSIONS=true ./build-images.sh`).

**Docker mode (CI-style):** the same scripts as for MongoDB with the database selected by `DITTO_DB`:
`DITTO_DB=postgres docker/start.sh` (default `SEARCH_BACKEND=postgres`; set `SEARCH_BACKEND=mongodb` for the
persistence-on-PG / search-on-Mongo split), `DITTO_DB=postgres docker/stop.sh`, and the tests with
`-Dtest.environment=docker-compose -Dpersistence.backend=postgres` — see
[docker/README-postgres.md](docker/README-postgres.md).

**IntelliJ mode (docker optional):** run Ditto from IntelliJ with only infrastructure in docker:

1. Start the `postgres` container (either the `Postgres for test`
   [run config](intelliJRunConfigurations/Postgres%20for%20test.run.xml), or
   `docker-compose -f docker-compose.yml -f docker-compose.override.yml -f docker-compose-postgres.yml up -d postgres`
   from `docker/`), plus `oauth` and the brokers exactly as in the sections above. Start `mongodb` only if
   you launch ThingsSearch on Mongo. Do **not** also run ditto's own `deployment/postgres-local` stack — it
   binds the same host `5432`.
2. Launch the `(Postgres)` run configs (`Policies`, `Things`, `ThingsSearch`, `Connectivity` — all
   `for test (Postgres)` — plus the unchanged `Gateway for test`), or the
   [`Ditto for test (Postgres)`](intelliJRunConfigurations/Ditto4test%20%28Postgres%29.run.xml) compound
   (needs the [Multirun](https://plugins.jetbrains.com/plugin/7248-multirun) plugin). These are imported
   into the ditto project and put the r2dbc modules on the classpath via the `ditto-ide-postgres-launcher`
   module. Selecting the search backend in this mode = launching `ThingsSearch for test (Postgres)` vs
   the Mongo `ThingsSearch for test`.
3. Run the tests from the host against the plain `local` environment plus the Postgres switch, e.g.:
   ```bash
   mvn verify -am --projects=:system -Dit.test=QueryThingsIT -Dtest.environment=local -Dpersistence.backend=postgres \
     -Dgateway.devops.auth.enabled=true -Dgateway.devops.auth.password=foobar -Dconnectivity.http.tunnel=host.docker.internal
   ```
   Run it from the repository root, not from `system/`: `-am` then builds `bom` and `common` in the reactor;
   resolved from `~/.m2` instead, their uninterpolated `${revision}` parent fails the build with
   `Could not find artifact …:bom:pom:${revision}`.
   (the last three flags are host-run specifics explained in
   [docker/README-postgres.md](docker/README-postgres.md#intellij-mode-docker-optional)).

## Authorization server mock

Tests authenticate themselves via OAuth against a mock server.  
The tests make use of the [mock-oauth2-server](https://github.com/navikt/mock-oauth2-server).

In order to run it, simply start it via compose:
```bash
cd docker

# OAuth mock server
docker-compose up -d oauth
```

# Acceptance tests
A subset of the system tests, called `Acceptance tests`, could be run against remote Ditto deployment.
These need some environment variables set before execution, depending on authentication method configured in Ditto deployment.
The following variables must be set:

   * for BasicAuth:
```
      BASIC_AUTH_ENABLED = true     # boolean to configure tests
      DEFAULT_HOSTNAME              # the hostname of the Ditto deployment
      BASIC_AUTH_USERNAME           # Ditto username for basic auth
      BASIC_AUTH_PASSWORD           # the password for the user
```
  * for OAuth - the tests need an OAuth provider and 4 separate OAuth clients:
```
      BASIC_AUTH_ENABLED = false  # default value, could be skipped
      DEFAULT_HOSTNAME            # the hostname of the Ditto deployment

      OAUTH_TOKEN_ENDPOINT        # OAuth token endpoint for getting tokens
      OAUTH_ISSUER                # should match the value configured in Ditto

      OAUTH_CLIENT_ID             # main client id
      OAUTH_CLIENT_SECRET         # main client secret
      OAUTH_CLIENT_SCOPE          # main client scope

      OAUTH_CLIENT2_ID            # 2nd client id
      OAUTH_CLIENT2_SECRET        # 2nd client secret
      OAUTH_CLIENT2_SCOPE         # 2nd client scope

      OAUTH_CLIENT3_ID            # 3rd client id
      OAUTH_CLIENT3_SECRET        # 3rd client secret
      OAUTH_CLIENT3_SCOPE         # 3rd client scope

      OAUTH_CLIENT4_ID            # 4th client id
      OAUTH_CLIENT4_SECRET        # 4th client secret
      OAUTH_CLIENT4_SCOPE         # 4th client scope
```
Run maven with profile `acceptance` and set property `test.environment=deployment`, e.g. with the following command:
```
mvn verify -am -amd --projects=system -Pacceptance -Dtest.environment=deployment -Dtest.timeoutMs=240000 -Dskip.categories=ServiceBus
```
