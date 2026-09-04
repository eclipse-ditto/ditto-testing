# Run the system tests against Ditto on PostgreSQL persistence

Ditto's persistence layer is pluggable: MongoDB is the bundled default, PostgreSQL is a drop-in extension.
The regular system-test environment is started with the SAME scripts CI uses; the database is selected by
the environment variable `DITTO_DB`:

```bash
cd docker
./start.sh                    # DITTO_DB=mongodb (default): everything on MongoDB, exactly as before
DITTO_DB=postgres ./start.sh  # policies, things and connectivity persist journals + snapshots to PostgreSQL 16
```

With `DITTO_DB=postgres` the [`docker-compose-postgres.yml`](docker-compose-postgres.yml) overlay is layered
on the regular stack. things-search's backend is **selectable** via `SEARCH_BACKEND` (default `postgres`):
`SEARCH_BACKEND=postgres` additionally layers [`docker-compose-postgres-search.yml`](docker-compose-postgres-search.yml)
so **things-search runs on PostgreSQL** too; `SEARCH_BACKEND=mongodb` keeps its index in MongoDB (the proven
persistence-on-PG / search-on-Mongo split). `SEARCH_BACKEND` is ignored in mongodb mode. Either way the
`mongodb` container stays defined — do not remove it or the `MONGO_DB_*` variables from `docker-compose.env`.

> **Migration note.** `start-postgres.sh` / `stop-postgres.sh` are gone (use `DITTO_DB=postgres ./start.sh` /
> `DITTO_DB=postgres ./stop.sh`), and so are the `docker-compose-postgres` / `local-postgres` test environments
> (use the plain `docker-compose` / `local` environment plus `-Dpersistence.backend=postgres`). The Postgres
> extension JARs are no longer bind-mounted from a ditto checkout; they are baked into the service images.

The Postgres backend is activated per service by

* **two extension JARs baked into the service image** under `/opt/ditto/extensions/` (on the images'
  default classpath) — the shaded base `ditto-postgres-client-extension` plus a thin JAR per role:
  `ditto-postgres-persistence-extension` for policies/things/connectivity, `ditto-postgres-search-extension`
  for things-search. `ditto/build-images.sh` copies them in when run with `BAKE_POSTGRES_EXTENSIONS=true`
  (companion ditto branch `feat/postgres-persistance-search`). All JARs **must come from the same build** as
  the service — a boot self-check fails fast on mismatch,
* an overlay config from [`postgres/`](postgres/) mounted into `/opt/ditto/` and selected via
  `HOSTING_ENVIRONMENT=filebased` + `HOSTING_ENVIRONMENT_FILE_LOCATION`,
* `POSTGRES_*` connection environment variables (`POSTGRES_SSL_MODE=disable` is mandatory locally).

> **pg_trgm:** things-search-on-PostgreSQL needs the DDL role to
> `CREATE EXTENSION IF NOT EXISTS pg_trgm` at boot. The local `ditto` superuser in `postgres:16`
> satisfies this automatically; against a restricted/managed Postgres, pre-create the extension or grant
> the DDL role the privilege.

## Prerequisites

1. **Ditto service images with the Postgres extensions baked in**, tagged `eclipse/ditto-<svc>:$DITTO_VERSION`
   (postgres mode defaults `DITTO_VERSION` to `0-SNAPSHOT` and `DOCKER_REGISTRY_NAMESPACE` to `eclipse`;
   `BUILD_IMAGES=1` tags what it builds with the same `DITTO_VERSION`). `start.sh` refuses to start when an
   image is missing or was built without the JARs, and a service started from such an image by other means
   dies at config load with `ConfigException$IO: … resource not found on classpath: ditto-postgres-persistence.conf`
   (`…-search.conf` for things-search).
   CI builds them before calling `start.sh`. Locally, build them once from a ditto worktree on the
   `feat/postgres-persistance-search` branch (it contains the marker module
   `internal/utils/postgres-persistence-extension`):
   ```bash
   cd /path/to/ditto && mvn install -DskipTests && BAKE_POSTGRES_EXTENSIONS=true ./build-images.sh
   ```
   (prefix `IMAGE_VERSION=<tag>` if you override `DITTO_VERSION`), or let `start.sh` do the image step for you
   with `BUILD_IMAGES=1` (see below).
2. Docker + a `docker-compose` binary on the PATH (the Compose v2 standalone build is fine), Maven, JDK — the
   same toolchain as for the MongoDB run. The scripts call `docker-compose` by that name (CI rewrites them to
   `docker compose` with `sed`); if you only have the `docker compose` plugin, put a two-line shim named
   `docker-compose` on your PATH: `#!/usr/bin/env bash` / `exec docker compose "$@"`. `DITTO_REPO_DIR`
   (default `./../../ditto`, relative to `docker/`) only matters for `BUILD_IMAGES=1`.

## One-command run

```bash
cd docker
DITTO_DB=postgres ./start.sh                                   # full-Postgres stack (SEARCH_BACKEND=postgres)
DITTO_DB=postgres SEARCH_BACKEND=mongodb ./start.sh            # persistence on PG, things-search on Mongo
DITTO_DB=postgres BUILD_IMAGES=1 DITTO_REPO_DIR=/path/to/ditto ./start.sh   # rebuild the images first
```

In postgres mode the script assembles the compose stack explicitly (`docker-compose.yml`,
`docker-compose.override.yml` **only if present** — CI deletes it — then `docker-compose-postgres.yml`, then
`docker-compose-postgres-search.yml` when `SEARCH_BACKEND=postgres`), pulls only the third-party images
(the Ditto images are local builds), starts PostgreSQL alongside MongoDB, waits for the Postgres
healthcheck, then starts the Ditto services in the usual order. `postgres` is included in the log tailing
(`postgres-<TAG>.log`) and in the container check. With `BUILD_IMAGES=1` it first fails fast if
`DITTO_REPO_DIR` is not a worktree with the marker module or if the extension JAR has not been built, then
runs `IMAGE_VERSION="$DITTO_VERSION" BAKE_POSTGRES_EXTENSIONS=true ./build-images.sh` there. `BUILD_IMAGES` is
ignored in mongodb mode.

Because the local `docker-compose.override.yml` stays in the stack, the gateway is published on
`localhost:8080` as usual and Postgres on `localhost:5432` (`POSTGRES_PORT_TCP` to change it) for debugging;
CI deletes the override, so CI binds no host port for Postgres:

```bash
PGPASSWORD=ditto psql -h localhost -U ditto -d ditto -c '\dt'    # expect *_journal/*_snaps tables
```

With `SEARCH_BACKEND=postgres` the same database also holds the things-search schema tables — their
presence (plus the things-search log) is what actually proves the search overlay booted on PostgreSQL.

The Postgres container has **no persistent volume** on purpose: a fresh container means a fresh
database, which the cleanup tests (`CleanupIT`) rely on.

## Teardown

```bash
cd docker
DITTO_DB=postgres ./stop.sh
```

`stop.sh` downs the same compose file stack (the search overlay is always included, so one command tears
down either `SEARCH_BACKEND` variant). Always stop with the `DITTO_DB` you started with, and stop before
switching modes: `start.sh` does run `compose down` (no `--remove-orphans`) before `up`, but only with the
compose files of its own mode, so leftovers from a run in the other mode stay behind and can make the
network removal fail (which aborts the start); `stop.sh`'s `down --volumes --remove-orphans` on the explicit
stack is the complete teardown.

## Running the tests (in-network, CI-style)

The tests run against the **same `docker-compose` environment as for MongoDB**; only the persistence backend
is switched with the system property `persistence.backend` (default `mongodb` in `test-common.conf`). The
`docker-compose` environment addresses all services by their **container hostnames** (`gateway`, `mongodb`,
`postgres`, `oauth`, ...), so run Maven in a container attached to the compose network (named `test` by
default, or `$DOCKER_NETWORK`) under the alias `system-test-container`, mirroring
`jenkins/Jenkinsfile_system`. From the repository root:

```bash
docker run --rm --network test --network-alias system-test-container \
  -v "$PWD":/ws -v "$HOME/.m2":/root/.m2 -w /ws maven:3.9-eclipse-temurin-25 \
  mvn verify -am --projects=:system -Dtest.environment=docker-compose -Dpersistence.backend=postgres
```

(Adjust the Maven image tag to the Java toolchain in use — the repo currently compiles with Java 25.)

To run a focused pair (note `-Dit.test`, failsafe — `-Dtest` would select nothing) — `CleanupIT`
exercises the DB-direct arm (it opens a JDBC connection to `postgres.jdbc-uri` from
`test-common-docker-compose.conf`), `QueryThingsIT` exercises search CRUD/RQL over the PG backend; both in
one invocation to avoid two ~64-module `-am` builds:

```bash
docker run --rm --network test --network-alias system-test-container \
  -v "$PWD":/ws -v "$HOME/.m2":/root/.m2 -w /ws maven:3.9-eclipse-temurin-25 \
  mvn verify -am --projects=:system -Dit.test=CleanupIT,QueryThingsIT \
    -Dtest.environment=docker-compose -Dpersistence.backend=postgres
```

> **Run Maven from the repository root** (the `-w /ws` above already does). `-am --projects=:system` then
> builds the `bom` and `common` modules from source in the reactor. If instead Maven resolves them from
> `~/.m2` (e.g. you run from `system/`, or without `-am`), this repo's CI-friendly `${revision}` versions —
> it has no flatten plugin, so an installed `common`/`bom` pom keeps a literal `${revision}` parent — make
> the build fail with `Could not find artifact …:bom:pom:${revision}`. `-Drevision` on the CLI does **not**
> fix that; building the modules in-reactor from the root does.

`mvn verify` here does **not** fail the build on IT failures (`system/pom.xml` binds only failsafe's
`integration-test` goal), so read the result from the report, not the exit code:
`grep -h "Tests run" system/target/failsafe-reports/*.txt` and require
`Tests run: N, Failures: 0, Errors: 0, Skipped: 0`. **`Skipped: 0` is load-bearing** — a
`@RunIf(DockerEnvironment)` miss surfaces as a silent skip.

## IntelliJ mode (docker optional)

To run the Ditto services from IntelliJ (as the Mongo-based `intelliJRunConfigurations/*.run.xml` flow
does) with only infrastructure in docker, and point the system tests at them:

1. **Infrastructure only** in docker: `postgres` (via the `Postgres for test` run config, or simply
   `docker-compose up -d postgres` from `docker/` — the override defines a self-contained `postgres` published
   on `localhost:5432`; the full definition with the healthcheck lives in `docker-compose-postgres.yml` and is
   what `DITTO_DB=postgres ./start.sh` uses), plus `oauth` and the brokers as in the main README's IntelliJ section. Start
   `mongodb` only if you run ThingsSearch on Mongo. Do **not** also run ditto's own
   `deployment/postgres-local` stack (it binds the same port).
2. **Ditto from IntelliJ**: launch the `(Postgres)` run configs (`Policies`, `Things`, `ThingsSearch`,
   `Connectivity` — all `for test (Postgres)` — plus the unchanged `Gateway for test`), or the
   `Ditto for test (Postgres)` compound (needs the **Multirun** plugin). They are imported into the ditto
   project and put the r2dbc modules on the classpath via the `ditto-ide-postgres-launcher` module.
   Search backend = which ThingsSearch config you launch (Mongo `ThingsSearch for test` vs
   `ThingsSearch for test (Postgres)`).
3. **Run the tests from the host** against the plain `local` environment plus the Postgres switch (a real
   host-run `mvn` works here — unlike the in-network docker mode, everything is on `localhost`):
   ```bash
   mvn verify -am --projects=:system -Dit.test=CleanupIT,QueryThingsIT \
     -Dtest.environment=local -Dpersistence.backend=postgres \
     -Dgateway.devops.auth.enabled=true -Dgateway.devops.auth.password=foobar \
     -Dconnectivity.http.tunnel=host.docker.internal
   ```
   The last three properties are host-run specifics (they used to live in the removed
   `test-common-local-postgres.conf`): the reused `Gateway for test` run config runs devops-secured
   (`DEVOPS_SECURED=true` / `DEVOPS_PASSWORD=foobar`) while `test-common-local.conf` disables devops auth, so
   without them `CleanupIT`'s `/devops/piggyback` calls get a 401; and an SSH tunnel's forward destination is
   resolved by sshd INSIDE the `ssh` container, so `localhost` would point at the container's own loopback
   instead of the host's `HttpTestServer` — `host.docker.internal` resolves to the host there. On macOS the
   host itself must resolve that name as well (Ditto's connectivity host validator checks it before any
   tunnel exists): one-time `sudo` edit, add `127.0.0.1 host.docker.internal` to `/etc/hosts`. Put the
   `-D` flags into an IntelliJ Maven/JUnit run configuration to avoid retyping them.
   Assert per the report block above (`Skipped: 0`).

The default MongoDB path is untouched: `./start.sh` + `-Dtest.environment=docker-compose` (and
`-Dtest.environment=local` for host runs) behave exactly as before.
