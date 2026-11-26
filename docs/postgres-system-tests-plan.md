# Run Ditto system tests against PostgreSQL persistence

> **Superseded again (2026-09-02) by the unified entry points.** `start-postgres.sh` / `stop-postgres.sh` and
> the `docker-compose-postgres` / `local-postgres` test environments described below no longer exist: use
> `DITTO_DB=postgres ./start.sh` / `DITTO_DB=postgres ./stop.sh` (same scripts as CI) and run the tests with
> `-Dtest.environment=docker-compose -Dpersistence.backend=postgres` (host runs: `-Dtest.environment=local
> -Dpersistence.backend=postgres`). The extension JARs are baked into the service images
> (`BAKE_POSTGRES_EXTENSIONS=true ./build-images.sh`), not bind-mounted, so `DITTO_REPO_DIR` only matters for
> the opt-in local image build (`BUILD_IMAGES=1`). Current instructions: `docker/README-postgres.md`.

> **Superseded (2026-07-08) by
> [`docs/superpowers/specs/2026-07-08-postgres-search-intellij-system-tests-design.md`](superpowers/specs/2026-07-08-postgres-search-intellij-system-tests-design.md).**
> That spec updates this work for three facts this document predates: things-search now also runs on
> PostgreSQL (selectable backend), the single shaded extension JAR was replaced by a three-JAR layout,
> and running Ditto in docker is now optional (IntelliJ-run `local-postgres` mode). Parts A/B and the
> `postgres/{policies,things,connectivity}-postgres.conf` overlays below remain accurate; the
> docker-only / search-stays-on-Mongo scope and the single-extension-JAR references are historic. The
> content below is retained unchanged for history.*

*Revised 2026-07-02 per `docs/postgres-system-tests-plan-review.md` (findings F1–F9 applied).*

## Context

Ditto now has a pluggable persistence layer: MongoDB is the bundled default, PostgreSQL
is a drop-in extension (things/policies/connectivity journals+snapshots move to Postgres;
**things-search stays on MongoDB**). The system-test suite in this repo currently only knows
MongoDB — but almost all of it is already backend-agnostic because it drives Ditto over
HTTP/WS. The single exception is **`CleanupIT`**, which opens a `MongoClient` directly to
count journal/snapshot documents.

Goal: make the whole system-test suite runnable against a Ditto that persists to Postgres,
without breaking the default MongoDB path. This means (a) teaching `CleanupIT` to inspect
either backend behind an abstraction selected by a new test environment, and (b) providing
dedicated docker-compose files + a launch script + instructions to run Ditto-on-Postgres
plus every dependency the system tests need.

Repos involved (siblings under `.../ditto-ws/`):
- **testing** = this repo (`ditto-testing_feat__postgres-persistance`) — all NEW files go here.
- **ditto** = `ditto_feat__postgres-persistance` — source of images + the r2dbc extension JAR; referenced, not modified.

Key facts established during exploration (all code-verified):
- `CleanupIT` is the ONLY DB-direct test (`system/src/test/java/org/eclipse/ditto/testing/system/cleanup/CleanupIT.java`); it counts docs in Mongo dbs `things`/`policies`/`connectivity`, collections `things_journal`/`policies_journal`/`connection_journal` (+ `_snaps`), filtered by `pid IN [...]`, with a connectivity-only filter on `events.p.type` matching `connectivity.events:`.
- The Mongo driver (`mongodb-driver-sync`, managed 5.2.0 in `bom/pom.xml:361`) is declared in
  **`system/pom.xml`**, NOT in `common/pom.xml` — the inspector abstraction therefore lives in
  the **system** module (see Part A).
- Postgres equivalent: tables in `public` schema — `things_journal`, `policies_journal`, `connections_journal` (**plural**), `things_snaps`, `policies_snaps`, `connections_snaps`; `pid TEXT` column, same `thing:/policy:/connection:` id format; event body in `event JSONB` (the full Ditto event JSON incl. top-level `type`). The backend also provisions a fourth table set **`wot_journal`/`wot_snaps`** (`PostgresSchema.ENTITIES = things, policies, connections, wot`) used by Things for WoT-validation-config events — out of scope for the inspector (CleanupIT doesn't cover it on Mongo either), but any system test touching WoT validation-config persistence exercises it.
- Postgres `*_snaps` PK is `(pid, sn, written_at)` — duplicate `(pid, sn)` rows are representable, unlike Mongo's one-doc-per-`(pid, sn)`. Snapshot counts must be `COUNT(DISTINCT sn)` (see Part A).
- Ditto Postgres activation: the shaded `ditto-internal-utils-persistence-r2dbc-extension-*.jar` on the classpath (stock `dockerfile-snapshot` sets `CLASSPATH=/opt/ditto/*:/opt/ditto/extensions/*`) + `HOSTING_ENVIRONMENT=filebased` + `HOSTING_ENVIRONMENT_FILE_LOCATION=<overlay>` + `POSTGRES_*` env vars. `POSTGRES_SSL_MODE=disable` is **mandatory** (profile default is `verify-full`). Proven working via IntelliJ/CLI compound runs — **never yet in containers** (see Part C caveat).
- `test.environment=docker-compose` is an **in-network** environment: CI attaches the Maven
  container to the compose network as `system-test-container`
  (`jenkins/Jenkinsfile_system:134,151-153`) and `test-common-docker-compose.conf` addresses
  everything by container hostname (`gateway`, `mongodb`, `oauth`). The base
  `docker-compose.yml` publishes NO host ports; publication comes only from
  `docker-compose.override.yml`, which compose auto-loads **only when invoked without `-f`
  flags** (CI deletes that file on purpose).
- Local images build via `ditto/build-images.sh` → `eclipse/ditto-<svc>:0-SNAPSHOT`. The testing compose already parametrises image coordinates (`${DOCKER_REGISTRY}/${DOCKER_REGISTRY_NAMESPACE}/ditto-<svc>:${DITTO_VERSION}`) and already uses cross-repo relative paths (`./../../ditto/...`).

## Design overview

Two independent, composable pieces:

1. **Test-side persistence abstraction** — a `PersistenceInspector` interface (count events /
   snapshots per entity kind) with `MongoPersistenceInspector` + `PostgresPersistenceInspector`
   implementations, selected by a new config key `persistence.backend` that a new
   `docker-compose-postgres` test environment sets to `postgres`. Default everywhere = `mongodb`.

2. **Ditto-on-Postgres docker runtime** — a compose override that adds a `postgres` container,
   mounts the r2dbc extension JAR + per-service overlay confs into policies/things/connectivity,
   and sets `POSTGRES_*` + `HOSTING_ENVIRONMENT` env; MongoDB stays for things-search. A
   `start-postgres.sh` boots everything in order; a README documents it.

## Part A — Test-side persistence abstraction (this repo)

New package `org.eclipse.ditto.testing.system.persistence` in the **system** module
(`system/src/test/java/...`). NOT in `common`: the Mongo driver dependency lives in
`system/pom.xml`, `CleanupIT` is the only consumer, and placing the package in `common`
would force both DB drivers into `common/pom.xml` for no benefit.

- `PersistenceInspector` (interface, `AutoCloseable`):
  ```
  long countThingEvents(List<String> thingIds);
  long countThingSnaps(List<String> thingIds);
  long countPolicyEvents(List<String> policyIds);
  long countPolicySnaps(List<String> policyIds);
  long countConnectionEvents(List<String> connIds, boolean onlyConnectivityEvents);
  long countConnectionSnaps(List<String> connIds);
  ```
  Javadoc must state the cross-backend counting contract: Mongo counts journal *documents*
  (one atomic batched persist = one doc with an `events[]` array) while Postgres counts rows
  (one per event). CleanupIT's equality assertions pass on Mongo today, i.e. no batching
  occurs in practice — the inspector relies on that; if Ditto ever batches, both impls
  diverge and this javadoc is the breadcrumb.
- `MongoPersistenceInspector implements PersistenceInspector` — lift the existing
  `createMongoClient()` + `countDocuments(...)` logic verbatim out of `CleanupIT` (dbs/collections/
  BSON `pid $in` + the `events.p.type` regex for connectivity). Constructed from `getMongoDBUri()`.
- `PostgresPersistenceInspector implements PersistenceInspector` — plain JDBC (`org.postgresql`,
  blocking is fine for a test) against `public` tables:
  - events: `SELECT count(*) FROM <entity>_journal WHERE pid = ANY(?)` with pids `= prefix + id`.
  - snaps:  `SELECT count(DISTINCT sn) FROM <entity>_snaps WHERE pid = ANY(?)` —
    **`DISTINCT sn`, not `count(*)`**: the Postgres snaps PK `(pid, sn, written_at)` can hold
    several rows per `(pid, sn)` where Mongo upserts a single doc; `DISTINCT sn` gives
    Mongo-equivalent semantics regardless.
  - table map: things→`things_journal/things_snaps`, policies→`policies_journal/policies_snaps`,
    connections→`connections_journal/connections_snaps`. (The `wot_*` tables are intentionally
    out of scope — CleanupIT does not cover WoT on Mongo either.)
  - `onlyConnectivityEvents` → add `AND event->>'type' LIKE 'connectivity.events:%'`
    (mirrors the Mongo `events.p.type` regex; the `event` JSONB column is the full Ditto event
    JSON with top-level `type`, and the base-`Event` adapter binding covers the `EmptyEvent`
    no-op entries the filter exists to exclude). Mongo's regex is an unanchored substring,
    `LIKE 'x%'` is anchored — equivalent for these values. **Still verify against real data**
    in verification step 4 before trusting the count.
  - Connect via new `postgres.*` config (jdbc-uri/user/password).
- `PersistenceInspectorFactory.create(CommonTestConfig)` → returns Postgres impl when
  `persistence.backend == "postgres"`, else Mongo impl.

`CommonTestConfig` (`common/src/main/java/.../CommonTestConfig.java`): add
`getPersistenceBackend()` (default `"mongodb"`), `getPostgresJdbcUri()`, `getPostgresUser()`,
`getPostgresPassword()` reading new HOCON keys. (Getters go to `common` — config only, no
driver dependency.)

`CleanupIT`: replace `createMongoClient()`/`count*` helpers and the `com.mongodb.*` imports with
`PersistenceInspector inspector = PersistenceInspectorFactory.create(TEST_CONFIG)` inside the
try-with-resources; swap each `count*(mongoClient, ...)` for `inspector.count*(...)`. Behaviour
and assertions unchanged. While at it, rename `WAIT_FOR_MONGO_WRITES` (and its "writes on
MongoDB" log line) to `WAIT_FOR_PERSISTENCE_WRITES` — it is backend-neutral now.

**Gating fix (required):** `@RunIf(DockerEnvironment.class)` → `DockerEnvironment.isSatisfied()` →
`CommonTestConfig.isLocalOrDockerTestEnvironment()` (`CommonTestConfig.java:220`), which today
only matches `local`/`docker-compose` via `equalsIgnoreCase`. Without a change, `CleanupIT` is
**silently skipped** under `docker-compose-postgres`. Preferred fix: match by prefix —
`testEnvironment.startsWith(TEST_ENVIRONMENT_DOCKER_COMPOSE)` — which mirrors the existing
`TestEnvironment.getForString()` convention (`common/.../config/TestEnvironment.java:44-52`
already `startsWith`-matches, so `"docker-compose-postgres"` maps to `DOCKER_COMPOSE` with no
enum change) and auto-covers future suffixed variants. Adding an explicit
`TEST_ENVIRONMENT_DOCKER_COMPOSE_POSTGRES` constant is the acceptable alternative; either way,
be deliberate. Note the change affects more than CleanupIT gating: `ServiceEnvironment.java:186`
also branches on `isLocalOrDockerTestEnvironment()` (solution/auth setup) — that is the
*desired* behaviour for the new env, but list it as an affected call-site in the PR.
(Config files load by `test-common-<env>.conf` convention, so
`test-common-docker-compose-postgres.conf` is picked up automatically; the new env file must
`include "test-common-docker-compose"` to inherit the docker networking — the automatic
fallback chain only covers `test-common.conf`.)

Build wiring:
- `bom/pom.xml`: add `org.postgresql:postgresql` (managed; follow the
  `mongo-java-driver.version` property precedent at `bom/pom.xml:55`).
- `system/pom.xml`: add the `org.postgresql:postgresql` dependency next to the existing
  `mongodb-driver-sync` (`system/pom.xml:265`). `common/pom.xml` is NOT touched.

## Part B — `docker-compose-postgres` test environment (this repo)

**Execution model (explicit):** this environment is **in-network**, exactly like its parent
`docker-compose` — the test JVM runs in a container attached to the compose network (CI does
this as `system-test-container`; locally use the same trick, see Part D). All hosts below are
container hostnames. Running the suite from the bare host against this env does not work —
the inherited `gateway`/`mongodb` hostnames don't resolve there either; if a host-run
variant is ever wanted, it is a separate `local-postgres` env based on
`test-common-local.conf`, not a tweak to this one.

New `common/src/main/resources/test-common-docker-compose-postgres.conf`:
```
include "test-common-docker-compose"      # gateway host, brokers, oauth, mongo (search) unchanged
persistence.backend = "postgres"
postgres {
  jdbc-uri = "jdbc:postgresql://postgres:5432/ditto"   # in-network hostname (NOT localhost)
  jdbc-uri = ${?POSTGRES_JDBC_URI}                     # override hook for ad-hoc setups
  user = "ditto"
  password = "ditto"
}
```
Base `test-common.conf` gets `persistence.backend = "mongodb"` (explicit default). Tests select
this env with `-Dtest.environment=docker-compose-postgres`, matching the chosen mechanism.

## Part C — Ditto-on-Postgres docker runtime (this repo, `docker/`)

New files under `docker/`:

1. **`docker-compose-postgres.yml`** — a compose override (layered after `docker-compose.yml`)
   that:
   - adds service `postgres` (`postgres:16`, env `POSTGRES_DB/USER/PASSWORD=ditto`, `pg_isready`
     healthcheck, expose 5432; also publish 5432 for host-side `psql` debugging), mirroring
     `ditto/deployment/postgres-local/docker-compose.postgres.yml` — but with **NO named
     volume**: `CleanupIT` is documented "should be the only test in a fresh environment", and
     a persistent volume would accumulate state across runs. Fresh container = fresh DB.
   - keeps `mongodb` (things-search index).
   - for `policies`, `things`, `connectivity` each: mount the extension JAR to
     `/opt/ditto/extensions/` and the overlay conf to `/opt/ditto/`, add env
     `HOSTING_ENVIRONMENT=filebased`, `HOSTING_ENVIRONMENT_FILE_LOCATION=/opt/ditto/<svc>-postgres.conf`,
     `POSTGRES_URI=r2dbc:postgresql://postgres:5432/ditto`, `POSTGRES_USER/PASSWORD=ditto`,
     `POSTGRES_DDL_USER/PASSWORD=ditto`, `POSTGRES_SSL_MODE=disable`; `depends_on: postgres`.
     (`read-journal.entity` needs no wiring — each service conf sets it itself:
     `things.conf:34`, `policies.conf:24`, `connectivity.conf:102`.)
   - `gateway` + `things-search` unchanged (stateless / Mongo). The shared
     `docker-compose.env` (Mongo hostnames etc.) keeps being injected into the three
     Postgres-backed services — harmless (their Mongo config resolves but the Postgres
     provider ignores it); add a one-line comment so nobody "cleans it up" and breaks
     things-search.
   - JAR + conf source paths via a `${DITTO_REPO_DIR:-./../../ditto}` variable so the ditto
     worktree location is configurable.

2. **`postgres/<svc>-postgres.conf`** (policies/things/connectivity) — the containerized
   Postgres overlays. **This is NEW, unproven config surface — not a "docker variant of
   proven overlays"**: `ditto/deployment/postgres-local/*-postgres.conf` are today one-line
   delegates to the classpath `<svc>-pg-dev.conf` profiles, which include `<svc>-dev`
   (localhost/dev cluster settings, unusable in containers), and there is NO connectivity
   overlay in `postgres-local/` at all. The proven Postgres runtime is the IntelliJ/CLI
   compound on one host; the `include classpath("<svc>")` chain below has never booted
   anywhere. It *should* work — compose supplies seed nodes via `-D` flags in
   `JAVA_TOOL_OPTIONS` (system properties override file config) and the raw service confs are
   env-var wired — but treat it as design-to-validate and budget iteration time for all three
   services, connectivity doubly so:
   ```
   include classpath("<svc>")                       # docker base (env-var wired); NOT <svc>-dev
   include classpath("ditto-postgres-persistence")  # TOP-LEVEL; swaps backend + R2DBC defaults
   pekko.persistence.journal.auto-start-journals = [ "ditto-postgres-<entity>-journal" ]
   pekko.persistence.snapshot-store.auto-start-snapshot-stores = [ "ditto-postgres-<entity>-snapshots" ]
   ditto-postgres-<entity>-journal.overrides   { journal-collection = "<entity>_journal"; metadata-collection = "<entity>_metadata" }
   ditto-postgres-<entity>-snapshots.overrides { snaps-collection = "<entity>_snaps" }
   ```
   The narrowed auto-start lists and `overrides { … }` placeholder blocks match the
   known-good `*-pg-dev.conf` profiles exactly (things: `things-pg-dev.conf:54-64`); keep the
   "no snapshot-adapter override" rule too (C3 removed the key; the adapter is composed
   automatically). Things' WoT plugins (`ditto-postgres-wot-*`) start lazily on first use and
   reuse the thing dispatchers, so the things-only auto-start list is correct.
   Verify each service boots on Postgres (confirm no "missing plugin config path" errors);
   adjust the include chain if the filebased overlay needs a different base than the raw
   service conf.

3. **`start-postgres.sh`** — sibling of `start.sh`, orchestrates the Postgres run:
   - **Fail fast on a wrong ditto worktree**: multiple ditto checkouts/worktrees exist
     side-by-side; before anything else, assert
     `$DITTO_REPO_DIR/internal/utils/persistence-r2dbc-extension` exists, else abort with a
     clear message — a Mongo-era checkout at the default path would otherwise silently build
     Postgres-less images.
   - `(cd $DITTO_REPO_DIR && ./build-images.sh)` unless `SKIP_IMAGE_BUILD=1`; build the extension
     JAR: `mvn -pl :ditto-internal-utils-persistence-r2dbc-extension -am -DskipTests package`.
   - export `DITTO_VERSION=0-SNAPSHOT`, `DOCKER_REGISTRY_NAMESPACE=eclipse` so the local images
     are used.
   - **Compose file stack (critical):** explicit `-f` flags disable compose's auto-loading of
     `docker-compose.override.yml` (the file that publishes gateway 8080 / mongo 27017 to the
     host and fixes Kafka's advertised listeners — CI deletes it deliberately; locally it must
     stay). Therefore build the stack explicitly:
     ```
     COMPOSE_FILES="-f docker-compose.yml"
     [ -f docker-compose.override.yml ] && COMPOSE_FILES="$COMPOSE_FILES -f docker-compose.override.yml"
     COMPOSE_FILES="$COMPOSE_FILES -f docker-compose-postgres.yml"
     docker-compose $COMPOSE_FILES up -d ...
     ```
     Bring services up in dependency order: postgres + mongodb + oauth + brokers → policies →
     things → things-search → connectivity → gateway (reuse `start.sh`'s
     ordering/log-tailing/health-wait, adapted; wait for the postgres healthcheck before
     starting policies).
   - a matching **`stop-postgres.sh`** using the **same `$COMPOSE_FILES` set** — a bare
     `docker-compose down` would not know the `postgres` service and would leave its
     container (and the extra mounts) running.

## Part D — Instructions (this repo)

- New `docker/README-postgres.md` (and a pointer from the top-level `README.md`) covering:
  - prerequisites (sibling ditto **feature-branch** worktree, `DITTO_REPO_DIR`),
  - one-command run (`cd docker && ./start-postgres.sh`), teardown (`./stop-postgres.sh`),
  - **how to run the tests — in-network, CI-style** (the env does not work from the bare
    host, see Part B):
    ```
    docker run --rm --network test --network-alias system-test-container \
      -v "$PWD":/ws -v "$HOME/.m2":/root/.m2 -w /ws maven:3.9-eclipse-temurin-21 \
      mvn verify -am --projects=:system -Dtest.environment=docker-compose-postgres
    ```
    (mirrors `jenkins/Jenkinsfile_system:134,151-153`; adjust the Maven image tag to the
    toolchain in use),
  - the note that both MongoDB (search index) and Postgres (journals/snapshots) run
    simultaneously.

## Preserving MongoDB default

- `persistence.backend` defaults to `mongodb`; no existing conf sets otherwise.
- `CleanupIT` behaviour identical under Mongo (same queries, just moved behind the interface).
- Existing `docker-compose.yml`, `docker-compose.override.yml`, `start.sh`,
  `test-common*.conf`, and all Mongo runs untouched.
- New Postgres path is entirely opt-in via the new env + compose files.

## Verification (end-to-end)

All `mvn verify` runs below execute **in-network** via the `docker run … --network-alias
system-test-container` wrapper from Part D (steps 1/4/5 are not executable from the bare
host — the `docker-compose*` envs resolve container hostnames only).

1. **Default unaffected**: `cd docker && ./start.sh`; in-network
   `mvn verify -am --projects=:system -Dtest.environment=docker-compose` — full suite green
   (esp. `CleanupIT`) on Mongo.
2. **Postgres boot**: `cd docker && DITTO_REPO_DIR=../../ditto ./start-postgres.sh`; confirm all
   containers healthy, no "missing plugin config path" in policies/things/connectivity logs, and
   tables auto-created (`psql -h localhost -U ditto -d ditto -c '\dt'` shows `*_journal`/`*_snaps`
   — including the `wot_*` and `*_journal_seq` tables the schema manager provisions).
3. **Smoke**: create a thing via gateway (localhost:8080 works because the override file stays
   in the stack, see Part C item 3); verify a row appears in `things_journal`
   (`SELECT count(*) FROM things_journal`), and the thing is searchable (search index in Mongo).
4. **CleanupIT on Postgres**: in-network
   `mvn verify -am --projects=:system -Dit.test=CleanupIT -Dtest.environment=docker-compose-postgres`
   (**`-Dit.test`** — failsafe; `-Dtest` targets surefire and would select nothing) — passes
   using `PostgresPersistenceInspector` (validates the connectivity `event->>'type'` filter
   against real data). **Assert the failsafe summary shows `Tests run: 1` — not skipped**:
   `@RunIf` skips silently, and a forgotten gating fix (Part A) would otherwise produce a
   green-but-meaningless run.
5. **Full suite on Postgres**: in-network
   `mvn verify -am --projects=:system -Dtest.environment=docker-compose-postgres`.
   **Known-risk area to watch first**: search tests (`system-sync/*`, search ITs) — they
   depend on things-search background/full sync against Postgres-backed things via the
   generalized `SnapshotStreamingActor`, which is implemented and IT-proven in the ditto repo
   but explicitly NOT yet proven end-to-end with the search service running. Expect first
   failures there, not in CRUD tests; triage those as Ditto-side findings, not test-harness
   bugs.

## Files to create / modify

**This repo — create**
- `system/src/test/java/org/eclipse/ditto/testing/system/persistence/PersistenceInspector.java`
- `.../persistence/MongoPersistenceInspector.java`
- `.../persistence/PostgresPersistenceInspector.java`
- `.../persistence/PersistenceInspectorFactory.java`
- `common/src/main/resources/test-common-docker-compose-postgres.conf`
- `docker/docker-compose-postgres.yml`
- `docker/postgres/{policies,things,connectivity}-postgres.conf`
- `docker/start-postgres.sh`, `docker/stop-postgres.sh`
- `docker/README-postgres.md`

**This repo — modify**
- `system/.../cleanup/CleanupIT.java` (use inspector; drop `com.mongodb.*` imports; rename
  `WAIT_FOR_MONGO_WRITES`)
- `common/.../CommonTestConfig.java` (backend + postgres getters; gating fix — prefix-match
  preferred; note `ServiceEnvironment.java:186` as affected call-site)
- `common/src/main/resources/test-common.conf` (`persistence.backend = "mongodb"`)
- `bom/pom.xml`, `system/pom.xml` (add `org.postgresql:postgresql`; `common/pom.xml` untouched)
- top-level `README.md` (pointer to Postgres instructions)

**Ditto repo — none required** (reuse `build-images.sh`, `dockerfile-snapshot`, the r2dbc
extension module, and `ditto-postgres-persistence.conf`). `DITTO_REPO_DIR` points the compose at it.
