# System tests: things-search on PostgreSQL + IntelliJ-run Ditto (docker optional)

> **Superseded (2026-09-02).** The two-script / suffixed-environment design below was replaced by a
> `DITTO_DB` selector in the CI entry points: `DITTO_DB=postgres ./start.sh` / `./stop.sh`,
> `-Dpersistence.backend=postgres` on the plain `docker-compose` / `local` environments, extension JARs baked
> into the images. See `docker/README-postgres.md`; the content below is retained for history.

*Design spec, 2026-07-08. Supersedes the docker-only / search-stays-on-Mongo scope of
`docs/postgres-system-tests-plan.md` (whose Parts A–D are implemented on this branch: Parts A/B and
the `postgres/{policies,things,connectivity}-postgres.conf` overlays stay as-is, while Part C's
compose file and scripts are updated by Part 1 below for the three-JAR layout. This spec updates
that work for two new facts and one new requirement).*

## Context

The branch `feat/postgres-persistance` already implements the original plan: the
`PersistenceInspector` abstraction behind `persistence.backend`, the in-network
`docker-compose-postgres` test environment, `docker/docker-compose-postgres.yml` +
`docker/postgres/{policies,things,connectivity}-postgres.conf` overlays,
`start-postgres.sh`/`stop-postgres.sh`, and `docker/README-postgres.md`.

Three things changed:

1. **Things-search now runs on PostgreSQL too.** The ditto worktree
   `ditto_feat__postgres-persistance-search` (branch `feat/postgres-persistance-search`, a strict
   superset of `feat/postgres-persistance`) implements the search backend in
   `internal/utils/search-r2dbc`, activated by the top-level include
   `classpath("ditto-postgres-search")` (dev twin: `search-pg-dev.conf`; CLI/compose proven via
   `deployment/postgres-local/run-compound-postgres.sh` and
   `deployment/postgres-local/search-postgres.conf`).
2. **The single shaded extension JAR is gone.** `ditto-internal-utils-persistence-r2dbc-extension`
   was replaced by a **three-JAR layout** (all `0-SNAPSHOT`, all must come from the same build —
   a boot self-check fails fast on mismatch or on a thin JAR without its base):

   | artifactId | contents | mounted into |
   |---|---|---|
   | `ditto-postgres-client-extension` (base, shaded) | postgres-client infra + all third-party deps (r2dbc-postgresql, r2dbc-pool, reactor, netty, scram) | every Postgres-backed service |
   | `ditto-postgres-persistence-extension` (thin) | persistence-r2dbc classes | policies, things, connectivity |
   | `ditto-postgres-search-extension` (thin) | search-r2dbc classes | things-search, only when search runs on Postgres |

   Module dirs: `internal/utils/postgres-{client,persistence,search}-extension`; JARs at
   `<module>/target/ditto-postgres-<x>-extension-0-SNAPSHOT.jar` (each module's main shaded JAR —
   all three run the shade plugin, so mount `ditto-postgres-<x>-extension-*.jar`, **not** its
   `original-*` sibling; "thin" in the table means no bundled third-party deps, not un-shaded). The
   current compose mounts and `start-postgres.sh` preflight/build
   reference the deleted module and are therefore broken against the search worktree.
3. **New requirement: running Ditto in docker must be optional.** The developer workflow is to run
   the Ditto services from IntelliJ (as the existing Mongo-based `intelliJRunConfigurations/*.run.xml`
   flow does) with only infrastructure in docker, and point the system tests at them.

Decisions taken with the user:

- **MongoDB stays in the Postgres docker environment; the search backend is selectable**
  (Mongo or Postgres). Selection is a *runtime* concern only — the test suite needs no
  `search.backend` key because no test does DB-direct *search* access (`CleanupIT`, the only
  DB-direct test, touches journal/snapshot tables only; search tests go over HTTP). This makes the
  suite backend-agnostic in *transport*, not backend-*verified*: the existing search IT suite
  (`QueryThingsWithFilterByEmptyIT`, the `like`/`ilike` and null / three-valued-logic filter ITs,
  `QueryThingsPagingIT` cursor-over-tied-keys) pins exact RQL result sets and is the real PG-search
  parity gate — a ditto `search-r2dbc` responsibility (result-set/order/collation parity; collation
  is already pinned ditto-side via `COLLATE "C"`). The smoke verification below does **not**
  establish that parity.
- **`start-postgres.sh` defaults to `SEARCH_BACKEND=postgres`** (full-Postgres stack);
  `SEARCH_BACKEND=mongodb` falls back to the proven persistence-on-PG / search-on-Mongo split.
- **IntelliJ run configs**: new `"<svc> for test (Postgres)"` variants in
  `intelliJRunConfigurations/`, not reuse of the ditto repo's `.run` configs.
- **Verification**: smoke-verify both modes (not the full suite).

## Key mechanism (why the IntelliJ variants are cluster-compatible)

The existing `"<svc> for test"` run configs set no `HOSTING_ENVIRONMENT`, so Ditto boots the
`<svc>-dev.conf` profile (static localhost cluster seeds, Policies founding seed on 2552). The
ditto repo's `<svc>-pg-dev.conf` profiles *include* those same `-dev` profiles and add the Postgres
backend. Therefore a mix of PG and non-PG "for test" configs joins one cluster, and Gateway —
stateless, no PG profile exists (`.run/GatewayService (Postgres).run.xml` in ditto sets no overlay
either) — needs **no** Postgres variant.

Classpath: IntelliJ must put the r2dbc modules on the run classpath. The ditto repo ships a
launcher module for exactly this: classpath module `ditto-ide-postgres-launcher`
(`<ditto>/ide-postgres-launcher`), main class stays the service's own starter. All new run configs
use it, mirroring ditto's own `.run/*(Postgres).run.xml`.

## Part 1 — Docker runtime: three-JAR layout + selectable search backend

**`docker/docker-compose-postgres.yml` (modify)**

- Replace the single `persistence-r2dbc-extension` JAR mount in policies/things/connectivity with
  two mounts each: `ditto-postgres-client-extension` + `ditto-postgres-persistence-extension`
  (paths via `${DITTO_REPO_DIR:-./../../ditto}` as today — the `:-./../../ditto` default is nominal;
  `DITTO_REPO_DIR` must point at the **search** worktree, which the new preflight enforces), still
  into `/opt/ditto/extensions/`.
- `mongodb` service and `docker-compose.env` injection stay exactly as-is (comment updated: Mongo
  is required when `SEARCH_BACKEND=mongodb` and harmless otherwise).
- Header comment updated (search no longer "stays" on Mongo; it is selectable).

**`docker/docker-compose-postgres-search.yml` (new)**

A second overlay, layered *after* `docker-compose-postgres.yml` only when
`SEARCH_BACKEND=postgres`, touching only `things-search`:

- mounts `ditto-postgres-client-extension` + `ditto-postgres-search-extension` into
  `/opt/ditto/extensions/` and `docker/postgres/search-postgres.conf` into `/opt/ditto/`,
- env: `HOSTING_ENVIRONMENT=filebased`,
  `HOSTING_ENVIRONMENT_FILE_LOCATION=/opt/ditto/search-postgres.conf`, `POSTGRES_URI=
  r2dbc:postgresql://postgres:5432/ditto`, `POSTGRES_USER/PASSWORD/DDL_USER/DDL_PASSWORD=ditto`,
  `POSTGRES_SSL_MODE=disable`,
- `depends_on: postgres: condition: service_healthy`.

**`docker/postgres/search-postgres.conf` (new)**

Sibling of the three existing overlays, same rules (top-level `include classpath(...)`, raw service
config — *not* the `-dev` profile):

```
include classpath("search")                # raw Things-Search service config (env-var wired)
include classpath("ditto-postgres-search") # opt-in PG search backend + shared client defaults
```

No auto-start-journal narrowing needed — the search service runs no event-sourcing journal (it sets
no `auto-start-journals`); the `ditto-postgres-search` include swaps
`ditto.extensions.search-persistence-provider` and pulls in the shared `ditto.postgresql.*`
client/plan/reaper defaults, but — unlike the persistence profile — needs no journal narrowing
(mirrors `<ditto>/deployment/postgres-local/search-postgres.conf`, whose base is `search-dev`
instead). **Caveat:** this raw-`classpath("search")` + `HOSTING_ENVIRONMENT=filebased` overlay has
never booted in a container — it is the exact new analog of the plan-review's F4 caveat for the
now-proven persistence overlays. Budget iteration time, and make Verification step 1 assert
things-search actually boots on PG before trusting `QueryThingsIT`.

**`docker/start-postgres.sh` (modify)**

- Preflight: assert `$DITTO_REPO_DIR/internal/utils/postgres-persistence-extension` exists (new
  layout marker; old worktrees and Mongo-era checkouts both fail fast with an updated message
  naming the search-branch worktree).
- Build step: replace the single `-pl :ditto-internal-utils-persistence-r2dbc-extension` build with
  `mvn -pl :ditto-postgres-client-extension,:ditto-postgres-persistence-extension,:ditto-postgres-search-extension -am -DskipTests package`
  (one reactor invocation; the search extension is built even under `SEARCH_BACKEND=mongodb` — it
  is cheap and keeps the script branch-free until compose-file assembly). `-am` pulls a ~64-module
  reactor (same magnitude as the old single-extension `-am`), and the client shade makes a real
  `package` take minutes — so it runs only when `SKIP_IMAGE_BUILD` is unset.
- JAR existence checks for all three JARs (the shaded client base + the two thin JARs; each is the
  main `ditto-postgres-<x>-extension-0-SNAPSHOT.jar`, not the `original-*` sibling).
- `SEARCH_BACKEND="${SEARCH_BACKEND:-postgres}"`; validate it is `postgres` or `mongodb`; when
  `postgres`, append `-f docker-compose-postgres-search.yml` to `COMPOSE_FILES`.
- Startup order, health-wait, log tailing, container checks unchanged.

**`docker/stop-postgres.sh` (modify)**

Always include `-f docker-compose-postgres-search.yml` in its stack (downing a service that was
never started is a no-op; this way one stop script tears down either variant).

## Part 2 — IntelliJ mode: `local-postgres` environment + run configs

**`common/src/main/resources/test-common-local-postgres.conf` (new)**

```
# NOTE (carry over from test-common-docker-compose-postgres.conf): config.TestConfig resolves its
# filename from the TestEnvironment *enum* (local-postgres -> LOCAL -> test-common-local.conf), so
# the persistence.backend / postgres.* keys below are visible only to CommonTestConfig consumers
# (which use the raw -Dtest.environment string), NOT to TestConfig consumers. CleanupIT reads them
# via CommonTestConfig -> PersistenceInspectorFactory, so this is fine — but a future key added
# here will NOT reach TestConfig.
include "test-common-local"        # gateway/oauth on localhost, unchanged
persistence.backend = "postgres"
postgres {
  jdbc-uri = "jdbc:postgresql://localhost:5432/ditto"
  jdbc-uri = ${?POSTGRES_JDBC_URI}
  user = "ditto"
  password = "ditto"
}
# C1: the reused "Gateway for test" run config keeps the Mongo-era DEVOPS_SECURED=true /
# DEVOPS_PASSWORD=foobar, but test-common-local sets gateway.devops.auth.enabled=false. Without the
# re-enable below, PiggyBackCommander sends CleanupIT's /devops/piggyback requests unauthenticated
# and the secured gateway returns 401. (Docker mode instead disables both ends.)
gateway.devops.auth.enabled = true
gateway.devops.auth.password = "foobar"
```

Selected with `-Dtest.environment=local-postgres`. `TestEnvironment.getForString` already
prefix-matches (`DOCKER_COMPOSE` is checked first and does not match, `LOCAL` does), so no enum
change. Config-file resolution follows the existing `test-common-<env>.conf` convention.

**`common/.../CommonTestConfig.java` (modify — one line)**

`isLocalOrDockerTestEnvironment()` currently `equalsIgnoreCase`-matches `local` and
prefix-matches `docker-compose`; change the `local` arm to
`testEnvironment.startsWith(TEST_ENVIRONMENT_LOCAL)` so the `@RunIf(DockerEnvironment)` gate covers
`local-postgres`. This flips **all ~22 `@RunIf(DockerEnvironment)` test classes** (`CleanupIT` plus
the connectivity/messaging/live-channel/ws suites) from skipped→runnable under `local-postgres` —
exactly as they already run under plain `local`; desired, and safe (the only two call sites are
`DockerEnvironment.isSatisfied()` and `ServiceEnvironment`'s solution/auth setup, and no env name
other than `local-postgres` starts with `local`). Same rationale and comment as the existing
`docker-compose` prefix fix.

**`intelliJRunConfigurations/` (new files)**

Each `(Postgres)` variant = copy of its sibling's test env vars (`INSTANCE_INDEX`, log levels,
throttling, oauth issuers on Gateway — n/a, see below) **plus**:

- `HOSTING_ENVIRONMENT=filebased`,
  `HOSTING_ENVIRONMENT_FILE_LOCATION=$PROJECT_DIR$/<svc>/service/src/main/resources/<svc>-pg-dev.conf`
  (`$PROJECT_DIR$` = the *ditto* project these configs are imported into; profiles exist for
  policies, things, connectivity, search — **but the `<svc>` template does not expand cleanly for
  search: its path is `thingsearch/service/src/main/resources/search-pg-dev.conf` (dir `thingsearch`,
  file prefix `search`). Copy ditto's own `.run/SearchService (Postgres).run.xml` verbatim**),
- `POSTGRES_URI=r2dbc:postgresql://localhost:5432/ditto`, `POSTGRES_USER/PASSWORD=ditto`,
  `POSTGRES_DDL_USER/DDL_PASSWORD=ditto`, `POSTGRES_SSL_MODE=disable`,
- classpath module `ditto-ide-postgres-launcher` (replaces `ditto-<svc>-service`),
- same JRE (`temurin-25`) and VM parameters as the sibling config.

Files:

1. `Policies for test (Postgres).run.xml`
2. `Things for test (Postgres).run.xml`
3. `ThingsSearch for test (Postgres).run.xml` — search-backend selection in this mode is simply
   which ThingsSearch config you launch (Mongo sibling vs this one)
4. `Connectivity for test (Postgres).run.xml`
5. `Ditto4test (Postgres).run.xml` — Multirun compound: Policies (PG), Things (PG), ThingsSearch
   (PG), Connectivity (PG), **Gateway for test** (unchanged Mongo-era config, intentionally — it
   keeps `DEVOPS_SECURED=true`/`DEVOPS_PASSWORD=foobar`). **C1:** because it stays secured,
   `test-common-local-postgres.conf` must re-enable devops auth with the matching password (done
   above), or CleanupIT's `/devops/piggyback` calls get 401. The alternative — giving the compound
   a `DEVOPS_SECURED=false` Gateway leg, like ditto's own `GatewayService (Postgres)` — was rejected
   to keep the Gateway config genuinely unchanged. (Multirun is a plugin; note it as a prerequisite.)
6. `Postgres.run.xml` — docker-deploy config mirroring `Mongo.run.xml`: image `postgres:16`,
   container `localPostgres`, port 5432→5432, env `POSTGRES_DB/USER/PASSWORD=ditto`

No changes to the five existing Mongo-era configs.

**Infrastructure for this mode** (documented, not scripted): `postgres` via `Postgres.run.xml` or
`docker-compose -f docker-compose.yml -f docker-compose-postgres.yml up -d postgres` — note the
`postgres` service is defined **only** in `docker-compose-postgres.yml`, so it is *not* part of the
existing brokers/oauth infra run-config (which uses `docker-compose.yml` + `.override.yml`) and must
be started separately; `mongodb` only when running ThingsSearch on Mongo; `oauth` and brokers
exactly as the existing IntelliJ section of `README.md` describes. Postgres is published on
`localhost:5432` (already the case in `docker-compose-postgres.yml`), matching both the run configs'
`POSTGRES_URI` and the test env's `postgres.jdbc-uri` — do **not** run ditto's own
`deployment/postgres-local` stack at the same time, as it also binds host `5432`.

## Part 3 — Documentation

- **`README.md`**: extend "How to run tests in IntelliJ" with the Postgres flow (start `postgres`
  container, launch the `(Postgres)` run configs or the `Ditto4test (Postgres)` compound — which
  needs the **Multirun** IntelliJ plugin — then run tests with an explicit command, e.g.
  `mvn verify -am --projects=:system -Dit.test=QueryThingsIT -Dtest.environment=local-postgres`);
  replace the stale "things-search stays on MongoDB" sentence in the Postgres pointer with the
  selectable-backend wording and the search-worktree prerequisite.
- **`docker/README-postgres.md`**: update to the three-JAR layout; document `SEARCH_BACKEND`
  (default `postgres`); update the prerequisite to the **search**-branch worktree
  (`internal/utils/postgres-persistence-extension` as the marker); note that search-on-PG requires
  the DDL role to `CREATE EXTENSION IF NOT EXISTS pg_trgm` at boot (auto-satisfied by the local
  `ditto` superuser in `postgres:16`; a gotcha against a restricted/managed Postgres); add an
  "IntelliJ mode (docker optional)" section covering infra-only startup, run configs, and
  `local-postgres`; keep the in-network CI-style instructions for the docker mode.
- **`docs/postgres-system-tests-plan.md`**: prepend a superseded-by note pointing here (historic
  content untouched).

## Out of scope / unchanged

- Default Mongo path: `start.sh`, `docker-compose.yml`, `docker-compose.override.yml`, the five
  Mongo-era run configs, `test-common-local.conf`, `test-common-docker-compose.conf` — untouched.
- `PersistenceInspector` and `CleanupIT` — already backend-agnostic; nothing to change.
- The `docker-compose-postgres` in-network test environment conf — unchanged (search selection
  does not affect test config).
- `wot_*` tables, WoT validation-config persistence — out of scope as before.
- No `local-postgres`-specific CI wiring (Jenkins) — developer workflow only.

## Verification (smoke, both modes)

Precondition for both: the search worktree built once (`mvn install -DskipTests`) so allinone +
extension JARs exist; `DITTO_REPO_DIR` pointed at it.

**How to assert a pass (applies to every step below).** `mvn verify` in this repo does **not** fail
the build on IT failures — `system/pom.xml` binds only failsafe's `integration-test` goal, not the
`verify` gate, so the process exits 0 whether a test passes, fails, errors, or is `@RunIf`-skipped.
Read the result from the report, not the exit code:
`grep -h "Tests run" system/target/failsafe-reports/*.txt` and require
`Tests run: N, Failures: 0, Errors: 0, Skipped: 0`. **`Skipped: 0` is load-bearing** — a
`@RunIf(DockerEnvironment)` miss surfaces as a silent skip (assumption violation), so a bare
"`Tests run` ≥ 1" cannot distinguish "ran and passed" from "gate-skipped".

1. **Docker mode, full-Postgres**: `DITTO_REPO_DIR=<search-worktree> ./start-postgres.sh` (default
   `SEARCH_BACKEND=postgres`) → all containers healthy; policies/things/connectivity/things-search
   logs show the Postgres backend and no missing-plugin errors; `psql \dt` shows journal/snaps
   *and* the search schema tables (this + the things-search log is what actually proves the search
   overlay booted on PG). Then run the tests **in-network** — a host-run `mvn` cannot reach the
   container-hostname cluster (brokers dial back to the `system-test-container` network-alias), so
   use the wrapper `docker/README-postgres.md` documents:
   `docker run --rm --network test --network-alias system-test-container -v "$PWD":/ws -v "$HOME/.m2":/root/.m2 -w /ws maven:3.9-eclipse-temurin-25 mvn verify -am --projects=:system -Dit.test=CleanupIT,QueryThingsIT -Dtest.environment=docker-compose-postgres`
   (both in one invocation, to avoid two ~64-module `-am` builds; assert per the report block above
   — `CleanupIT` exercises the already-shipped `docker-compose` arm, `QueryThingsIT` exercises
   search CRUD/RQL over the PG backend). Then run `./stop-postgres.sh` and assert the stack is gone
   (`docker compose … ps -q` empty) — **this is the only step that exercises the stop-script
   change**.
2. **Docker mode, search fallback** (cheap sanity): restart with `SEARCH_BACKEND=mongodb`; confirm
   things-search boots on Mongo (no PG env/extension mounted) and a created thing is searchable.
3. **IntelliJ mode**: infra containers only (postgres, oauth, brokers). The `.run.xml` files
   themselves are confirmed by the user with a one-click launch in IntelliJ (the IDE cannot be
   driven headlessly, and a wrong module ref or env typo in an XML is caught *only* here). For an
   automated stand-in of the backend wiring, reproduce the run configs' classpath from the launcher
   module — `mvn -pl :ditto-ide-postgres-launcher dependency:build-classpath
   -Dmdep.outputFile=cp.txt` in the ditto worktree, then one
   `java -cp "$(cat cp.txt)" <service-main-class>` per service with the Part-2 env table (the
   launcher has no main class of its own; each service keeps its own starter). This is a set of
   separate clustered processes — **not** `run-compound-postgres.sh`, which is a single compound
   process and only approximates the wiring. Then run
   `mvn verify -am --projects=:system -Dit.test=CleanupIT,QueryThingsIT -Dtest.environment=local-postgres`
   from the host and assert per the report block above. **This `CleanupIT` run is the only automated
   check of the `CommonTestConfig` `local`-arm one-liner** — if it regresses, `CleanupIT` silently
   gate-skips, so the `Skipped: 0` assertion (not "≥ 1") is what catches it. This step also depends
   on **C1** (devops auth) being resolved, or `CleanupIT`'s piggyback calls 401.

## Files to create / modify

**Create**
- `docker/docker-compose-postgres-search.yml`
- `docker/postgres/search-postgres.conf`
- `common/src/main/resources/test-common-local-postgres.conf`
- `intelliJRunConfigurations/Policies for test (Postgres).run.xml`
- `intelliJRunConfigurations/Things for test (Postgres).run.xml`
- `intelliJRunConfigurations/ThingsSearch for test (Postgres).run.xml`
- `intelliJRunConfigurations/Connectivity for test (Postgres).run.xml`
- `intelliJRunConfigurations/Ditto4test (Postgres).run.xml`
- `intelliJRunConfigurations/Postgres.run.xml`

**Modify**
- `docker/docker-compose-postgres.yml` (three-JAR mounts; comments)
- `docker/start-postgres.sh` (preflight, three-JAR build/checks, `SEARCH_BACKEND` compose stack)
- `docker/stop-postgres.sh` (include the search overlay in the stack)
- `common/src/main/java/org/eclipse/ditto/testing/common/CommonTestConfig.java` (`local` prefix-match)
- `README.md`, `docker/README-postgres.md` (Part 3)
- `docs/postgres-system-tests-plan.md` (superseded-by note only)

**Ditto repo — no changes** (`ide-postgres-launcher`, `*-pg-dev.conf`, `ditto-postgres-search.conf`,
`build-images.sh` are all consumed as-is).
