> **Superseded (2026-09-02).** Findings record for the two-script design; the scripts and suffixed test
> environments it reviews were replaced by `DITTO_DB=postgres ./start.sh` + `-Dpersistence.backend=postgres`.
> Current instructions: `docker/README-postgres.md`.

# Critical review — `docs/postgres-system-tests-plan.md`

Reviewed 2026-07-02 against both repos at their current state:
- testing repo `ditto-testing_feat__postgres-persistance` (this repo)
- ditto repo `ditto_feat__postgres-persistance` @ `0ec1c28ee5`

Every factual claim in the plan was checked against source. **Verdict: the plan is
well-researched and structurally sound — Parts A/B/C/D are the right decomposition, and most
"key facts" are code-verified correct. It is NOT executable as written**: it contains one
blocking self-contradiction (F1), one high-severity compose mechanic it doesn't know about
(F2), one compile-breaking module-placement error (F3), and its verification section cannot
be run from a dev host as written. Fix those and this is a good plan.

---

## What the plan gets right (verified)

| Claim | Verified against |
|---|---|
| `CleanupIT` is the only DB-direct test | repo-wide grep for `com.mongodb`/`MongoClient` → single hit: `system/.../cleanup/CleanupIT.java` |
| Mongo dbs/collections/filters (`things/things_journal`, `connection_journal` **singular**, `pid $in`, `events.p.type` regex) | `CleanupIT.java:281-336` |
| Gating fix is genuinely required | `@RunIf(DockerEnvironment.class)` → `CommonTestConfig.isLocalOrDockerTestEnvironment()` (`CommonTestConfig.java:220-223`) matches only `local`/`docker-compose` via `equalsIgnoreCase` |
| `test-common-<env>.conf` auto-pickup + need for explicit include of the docker-compose conf | `CommonTestConfig.java:111-135` (fallback chain is only `test-common.conf`, so inheritance must be an explicit `include`) |
| Postgres tables **plural** `connections_journal`/`connections_snaps`, `pid TEXT`, `event JSONB` | `PostgresSchema.java` — `ENTITIES = [things, policies, connections, wot]`, `createJournalTable()` |
| Cleanup + snapshot streaming actually work on Postgres | Physical `DELETE FROM` in `PostgresPersistenceOperations.java:232,322-346`; `SnapshotStreamingActor` is backend-neutral; provider wires `streaming()/operations()/healthCheck()`; the old "snapshot-streaming gap" (bd `3l6`) is **resolved** — `PostgresStreamingIT` proves it. CleanupIT's piggyback → `persistenceCleanup` path is expected to function. |
| Activation mechanism (`HOSTING_ENVIRONMENT=filebased` + file location + `POSTGRES_*` env; extension JAR under `/opt/ditto/extensions/`) | `dockerfile-snapshot:40` (`CLASSPATH=/opt/ditto/*:/opt/ditto/extensions/*`); `ditto-postgres-persistence.conf` env hooks incl. `POSTGRES_DDL_USER/PASSWORD`, `POSTGRES_SSL_MODE` |
| `POSTGRES_SSL_MODE=disable` is required (not optional) | default is `ssl.mode = "verify-full"` (`ditto-postgres-persistence.conf:345`) — without the override every service fails to connect |
| The odd `overrides { journal-collection … }` placeholder blocks | real and required (`things-pg-dev.conf:57-64`, "so ThingPersistenceOperationsActor can construct") — plan copies them correctly |
| No snapshot-adapter override in the overlays | correct — C3 removed the key; adapter is composed automatically |
| `read-journal.entity` needs no compose wiring | each service conf sets it itself (`things.conf:34`, `policies.conf:24`, `connectivity.conf:102`) |
| "Ditto repo — none required" | extension module is in the reactor, shade plugin produces `ditto-internal-utils-persistence-r2dbc-extension-*.jar`, `build-images.sh` exists |
| Compose image parametrisation + cross-repo relative paths already exist | `docker-compose.yml:114` etc.; `docker-compose.override.yml:101-102` already mounts `./../../ditto/...` |

The plan's two self-flagged caveats (verify the `event->>'type'` JSONB path; verify the
`include classpath("<svc>")` boot chain) are both correctly identified as the risky spots —
but the second is under-weighted (see F4).

---

## Findings

### F1 — BLOCKER: the new test env contradicts itself about where the test JVM runs

`test.environment=docker-compose` is an **in-network** environment: CI attaches the Maven
container to the compose network as `system-test-container`
(`jenkins/Jenkinsfile_system:134`, `:151-153`) and `test-common-docker-compose.conf`
addresses everything by service hostname (`gateway`, `mongodb`, `oauth`). The base
`docker-compose.yml` publishes **no** ports at all — host-port publication comes only from
`docker-compose.override.yml`, which CI deliberately deletes.

Part B inherits that conf and then sets
`postgres.jdbc-uri = "jdbc:postgresql://localhost:5432/ditto"  # host reachable from the test JVM`.
Both cannot be true:

- **In-network JVM** (the thing `docker-compose` means today): `localhost:5432` points at the
  Maven container itself → `PostgresPersistenceInspector` cannot connect. Must be
  `jdbc:postgresql://postgres:5432/ditto`.
- **Host JVM**: the JDBC URI works (with the published port) but every inherited setting is
  broken — `http://gateway:8080`, `mongodb://mongodb:27017` don't resolve from the host.

**Fix:** decide the execution model and make the env self-consistent. Recommended:
`jdbc-uri = "jdbc:postgresql://postgres:5432/ditto"` (+ `${?POSTGRES_JDBC_URI}` env hook for
flexibility), document the env as CI-style in-network, and give README-postgres.md the
runner command (see F8). If host-run development is also wanted, that is a *second* variant
based on `test-common-local.conf` (`local-postgres`), not a tweak to this one.

### F2 — HIGH: explicit `-f` layering silently drops `docker-compose.override.yml`

`docker/docker-compose.override.yml` is applied **only** when compose is invoked without
`-f` flags (that is how `start.sh` works today). It is what publishes gateway 8080, mongo
27017, fixes Kafka's advertised listeners for host clients, and strips the mongo replica-set
arg. The moment `start-postgres.sh` runs
`docker-compose -f docker-compose.yml -f docker-compose-postgres.yml up`, the override file
stops being loaded — a behavior change the plan never mentions, and one that breaks its own
verification steps (step 3's smoke test against the gateway from the host, and any local
psql convenience beyond the port the postgres file itself publishes).

**Fix:** `start-postgres.sh` / `stop-postgres.sh` must pass the full stack explicitly:
`-f docker-compose.yml -f docker-compose.override.yml -f docker-compose-postgres.yml`
(or export `COMPOSE_FILE=docker-compose.yml:docker-compose.override.yml:docker-compose-postgres.yml`),
mirroring CI by simply omitting the override when the file is absent. `stop-postgres.sh`
must use the **same** `-f` set — a bare `docker-compose down` will not know about the
`postgres` service and will leave its container running.

### F3 — HIGH: Part A does not compile as specified — the Mongo driver is in `system`, not `common`

`mongodb-driver-sync` is declared in `system/pom.xml:265` (managed at 5.2.0 in
`bom/pom.xml:361`). `common/pom.xml` has **no** Mongo dependency. The plan puts
`MongoPersistenceInspector` in the `common` module and says "The Mongo driver already
present stays" — as written, `common` fails to compile.

**Fix (pick one):**
- **Recommended:** put `org.eclipse.ditto.testing.*.persistence` in the **system** module
  (e.g. `system/src/test/java/.../persistence/`). `CleanupIT` is its only consumer; both
  drivers then live where they are used and `common` stays lean. The config getters still go
  to `CommonTestConfig`.
- Or keep the package in `common` and add `mongodb-driver-sync` **and** `postgresql` to
  `common/pom.xml`.

### F4 — MEDIUM: the docker overlay confs are new, unproven config surface — not "docker variants of proven overlays"

The plan presents Part C item 2 as adapting `ditto/deployment/postgres-local/*-postgres.conf`.
Those files no longer contain the include chain: they are one-line delegates to the
classpath `<svc>-pg-dev.conf` profiles, which include **`<svc>-dev`** (localhost/dev cluster
settings) — unusable in containers. There is also **no connectivity overlay** in
`postgres-local/` at all (only things + policies + the compound run script); the proven
Postgres runtime is the IntelliJ/CLI compound on one host, never the containerized topology.

So `include classpath("<svc>") + include classpath("ditto-postgres-persistence")` inside a
`HOSTING_ENVIRONMENT=filebased` file has **never booted anywhere**. It is *probably* right —
compose supplies seed nodes via `-D` flags in `JAVA_TOOL_OPTIONS` (system properties override
file config), and the raw service confs are env-var wired — but treat this as
design-to-validate and budget iteration time for all three services, connectivity doubly so.
The plan's per-service `pekko.persistence.*.auto-start-*` narrowing and `overrides { … }`
placeholders match the known-good pg-dev profiles exactly (good); keep the "no
snapshot-adapter" rule too.

### F5 — MEDIUM: Mongo↔Postgres count semantics are close but not provably 1:1

The inspector abstraction assumes `countDocuments == count(*)`. Three gaps to close during
implementation:

1. **Journal batching:** Mongo counts journal *documents*; an atomic batched persist is ONE
   doc with an `events[]` array. Postgres is one row per event. CleanupIT's equality asserts
   pass on Mongo today, so no batching occurs in practice — state that assumption in the
   inspector's javadoc so a future batching change doesn't produce a mystery diff.
2. **Snapshot PK:** Postgres `*_snaps` PK is `(pid, sn, written_at)` — duplicate `(pid, sn)`
   rows are representable (Mongo upserts one doc per `(pid, sn)`). If the store ever
   rewrites a snapshot at the same `sn`, raw `count(*)` diverges from Mongo. Either verify
   `PostgresSnapshotStore` never does that, or make the snaps queries
   `COUNT(DISTINCT sn)` — cheap insurance, identical semantics.
3. **`event->>'type'`:** shape is as the plan hopes — the `event` JSONB column is the full
   Ditto event JSON (adapters bind `ThingEvent`/`PolicyEvent`/`ConnectivityEvent` + a base
   `Event` fallback for the `EmptyEvent` no-op events the Mongo regex exists to exclude), so
   `LIKE 'connectivity.events:%'` is the right filter. Note the Mongo regex is an unanchored
   substring match while `LIKE 'x%'` is anchored — equivalent for these values; fine. The
   plan's "verify against real data" instruction stays (verification step 4 covers it).

### F6 — MEDIUM: the `wot` entity is invisible to the plan

`PostgresSchema.ENTITIES` provisions a fourth table set — `wot_journal`/`wot_snaps` — and
Things routes WoT-validation-config events to `ditto-postgres-wot-journal`
(`ditto-postgres-persistence.conf:62-63,236-263`). The plan never mentions it. Consequences
are mild but should be explicit: (a) the things overlay's narrowed auto-start list
(`things` only, matching `things-pg-dev.conf`) is correct — the wot plugins start lazily on
first use and reuse the thing dispatchers; (b) `PersistenceInspector` intentionally does not
cover wot tables (CleanupIT doesn't either) — say so; (c) any system test that touches WoT
validation-config persistence will exercise this fourth journal on Postgres for the first
time in the full-suite run.

### F7 — MINOR: Maven command errata

- Step 4: `mvn verify --projects=:system -Dtest '*CleanupIT*' …` is wrong twice — missing
  `=`, and failsafe selects ITs with **`-Dit.test=CleanupIT`** (`-Dtest` drives surefire).
- All `mvn verify --projects=:system` invocations need **`-am`** (CI uses `-am -amd`,
  `Jenkinsfile_system:151`); without it the reactor doesn't build `bom`/`common` and the run
  fails unless they happen to be installed.
- (`skipITs` is fine — `system/pom.xml:34` overrides it to `false`; no flag needed.)
- `bom/pom.xml` version-property style: follow `mongo-java-driver.version` (`bom/pom.xml:55`)
  precedent for the new `postgresql.version`.

### F8 — MINOR: the verification section is not executable as written, and misses two traps

- Steps 1, 4, 5 run `mvn … -Dtest.environment=docker-compose[-postgres]` apparently from the
  host — per F1 that env only works in-network. Prescribe the CI-style runner explicitly, e.g.
  `docker run --rm --network test --network-alias system-test-container -v $PWD:/ws -w /ws maven:… mvn …`,
  and put it in README-postgres.md; otherwise step 1's "default unaffected" baseline can't
  even be established.
- Step 4 must assert **"CleanupIT actually ran"** (failsafe summary `Tests run: 1`, not
  skipped). `@RunIf` skips silently — precisely the failure mode of forgetting the gating
  fix this plan itself identified. A green-but-skipped run proves nothing.
- Step 5: call out the known-risk area instead of an undifferentiated "full suite": search
  tests (`system-sync/*`, search ITs) depend on things-search background/full sync against
  Postgres-backed things via the generalized `SnapshotStreamingActor` — implemented and
  unit/IT-proven, but explicitly **not yet proven end-to-end with the search service
  running** (ditto status doc, 2026-07-02 correction). Expect first failures there, not in
  CRUD tests.
- Freshness: `CleanupIT` is documented "should be the only test in a fresh environment". If
  `docker-compose-postgres.yml` gives postgres a named volume (postgres-local uses
  `ditto-pg-data`), re-runs accumulate state; prefer no named volume for the test stack, or
  document `down -v`.

### F9 — NITS

- `TestEnvironment.getForString()` (`common/.../config/TestEnvironment.java:44-52`) matches
  by `startsWith`, so `"docker-compose-postgres"` already maps to `DOCKER_COMPOSE` — no
  change needed there, but worth a code comment. Alternatively, make
  `isLocalOrDockerTestEnvironment()` use the same `startsWith` convention instead of adding
  a third constant — smaller diff, auto-covers future suffixed variants. Either is fine;
  just be deliberate.
- The gating change affects more than CleanupIT: `ServiceEnvironment.java:186` also branches
  on `isLocalOrDockerTestEnvironment()` (solution/auth setup). That is the *desired*
  behavior for the new env, but list it as an affected call-site so the reviewer of the
  implementation isn't surprised.
- `WAIT_FOR_MONGO_WRITES` and its log line ("writes on MongoDB") become misnomers once the
  test is backend-neutral — rename to `WAIT_FOR_PERSISTENCE_WRITES` while refactoring.
- `docker-compose.env` (Mongo hostnames etc.) keeps being injected into the three
  Postgres-backed services — harmless (their Mongo config resolves but the provider ignores
  it), worth one line in the compose file comment so nobody "cleans it up" and breaks
  things-search.
- Plan line 25: "stock `dockerfile-snapshot` globs" — confirmed, but note the images must be
  built from the **feature branch** ditto worktree (`DITTO_REPO_DIR`), not any sibling
  `ditto` checkout; the plan's default `./../../ditto` points at whatever sits at that path.
  With multiple worktrees around (this workspace has several), a wrong default silently runs
  Mongo-era images. Recommend `start-postgres.sh` fail fast if the extension module or
  `ditto-postgres-persistence.conf` is absent from `$DITTO_REPO_DIR`.

---

## Recommended plan amendments (checklist)

1. **F1** Rewrite Part B: `jdbc-uri = "jdbc:postgresql://postgres:5432/ditto"` (+ env-var
   hook), declare the env in-network/CI-style, add the containerized runner command.
2. **F2** start/stop-postgres.sh: explicit three-file `-f` stack (or `COMPOSE_FILE`),
   identical set in stop.
3. **F3** Move the inspector package to `system` (or add the Mongo driver to `common`).
4. **F4** Reframe Part C item 2 as new config to validate; note there is no proven
   containerized Postgres topology yet and no connectivity overlay to crib from.
5. **F5** Inspector: document the no-batching assumption; use `COUNT(DISTINCT sn)` for
   snaps or verify single-row-per-sn.
6. **F6** Add a WoT paragraph (fourth table set exists; out of inspector scope).
7. **F7** Fix Maven flags: `-Dit.test=CleanupIT`, add `-am`.
8. **F8** Verification: in-network runner for steps 1/4/5; "CleanupIT ran, not skipped"
   check; flag search-sync as the known-risk area of step 5; no named volume (or `down -v`).
