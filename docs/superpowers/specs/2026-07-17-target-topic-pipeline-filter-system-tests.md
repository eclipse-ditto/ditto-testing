# Target Topic Pipeline Filter — System Tests (ditto-testing)

> **SYNTAX CHANGE 2026-09-14 (rev 3, separate `fn-filter` parameter):** main-repo commit `8d06aa9f89`
> ("fn-filter and filter as separate connection topic parameters", 2026-07-31, replacing the vanished
> `9ad6669a1c`) splits the two filter kinds by NAME: `filter` is RQL-only, `fn-filter` holds the (optionally
> `|`-chained, max 10 stages) placeholder pipeline; both may be present and are ANDed:
> `?filter=gt(attributes/counter,42)&fn-filter=fn:filter(...)`. An `fn:` expression placed in `filter` → 400
> `connectivity:connection.configuration.invalid` whose description points to `fn-filter` (raised BEFORE the
> RQL parser). Repeating ANY query param (`filter`, `fn-filter`, `namespaces`) is no longer a defined 400 — it is
> the parser's generic duplicate-key rejection like on eclipse master, so the two "two params" REST tests of rev 2
> were REMOVED and replaced by `createConnectionWithPipelineExpressionInRqlFilterParamFailsPointingToFnFilter`.
> Updated: `ConnectivityFactory` (all four pipeline connection builders now emit `fn-filter=`), scenario comments
> in `AbstractConnectivityITestCases`, `RestConnectionsIT` (now 8 pipeline tests). Runtime scenarios A–E2 unchanged
> in substance. **Placeholder placement (verified 2026-09-14):** `8d06aa9f89` accepts BOTH documented forms —
> function-first `fn:filter(header:x,...)` (placeholder as function parameter; the primary docs example) and
> placeholder-first `header:x|fn:filter(...)` (placeholder feeds the pipeline, consistent with the placeholder
> function docs). They differ only for an ABSENT header: function-first applies the rqlFunction to the missing
> value (`ne` → published, the "ne trap"), placeholder-first never resolves (always suppressed). The runtime
> scenarios keep the function-first form (scenario E2 pins the ne trap); NEW coverage for placeholder-first:
> runtime scenario E3 `suppressLiveMessagesWithAbsentOriginHeaderForPlaceholderFirstNeOriginPipelineFilter`
> (new category `CONNECTION_WITH_PLACEHOLDER_FIRST_ORIGIN_PIPELINE_FILTER`, connection #20, topic
> `live/messages?fn-filter=header:ditto-origin|fn:filter('ne','<conn1>')`: HTTP-sent = absent origin → suppressed,
> via connection1 → suppressed, via connection2 → delivered), a placeholder-first topic in the REST round-trip
> test, and `createConnectionWithNamelessLeadingPlaceholderInFnFilterFails` (`header:|fn:filter('eq','x')` → 400). **Status: re-verified 2026-09-14 against ditto 8d06aa9f89 — RestConnectionsIT 18/18 (133 s), Amqp10ConnectivityIT 7/7 (117 s).**

## Verification evidence (rev 3 + gap closure, 2026-09-14)

Full-suite proof of all 10 `PIPE` runtime scenario methods (Tasks 0–5, including the three new Task 5 scenarios and the Task 4 REST additions) on every broker suite, plus the whole `RestConnectionsIT` class. Ditto commit under test: `8d06aa9f89`. Testing commit under test: `449bab4`. Stack: host-run IntelliJ Ditto (gateway `localhost:8080`, devops `devops:foobar`), dockerized artemis/rabbitmq/mqtt/kafka/oauth/ssh, local mongod.

`PIPE` = `sendCommandsConsumeEventsFilteredByPipelineOriginatorFilter+sendCommandsConsumeEventsFilteredByCombinedRqlAndPipelineFilter+publishEnrichedSignalsFilteredByPipelineOriginatorFilter+filterLiveMessagesByPipelineOriginatorFilter+consumeEventsFilteredByOriginPipelineFilter+deliverLiveMessagesWithAbsentOriginHeaderForNeOriginPipelineFilter+suppressLiveMessagesWithAbsentOriginHeaderForPlaceholderFirstNeOriginPipelineFilter+publishLiveCommandsFilteredByChainedLikeAndOriginPipelineFilter+consumeOnlyModifiedTwinEventsForTopicActionPipelineFilter+suppressLiveMessagesWithAbsentOriginHeaderForTwoParamExistsPipelineFilter`

| Suite | Result |
|---|---|
| `Amqp10ConnectivityIT` | 10/10 (165.1 s) |
| `RabbitMqConnectivityIT` | 10/10 (166.1 s) |
| `Mqtt3ConnectivitySuite` | 10/10 (216.9 s) |
| `Mqtt5ConnectivitySuite` | 10/10 (217.8 s) |
| `KafkaConnectivitySuite` | 10/10 (195.0 s) |
| `RestConnectionsIT` | 21/21 (147.8 s) |

Unit run: `TargetTopicFilterTest` 46/46, `ImmutableFilteredTopicTest` 34/34 (Task 1 run, main repo commit `b0fc40543f`); full 4-class re-run owed once the IntelliJ stack is stopped (Ruling 9).

No flakes: every broker suite and `RestConnectionsIT` passed green on the first attempt, sequential one-Maven-invocation-per-class, run 2026-09-14.

> **SYNTAX CHANGE 2026-07-31 (rev 2, chained-pipeline redesign — SUPERSEDED by rev 3 above):** a target topic carries at most TWO
> `filter` query params — at most one RQL expression and at most one `fn:` pipeline expression, ANDed:
> `?filter=gt(attributes/counter,42)&filter=fn:filter(...)`. Several pipeline conditions are chained with
> `|` INSIDE the single `fn:` param (AND, max 10 stages): `?filter=fn:filter(...)|fn:filter(...)`. A second
> param of either kind → 400 `connectivity:connection.configuration.invalid`; the legacy `rql|fn:` single
> param (not starting with `fn:`) → 400 `rql.expression.invalid`. Rev 1 of this banner (repeated single-stage
> `fn:` params, "exactly one fn: stage per param") was implemented in ditto `ca17002257` + here in `6c94457`
> and then REVERSED by the user's design call — chaining is the documented pipeline idiom. Updated here:
> `RestConnectionsIT` (two-fn:-params negative test replaces the multi-stage one; test I adds a chained topic
> and per-topic-anchored round-trip assertions), stale scenario-B comments. `ConnectivityFactory` unchanged
> (its RQL+fn: two-param topic is valid under both revisions). Runtime scenarios A–E2 unchanged.

**Date:** 2026-07-17 · **Branch:** `feature/target-topic-pipeline-filter` (ditto-testing) · **Commits:** `d862134..adc961c` — rev-1: `761eed2` + `6c94457` (per-task shas in the table below are historical, pre-squash; the 2026-07-17 "review fixes" are long since committed); rev-2: `7eb7c7a` + evidence `adc961c` · **Status:** rev 3 re-verified 2026-09-14 against ditto `8d06aa9f89` (RestConnectionsIT 18/18, Amqp10ConnectivityIT 7/7), committed as `4cde8d7`, NOT pushed; the rev-2 evidence below (RestConnectionsIT 8/8, Amqp10ConnectivityIT 6/6 against ditto `9ad6669a1c`) is for the superseded syntax.

Companion to the feature in the main ditto repo (same-named branch, commits `b369d86bd4..26f68dd19f`, spec `docs/superpowers/specs/2026-07-14-target-topic-pipeline-filter-plan.md` there). Execution plan + full task briefs/reports/review packages: `.superpowers/sdd/` in this worktree (ledger: `progress.md`); the master plan file was `~/.claude/plans/for-the-feature-implementedf-memoized-acorn.md`.

## What was added

| Commit | Content |
|---|---|
| `5f121f8` | 4 `ConnectionCategory` values + `ConnectivityFactory` plumbing (fields, both constructors, switch, `allConnectionNames`, supplier map, 4 setup methods). Excluded subject wired as `integration:<username>:<connectionName1>`; origin category uses bare `connectionName1` (connection ID == name). |
| `665203e` | `AbstractConnectivityITestCases`: scenario A `sendCommandsConsumeEventsFilteredByPipelineOriginatorFilter` (pure `fn:filter(header:ditto-originator,'ne',…)` — exclude a specific OTHER originator from a target) + B `sendCommandsConsumeEventsFilteredByCombinedRqlAndPipelineFilter` (`gt(attributes/counter,42)|fn:…` AND-quadrants). |
| `5c202a9` | Scenario C `publishEnrichedSignalsFilteredByPipelineOriginatorFilter` (`extraFields=attributes/counter` → post-enrichment re-evaluation; suppression holds despite enrichment) + D `filterLiveMessagesByPipelineOriginatorFilter` (live-messages topic — RQL can't do this, pipeline can). |
| `6b16805` | E1 `consumeEventsFilteredByOriginPipelineFilter` (`header:ditto-origin` 'eq': HTTP-triggered events suppressed — absent header drops) + E2 `deliverLiveMessagesWithAbsentOriginHeaderForNeOriginPipelineFilter` (the "ne trap": absent `ditto-origin` RESOLVES with 'ne' → HTTP live message delivered). |
| `e099d7a` | `rest/RestConnectionsIT`: F `fn:unknownfn('x')` → 400 `connectivity:connection.configuration.invalid`; G malformed RQL head in combined → 400 `rql.expression.invalid`; H whitespace-only `?filter=%20%20` → 400 `rql.expression.invalid`; I valid pure/combined/unknown-rqlFunction-name (`'nope'`) → 201 + round-trip + delete. Helper `connectionWithTargetTopics(String…)`. |

All six runtime tests: `@Category(RequireSource.class)` → HttpPush suites skip them (correct: its `sendSignal` goes via WebSocket with the oauth subject, so the originator premise doesn't exist there).

## Verification evidence (2026-07-31, chained-pipeline redesign)

Stack: host-run IntelliJ Ditto rebuilt from the main repo's redesign work, since squashed into `9ad6669a1c` (byte-identical tree `d167b4c35a` — the exact content the stack was built from), dockerized brokers/oauth via `docker compose ... up -d artemis mqtt kafka rabbitmq oauth ssh`. Testing-repo commit under test: `7eb7c7a`.

Freshness probes (dry-run `POST /api/2/connections?dry-run=true`, devops auth): two-`fn:`-params topic → 400 `connectivity:connection.configuration.invalid` "declares 2 pipeline 'filter' parameters - at most one pipeline filter is allowed per topic" (NEW build; the rev-1 build accepted this form); chained `fn:filter(...)|fn:filter(...)` topic → HTTP 200 (rev-1 build rejected it with "exactly one 'fn:' stage").

| Suite | Result |
|---|---|
| `RestConnectionsIT` (8 pipeline tests incl. NEW `createConnectionWithTwoPipelineTargetTopicFilterParamsFails` and chained-topic round-trip in test I) | 8/8 PASS (60.4 s) |
| `Amqp10ConnectivityIT` (runtime scenarios A–E2 incl. scenario B one-RQL+one-fn: two-param topic) | 6/6 PASS (100.3 s) |

The chained topic round-trips verbatim (`fn:filter(header:ditto-origin,'ne','chained-excluded-connection')|fn:filter(header:ditto-originator,'exists')` asserted in the GET body) — no URL-encoding drift in `ImmutableFilteredTopic` serialization.

## Verification evidence (2026-07-17, host-run stack rebuilt from the feature branch — RETIRED rev-1 syntax)

Pre-check probe: dry-run `POST /api/2/connections?dry-run=true` with a pipeline filter — stale build rejects with `rql.expression.invalid` ("Invalid input 'f'"), feature build passes validation. `fn:unknownfn` probe returns the feature's own `connectivity:connection.configuration.invalid` message.

| Suite | Result |
|---|---|
| `Amqp10ConnectivityIT` (6 new tests) | 6/6 PASS (102 s) |
| `Mqtt3ConnectivitySuite` | 6/6 PASS (129 s) |
| `Mqtt5ConnectivitySuite` | 6/6 PASS (131 s) — review-fix run, see below |
| `KafkaConnectivitySuite` | 6/6 PASS (114 s) |
| `RabbitMqConnectivityIT` | 6/6 PASS (97 s) |
| `RestConnectionsIT` (4 new tests) | 4/4 PASS (24 s); re-run 5/5 PASS (42 s) incl. new modify-path test, see below |
| Neighbor regression `Amqp10ConnectivityIT#sendMultipleCommandsConsumeEventsFilteredByRql+publishEnrichedSignals` | 2/2 PASS |

- **E1 empirically resolved the plan's one runtime-only risk:** `ditto-origin` DOES reach `SignalFilter` end-to-end; 'eq' suppression of HTTP-triggered events works.
- HttpPush skip verified by annotation mechanics only (a `Categories`-runner wrapper swallows `-Dit.test` method filters — see below — so an empirical skip-check would require the full HttpPush suite; disproportionate).

## Run recipe for THIS machine (host-run IntelliJ Ditto + dockerized brokers)

```bash
cd <ditto-testing worktree>
/opt/homebrew/Cellar/sdkman-cli/5.18.2/libexec/candidates/maven/3.9.3/bin/mvn verify -am -amd --projects=:system \
  -Dtest.environment=local \
  -Dgateway.devops.auth.enabled=true -Dgateway.devops.auth.password=foobar \
  -DfailIfNoTests=false \
  -Dit.test='<Class>#<method>[+<method>…]'
```

Hard-won corrections vs the original plan:
1. **`-Dtest.environment=docker-compose` is CI in-network mode** (test JVM runs as `system-test-container` inside the compose network; hostnames `oauth`/`artemis`/`gateway` are container names → `UnknownHostException: oauth` from the host). For host-run tests use **`local`** (the default), whose `test-common.conf` defaults match the published ports exactly: gateway `localhost:8080`, oauth `localhost:9900`, artemis `localhost:5673` (host port!), rabbitmq `5672`, kafka `9092`, mqtt `1883/8883`, mongo `27017`, ssh `2222`.
2. **Devops auth:** `test-common-local.conf` defaults `gateway.devops.auth.enabled=false`, but the host gateway enforces devops auth (`devops:foobar`). `CommonTestConfig` uses `ConfigFactory.load(...)`, so `-D` system properties override the conf files — hence the two `-Dgateway.devops.auth.*` props.
3. **Suite wrappers swallow method filters:** `Mqtt3ConnectivityIT`/`Mqtt5ConnectivityIT`/`KafkaConnectivityIT`/`HttpPushConnectivityIT` are `@RunWith(Categories.class)` wrappers — `-Dit.test=Mqtt3ConnectivityIT#method` runs 0 tests silently. Target `Mqtt3ConnectivitySuite`/`Mqtt5ConnectivitySuite`/`KafkaConnectivitySuite` instead (safe for our tests: the Mqtt3 wrapper excludes only `RequireProtocolHeaders`, the Kafka wrapper only `RequireSshTunnel`, the Mqtt5 wrapper nothing — the new tests use neither category). `Amqp10ConnectivityIT`/`RabbitMqConnectivityIT` are direct test classes.

## Post-verification fixup `7e7682e` (2026-07-17, user-prompted)

User correctly flagged the scenario-A comment's "exclude own echo" framing: **Ditto already never publishes a signal back to the connection that caused it** — the pre-existing `"Was sent by myself."` drop in `OutboundDispatchingActor.handleSignal` (baseline eclipse code, `getOrigin()` vs own connection ID, runs BEFORE `SignalFilter`). The feature's actual value is excluding a specific *OTHER* originator (same app via another connection, or same subject via HTTP) — which the built-in drop cannot do (it keys strictly on the same connection ID). Tests were never affected (the filtering target is a third connection); comments reworded, and the scenario-C "AttributeModified"→"AttributeCreated" review nit folded in. The main-repo feature docs were checked and are already correctly worded (`basic-connections.md` line ~349 scopes `ditto-origin` to "*other* connections").

## Review fixes (2026-07-17, uncommitted working-tree changes, from review `docs/superpowers/reviews/2026-07-17-pipeline-filter-system-tests-review.md`)

1. **I-1 (modify-path REST validation):** new test `RestConnectionsIT#modifyConnectionRevalidatesPipelineTargetTopicFilters` — GETs the `@Before` default connection, PUTs it with `?filter=fn:unknownfn('x')` expecting 400 `connectivity:connection.configuration.invalid`, then PUTs a valid pipeline filter expecting 204 (helper `withTargetTopics(JsonObject, String...)` rewrites all targets' topics). Run evidence: `RestConnectionsIT` 5/5 PASS (41.8 s) — the 4 existing pipeline REST validation tests + the new modify test.
2. **M-1:** scenario-C suppression comment reworded — suppression happens at the pre-enrichment gate (SignalFilter short-circuits pipeline non-matches before enrichment) even though the topic carries extraFields; the *delivered* events in the same test are what prove the post-enrichment re-evaluation reaches the same verdict.
3. **M-3:** `Mqtt5ConnectivitySuite` executed for the six new tests — 6/6 PASS (131 s), row added to the suite table above; the wrapper-exclusion sentence in the run recipe corrected (Kafka's wrapper excludes `RequireSshTunnel`, not `RequireProtocolHeaders`).
4. **M-4:** `createConnectionWithValidPipelineTargetTopicFilters` now deletes its connection in a `finally` block, so it no longer leaks on a mid-test assertion failure (covered by the same 5/5 run).

### Main-repo `290a971839` (pipeline evaluation failure → connection-log FAILURE entry): deliberately NOT system-tested

The main repo added (same day) a user-visible connection-log FAILURE entry whenever *evaluating* a pipeline filter throws at runtime, at both guarded sites (SignalFilter pre-enrichment gate, OutboundMappingProcessorActor post-enrichment re-evaluation); ordinary non-matches stay silent. This observable is not reachable through the public API: `TargetTopicFilter.validatePipelineFilter` evaluates the complete pipeline expression at connection create/modify time against a strict validation resolver (every placeholder resolves to a dummy value), so any expression that throws structurally (unknown function, bad signature, too many stages) is rejected with 400 before it can ever be persisted — exactly what the REST validation tests above prove. At runtime only the placeholder *values* differ, and value differences surface as unresolved pipeline elements (ordinary non-match), not as throws. A trigger would need a pre-seeded legacy connection bypassing validation, which the system-test API cannot create. The behavior is covered in the main repo by the actor-level unit test `SignalFilterWithFilterTest#applySignalFilterWithFailingPipelineFilterDropsTargetAndRecordsConnectionLogFailure` (which injects the never-persistable `fn:unknownfn('x')` directly into the model). Note: the 2026-07-17 runs above ran against a stack built *before* `290a971839`; irrelevant, since none of the executed tests can reach the new log entry.

## Open leftovers (cosmetic, from final review + 2026-07-17 review — batch into any future fixup)

1. ~~Comment in scenario C "AttributeModified"~~ — fixed in `7e7682e`.
2. Style: `setupSingleConnectionWithPipelineFilter` hoists the filter string; the origin variant inlines its two (eq/ne) strings.
3. Excluded-subject literal `"integration:" + username + ":" + connectionName1` in the supplier map re-derives what `connectionAuthIdentifier` encodes (mirrors existing CONNECTION1 entry style).
4. ~~Review M-2: the `like`/2-param-`exists` absent-header rows of the docs matrix have no system test (`eq`/`ne` are covered; the matrix is unit-tested in the main repo). Bolting extra topics onto the existing origin-filter connection is unsafe — topics OR across a target, so an added always-matching topic would un-suppress the existing assertions; a proper fix needs its own topic/target and test.~~ — closed by suppressLiveMessagesWithAbsentOriginHeaderForTwoParamExistsPipelineFilter (2026-09-14)
5. Review M-6: the six near-identical 20-line DEFAULT/RESTRICTED policy blocks in `AbstractConnectivityITestCases` could share a small `policyWithRestrictedReader(reader, writers...)` helper (~100 lines; `putPolicyForThing` cannot be reused — it grants WRITE to all subjects, the tests deliberately keep the observing target read-only).
6. `like` on an ABSENT header (docs: dropped unless pattern matches `''`) is unit-only (`matchesFnFilterAbsentHeaderLikeDrops` / `…MatchAllPatternPublishes`) — no system path produces an authenticated signal without `ditto-originator`.
7. Pre-existing `FilteredTopic` behavior (eclipse master, not this feature): topic query values are URL-decoded on every parse and never re-encoded on serialization, so an encoded `+` (`%2B`) in a `filter`/`fn-filter` compared value becomes a space after the first persistence cycle; `%7C` round-trips. The docs sentence "a literal + ... must itself be URL-encoded" holds only for the first parse. Round-trip test uses `%7C` only (see `createConnectionWithValidPipelineTargetTopicFilters`). Candidate upstream fix: re-encode in `ImmutableFilteredTopic.toString()`.
8. `setupSingleConnectionWithPipelineFilterMatrix` declares an unused `excludedOriginatorSubject` parameter (plan-mandated; remove in a cleanup).

Final whole-branch review verdict: **Ready** (0 Critical / 0 Important). Every filter string, suppress/deliver outcome, and error code was cross-verified against `TargetTopicFilter`, `SignalFilter`, `OutboundMappingProcessorActor`, `ConnectionValidator`, `PipelineFunctionFilter`, `ImmutableFilteredTopic` in the main repo.
2. (2026-07-31 final review, optional) Main-repo `TargetTopicFilterTest`: add a rejection test for a bare placeholder mid-pipeline (`fn:filter(header:a,'exists')|header:foo`) to pin the docs' restrictions bullet to code, and optionally one for an empty middle stage (`fn:a||fn:b`). Enforcement lives in the untouched placeholders module; behavior verified correct in review.
