# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

System tests for [Eclipse Ditto](https://eclipse.dev/ditto/) IoT platform. Tests validate API stability, prevent regressions, and verify functionality from a user perspective. Written in Java 25, built with Maven.

## Build & Test Commands

```bash
# Compile all modules
mvn compile

# Run all system integration tests (requires running Ditto stack + Docker services)
mvn verify -am -amd --projects=system

# Run acceptance tests against a remote Ditto deployment
mvn verify -am -amd --projects=system -Pacceptance -Dtest.environment=deployment -Dtest.timeoutMs=240000 -Dskip.categories=ServiceBus

# Run a single test class (failsafe IT pattern)
mvn verify -am -amd --projects=system -Dit.test=org.eclipse.ditto.testing.system.things.rest.ThingIT

# Skip specific test categories
mvn verify -am -amd --projects=system -Dskip.categories=ServiceBus

# Check license headers
mvn license:check
```

**Note:** Integration tests are disabled by default (`<skipITs>true</skipITs>` in root POM). The `system` module overrides this to `false`.

## Test Prerequisites

Tests require Docker services running before execution:

```bash
cd docker
docker-compose up -d mongodb    # Required: MongoDB
docker-compose up -d oauth      # Required: OAuth mock server (mock-oauth2-server)
docker-compose up -d artemis    # AMQP broker (for connectivity tests)
docker-compose up -d mqtt       # MQTT broker (for connectivity tests)
docker-compose up -d kafka      # Kafka broker (for connectivity tests)
docker-compose up -d rabbitmq   # RabbitMQ broker (for connectivity tests)
```

Plus a running Ditto stack (via `docker/start.sh` or IntelliJ run configs in `intelliJRunConfigurations/`).

## Module Structure

| Module        | Purpose                                                       |
|---------------|---------------------------------------------------------------|
| `bom`         | Bill of Materials — centralizes all dependency versions       |
| `common`      | Shared test utilities, HTTP clients, matchers, protocol impls |
| `system`      | Integration test suite (test classes use `*IT` suffix)        |
| `system-sync` | Sync-specific test suites (4 sub-modules)                     |

## Architecture & Key Patterns

### Test Base Classes (in `common`)

- **`IntegrationTest`** — Primary base class for all system tests. Provides REST helper methods (`putThing`, `getThing`, `deleteThing`, etc.), custom matchers (`GetMatcher`, `PutMatcher`, `DeleteMatcher`), and `TestingContext` management.
- **`SearchIntegrationTest`** / **`VersionedSearchIntegrationTest`** — Base classes for search-related tests.

### Test Configuration

Configuration uses TypeSafe Config via `CommonTestConfig` singleton:
- Default config: `test.conf` on classpath
- Environment-specific overlay: `test-{environment}.conf` (set via `-Dtest.environment=...`)
- Environment variables override config (e.g., `DEFAULT_HOSTNAME`, `BASIC_AUTH_ENABLED`, `OAUTH_TOKEN_ENDPOINT`)

### Test Conventions

- Integration test classes use the `*IT` suffix (Maven Failsafe convention)
- Tests requiring Docker use `@RunIf(DockerEnvironment.class)` for conditional execution
- Acceptance test subset is marked with `@Category(Acceptance.class)`
- JUnit 4 with `@Rule`-based resource management (`TestSolutionResource`, `ConnectionsHttpClientResource`)
- Async assertions via Awaitility; REST assertions via REST-assured + custom matchers

### Connectivity Protocol Implementations

The `common` module provides clients for all Ditto-supported connectivity protocols:
- AMQP 1.0 (Qpid JMS + Vert.x Proton)
- MQTT 3/5 (HiveMQ client)
- Kafka (Apache Kafka clients)
- RabbitMQ (AMQP 0.9.1 client)
- HTTP Push

Test classes in `system/src/test/java/.../connectivity/` test each protocol via `AbstractConnectivity*ITestCases` base classes.

## Branching Convention

Feature branches in this repository use the **same branch name** as the corresponding feature branch in the main Ditto repository ([github.com/eclipse-ditto/ditto](https://github.com/eclipse-ditto/ditto)). This keeps test PRs aligned with the Ditto code changes they validate.

Changes to `CLAUDE.md` and the `.claude/` folder must **only** be committed on the `claude` branch — never on feature branches or `main`.

## Code Style

- Google Java Style Guide with **120-character** line limit
- EPL-2.0 license header required on all `.java`, `.scala`, and `pom.xml` files (validated by `license-maven-plugin` during build)
- Sign commits with `-s` flag (Eclipse Foundation CLA requirement)
