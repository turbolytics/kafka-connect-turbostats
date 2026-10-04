# Kafka Connect reporter, Plan A: every task reports its state Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A jar that a Kafka Connect worker loads from its classpath and that reports one signed TurboStats bundle per local task every interval: identity, state, restarts, counter epoch, Connect's counters, acknowledged writes, process memory, and an exit bundle when a connector is stopped or deleted.

**Architecture:** One jar, three entry points. A REST extension (`TurboStatsExtension`) parses `turbostats.*` worker properties and starts one daemon thread. A producer interceptor and a consumer interceptor count task starts, acknowledgments and consumed batches into a static registry the extension reads; all three load from the application classloader because the jar sits on the worker classpath. Every interval the thread reads Connect's JMX metrics and cluster state, builds a bundle per local task, signs it with Ed25519 and posts it asynchronously.

**Tech Stack:** Java 17, Maven, Kafka Connect API 3.8.0 (provided), JDK `HttpClient` and Ed25519, no runtime dependencies. Tests: JUnit 5, networknt json-schema-validator, Testcontainers.

**Spec:** `turbolytics/sql-flow` `docs/superpowers/specs/2026-10-03-turbostats-kafka-connect-design.md` (PR #431 merged; corrections in PR #435). Read its "Kafka Connect and Debezium" section and "What the spike verified" before starting.

**Scope:** Plan A of two. Plan B adds Debezium's snapshot and streaming fields (`backfill`, `source_connected`, event lag from `MilliSecondsBehindSource`), sink event lag from the consumer interceptor, sink lag in messages from the broker, and wire bytes. Plan A's bundles are complete and valid without them: every Plan B field is optional in the contract.

## Global Constraints

- Java 17 (`maven.compiler.release` 17). Ed25519 and `HttpClient` come from the JDK.
- **Zero runtime dependencies.** The jar lands in `/kafka/libs`, beside Kafka's own jars; a bundled library could clash with Kafka's. Kafka, Connect and SLF4J are `provided`.
- **The jar installs on the worker classpath, never the plugin path.** In the plugin path every source task fails with `Failed to construct kafka producer`.
- **Nothing in this jar may stop a worker or a task.** Interceptors swallow every `Throwable`. The extension's `configure` and `register` catch every `Throwable` and leave reporting off.
- Contract field names, exactly as `turbostats/wire/bundle.go` spells them. Absent and zero are different facts: an optional field the reporter cannot measure is left out, never sent as 0.
- `instance.id` = `<cluster>/<connector>/<task>`; `instance.name` = `<cluster>/<connector>`; `instance.runtime` = `kafka-connect`.
- Signing is byte-for-byte the Go reference: canonical string `v1\n<METHOD>\n<path>\n<unix seconds>\n<hex sha256(body)>`, headers `X-Turbostats-Key-Id`, `X-Turbostats-Timestamp`, `X-Turbostats-Signature` (standard base64), `Content-Type: application/vnd.turbolytics.turbostats.v1+json`. The vectors in `src/test/resources/vectors.json` are the authority.
- Worker properties: `turbostats.report.to`, `turbostats.key`, `turbostats.cluster` (default `group.id`), `turbostats.interval.seconds` (default 60), `turbostats.timeout.seconds` (default 10, less than the interval), `turbostats.label.<key>`.
- The credential never appears in a log line, an exception message or a bundle.
- A failing receiver logs one warning when failing begins and one info line when it recovers, debug in between, as SQLFlow's reporter does.
- Prose in comments, README and commit messages follows `turbolytics/sql-flow`'s CLAUDE.md: Google Technical Writing One, SQLFlow and TurboStats named as products. Comments explain why. Commit messages name the defect, the fix and the evidence.
- CI never caches build artifacts between runs.
- Every Maven command in this plan runs through `scripts/mvn`, which runs Maven in Docker, outside the sandbox.

## Review Focus

The five conditions most likely to bite a user that no task's main tests exercise, and where each is pinned:

1. **A connector name with hyphens, dots or JMX-special characters, such as `orders-cdc-v2` or `a:b`.** Its tasks must still be found, keyed and named. Pinned in Task 6 (`TaskKeyTest.aConnectorNameWithHyphensKeepsThem`) and Task 9 (`ConnectMetricsTest.aQuotedConnectorNameIsUnquoted`).
2. **A worker that runs no tasks.** It must send nothing and log nothing. Pinned in Task 10: `TaskCollectorTest.aWorkerWithNoTasksSendsNothing`.
3. **A receiver that answers 401 forever,** because the key was never registered. The reporter must log one warning and keep the worker healthy. Pinned in Task 11: `ReporterTest.aRefusingReceiverWarnsOnce`.
4. **A task whose Connect metrics are not registered yet,** because it is starting or moving. It must be skipped, not reported with zeros. Pinned in Task 10: `TaskCollectorTest.aTaskWithoutMetricsIsSkipped`.
5. **A label value with quotes, newlines or non-ASCII text.** The bundle must stay valid JSON and pass the schema. Pinned in Task 3: `BundleSchemaTest.aLabelWithEscapesValidates`.

---

### Task 1: The build

**Files:**
- Create: `pom.xml`
- Create: `scripts/mvn`
- Create: `.github/workflows/ci.yml`
- Create: `src/main/java/io/turbolytics/turbostats/connect/ReporterVersion.java`
- Test: `src/test/java/io/turbolytics/turbostats/connect/ReporterVersionTest.java`

**Interfaces:**
- Produces: `ReporterVersion.get(): String`, the jar's `Implementation-Version`, or `dev` outside a jar.

- [ ] **Step 1: Write the build files**

`pom.xml`:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>io.turbolytics</groupId>
  <artifactId>kafka-connect-turbostats</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <name>kafka-connect-turbostats</name>
  <description>Reports Kafka Connect connectors to TurboStats.</description>

  <properties>
    <maven.compiler.release>17</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <kafka.version>3.8.0</kafka.version>
  </properties>

  <dependencies>
    <!-- Provided by the worker. The jar bundles nothing: it sits in
         /kafka/libs beside Kafka's own jars, where a second copy of any
         library could shadow the worker's. -->
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>connect-api</artifactId>
      <version>${kafka.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
      <version>${kafka.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-api</artifactId>
      <version>1.7.36</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>javax.ws.rs</groupId>
      <artifactId>javax.ws.rs-api</artifactId>
      <version>2.1.1</version>
      <scope>provided</scope>
    </dependency>

    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <version>5.11.4</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>com.networknt</groupId>
      <artifactId>json-schema-validator</artifactId>
      <version>1.5.6</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>testcontainers</artifactId>
      <version>1.20.6</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-simple</artifactId>
      <version>1.7.36</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <finalName>kafka-connect-turbostats-${project.version}</finalName>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>3.5.2</version>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-failsafe-plugin</artifactId>
        <version>3.5.2</version>
        <executions>
          <execution>
            <goals>
              <goal>integration-test</goal>
              <goal>verify</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-jar-plugin</artifactId>
        <version>3.4.2</version>
        <configuration>
          <archive>
            <manifest>
              <addDefaultImplementationEntries>true</addDefaultImplementationEntries>
            </manifest>
          </archive>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
```

`scripts/mvn` (make it executable with `chmod +x scripts/mvn`):

```sh
#!/bin/sh
# Runs Maven in Docker, so a machine without a JDK can build and test.
# Integration tests start containers through the host's Docker socket, and
# reach them through the host's gateway.
exec docker run --rm \
  -v "$PWD":/w -w /w \
  -v "$HOME/.m2":/root/.m2 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal \
  maven:3.9-eclipse-temurin-17 mvn -B "$@"
```

`.github/workflows/ci.yml`:

```yaml
name: ci
on:
  push:
    branches: [main]
  pull_request:
jobs:
  verify:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      # No cache: every run resolves its dependencies fresh.
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "17"
      - run: mvn -B verify
```

- [ ] **Step 2: Write the failing test**

`src/test/java/io/turbolytics/turbostats/connect/ReporterVersionTest.java`:

```java
package io.turbolytics.turbostats.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ReporterVersionTest {
    // Tests run from classes, not the jar, so there is no manifest to read.
    @Test
    void outsideAJarTheVersionIsDev() {
        assertEquals("dev", ReporterVersion.get());
    }
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `scripts/mvn -q test -Dtest=ReporterVersionTest`
Expected: FAIL to compile with `cannot find symbol: variable ReporterVersion`.

- [ ] **Step 4: Implement**

`src/main/java/io/turbolytics/turbostats/connect/ReporterVersion.java`:

```java
package io.turbolytics.turbostats.connect;

/** The reporter's own version, sent as instance.reporter_version. */
public final class ReporterVersion {
    private ReporterVersion() {
    }

    /**
     * The jar manifest's Implementation-Version. Outside a jar there is no
     * manifest, and "dev" says so rather than claiming a release.
     */
    public static String get() {
        String v = ReporterVersion.class.getPackage().getImplementationVersion();
        return v == null ? "dev" : v;
    }
}
```

- [ ] **Step 5: Run it, and check the jar's manifest**

Run: `scripts/mvn -q package && unzip -p target/kafka-connect-turbostats-0.1.0-SNAPSHOT.jar META-INF/MANIFEST.MF | grep Implementation-Version`
Expected: tests PASS, and the manifest prints `Implementation-Version: 0.1.0-SNAPSHOT`.

- [ ] **Step 6: Commit**

```bash
git add pom.xml scripts/mvn .github/workflows/ci.yml src
git commit -m "build: a Java 17 jar with no runtime dependencies

The jar installs in a Kafka Connect worker's /kafka/libs, beside Kafka's
own jars, where a bundled library could shadow the worker's copy. Kafka,
Connect and SLF4J are provided. scripts/mvn runs Maven in Docker for
machines without a JDK; CI runs mvn verify without a cache."
```

---

### Task 2: JSON writing

**Files:**
- Create: `src/main/java/io/turbolytics/turbostats/connect/json/JsonObject.java`
- Test: `src/test/java/io/turbolytics/turbostats/connect/json/JsonObjectTest.java`

**Interfaces:**
- Produces: `JsonObject` with `put(String, String)`, `put(String, long)`, `put(String, Long)`, `put(String, boolean)`, `put(String, Instant)`, `put(String, JsonObject)`, `putMap(String, Map<String,String>)`, `toJson(): String`. A `null` value or an empty map writes no key. `JsonObject.formatTime(Instant)` renders whole seconds in UTC, `2026-10-04T10:00:00Z`.

- [ ] **Step 1: Write the failing tests**

```java
package io.turbolytics.turbostats.connect.json;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonObjectTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Absent and zero are different facts in the contract, so null writes
    // no key at all.
    @Test
    void aNullValueWritesNoKey() {
        String nothing = null;
        Long none = null;
        assertEquals("{\"a\":1}", new JsonObject().put("a", 1L).put("b", nothing).put("c", none).toJson());
    }

    @Test
    void anEmptyObjectIsBraces() {
        assertEquals("{}", new JsonObject().toJson());
    }

    // A label value is operator text. Quotes, backslashes, control
    // characters and non-ASCII text must survive a round trip.
    @Test
    void stringsAreEscaped() throws Exception {
        String hostile = "a\"b\\c\nd\te\u0001f é 東京";
        String json = new JsonObject().put("k", hostile).toJson();
        JsonNode parsed = MAPPER.readTree(json);
        assertEquals(hostile, parsed.get("k").asText());
    }

    @Test
    void timesAreWholeSecondsInUtc() {
        Instant t = Instant.parse("2026-10-04T10:00:00.987Z");
        assertEquals("{\"t\":\"2026-10-04T10:00:00Z\"}", new JsonObject().put("t", t).toJson());
    }

    // Labels are written sorted, so the same labels always make the same
    // bytes; an empty map is absent, like an unset field.
    @Test
    void mapsAreSortedAndAnEmptyMapIsAbsent() {
        assertEquals("{\"labels\":{\"a\":\"1\",\"b\":\"2\"}}",
                new JsonObject().putMap("labels", Map.of("b", "2", "a", "1")).toJson());
        assertEquals("{}", new JsonObject().putMap("labels", Map.of()).toJson());
    }

    @Test
    void nestedObjectsAndBooleans() {
        assertEquals("{\"o\":{\"x\":true}}", new JsonObject().put("o", new JsonObject().put("x", true)).toJson());
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `scripts/mvn -q test -Dtest=JsonObjectTest`
Expected: FAIL to compile with `cannot find symbol: class JsonObject`.

- [ ] **Step 3: Implement**

```java
package io.turbolytics.turbostats.connect.json;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.TreeMap;

/**
 * A JSON object written field by field.
 *
 * Hand-written because the jar carries no dependencies: a JSON library in
 * /kafka/libs could shadow the one Kafka ships. The bundle is a fixed set of
 * fields, so writing is all it needs; nothing here parses.
 */
public final class JsonObject {
    private static final DateTimeFormatter TIME = DateTimeFormatter.ISO_INSTANT;

    private final StringBuilder b = new StringBuilder("{");
    private boolean empty = true;

    public JsonObject put(String key, String value) {
        if (value != null) {
            field(key).append(quote(value));
        }
        return this;
    }

    public JsonObject put(String key, long value) {
        field(key).append(value);
        return this;
    }

    public JsonObject put(String key, Long value) {
        if (value != null) {
            field(key).append(value.longValue());
        }
        return this;
    }

    public JsonObject put(String key, boolean value) {
        field(key).append(value);
        return this;
    }

    public JsonObject put(String key, Instant value) {
        if (value != null) {
            field(key).append(quote(formatTime(value)));
        }
        return this;
    }

    public JsonObject put(String key, JsonObject value) {
        if (value != null) {
            field(key).append(value.toJson());
        }
        return this;
    }

    /** Writes a string map as an object, keys sorted; absent when empty. */
    public JsonObject putMap(String key, Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return this;
        }
        JsonObject o = new JsonObject();
        new TreeMap<>(map).forEach(o::put);
        return put(key, o);
    }

    public String toJson() {
        return b + "}";
    }

    /** Whole seconds in UTC, the form Go's encoder writes a truncated time in. */
    public static String formatTime(Instant t) {
        return TIME.format(t.truncatedTo(ChronoUnit.SECONDS));
    }

    private StringBuilder field(String key) {
        if (!empty) {
            b.append(',');
        }
        empty = false;
        return b.append(quote(key)).append(':');
    }

    static String quote(String s) {
        StringBuilder q = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> q.append("\\\"");
                case '\\' -> q.append("\\\\");
                case '\n' -> q.append("\\n");
                case '\r' -> q.append("\\r");
                case '\t' -> q.append("\\t");
                case '\b' -> q.append("\\b");
                case '\f' -> q.append("\\f");
                default -> {
                    if (c < 0x20) {
                        q.append(String.format("\\u%04x", (int) c));
                    } else {
                        q.append(c);
                    }
                }
            }
        }
        return q.append('"').toString();
    }
}
```

- [ ] **Step 4: Run them**

Run: `scripts/mvn -q test -Dtest=JsonObjectTest`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "json: a field-by-field JSON writer, so the jar needs no library

A null value writes no key, because the contract reads absent and zero as
different facts. Strings escape quotes, backslashes and control
characters; a test round-trips hostile label text through a parser."
```

---

### Task 3: The bundle and its schema

**Files:**
- Create: `src/main/java/io/turbolytics/turbostats/connect/wire/Instance.java`, `ProcessInfo.java`, `Memory.java`, `Pipeline.java`, `Exit.java`, `Bundle.java`
- Create: `scripts/sync-schema`
- Create: `src/test/resources/schema/bundle.schema.json` (fetched)
- Test: `src/test/java/io/turbolytics/turbostats/connect/wire/BundleSchemaTest.java`, `src/test/java/io/turbolytics/turbostats/connect/wire/Fixtures.java`

**Interfaces:**
- Consumes: `JsonObject` (Task 2).
- Produces, all in `io.turbolytics.turbostats.connect.wire`:
  - `record Instance(String id, String name, String version, String commit, String arch, String configHash, String sourceType, String sinkType, String runtime, String runtimeVersion, String reporterVersion, Map<String,String> labels)` with `JsonObject toJson()`.
  - `record Memory(String runtime, Long retainedBytes, Long liveBytes, Long heapLimitBytes, long gcCount)` with `toJson()`.
  - `record ProcessInfo(String id, String host, Instant startedAt, Long uptimeSeconds, Long rssBytes, Long memoryLimitBytes, Memory memory)` with `toJson()` and `ProcessInfo withHost(String host)`.
  - `record Pipeline(String state, Instant startedAt, Long restartCount, long messageCount, long handlerRowsRead, long errorCount, Long sinkFlushCount, long sinkRowsAccepted, long sinkRowsWritten, long stateCommitCount, Instant lastMessageAt, Instant lastSinkWriteAt, Instant lastErrorAt)` with `toJson()` and `Pipeline withState(String state)`.
  - `record Exit(String reason, int code)` with `toJson()`.
  - `record Bundle(Instant sentAt, int intervalSeconds, Instant lastActivityAt, Instance instance, ProcessInfo process, Pipeline pipeline, Exit exit)` with `String toJson()`, `Bundle withExit(Exit exit, Instant sentAt)`.
  - Test helper `Fixtures.sourceBundle()`, `Fixtures.sinkBundle()`.

- [ ] **Step 1: Fetch the contract's schema**

`scripts/sync-schema` (make it executable):

```sh
#!/bin/sh
# Copies the TurboStats bundle schema from turbolytics/sql-flow at a ref, so
# the tests validate against the contract and not against a copy someone
# edited. Run it when the contract changes, and commit the result.
set -eu
ref="${1:-main}"
curl -fsSL "https://raw.githubusercontent.com/turbolytics/sql-flow/${ref}/turbostats/wire/schema/bundle.schema.json" \
  -o src/test/resources/schema/bundle.schema.json
echo "synced bundle.schema.json from sql-flow@${ref}"
```

Run: `mkdir -p src/test/resources/schema && scripts/sync-schema spec/kafka-connect-spike-findings`
Expected: `synced bundle.schema.json from sql-flow@spec/kafka-connect-spike-findings`. That branch is PR #435, whose schema makes `sink_flush_count` optional. After #435 merges, re-run with `main`.

Check: `python3 -c "import json;print(json.load(open('src/test/resources/schema/bundle.schema.json'))['properties']['pipeline']['required'])"`
Expected: a list without `sink_flush_count`.

- [ ] **Step 2: Write the failing tests**

`src/test/java/io/turbolytics/turbostats/connect/wire/Fixtures.java`:

```java
package io.turbolytics.turbostats.connect.wire;

import java.time.Instant;
import java.util.Map;

/** Bundles shaped like the ones a worker sends, for tests. */
public final class Fixtures {
    public static final Instant T = Instant.parse("2026-10-04T10:00:00Z");

    private Fixtures() {
    }

    public static ProcessInfo process() {
        return new ProcessInfo(
                "0123456789abcdef0123456789abcdef",
                "10.0.3.7:8083",
                T.minusSeconds(3600),
                3600L,
                912_000_000L,
                2_147_483_648L,
                new Memory("jvm", 780_000_000L, 240_000_000L, 1_073_741_824L, 18_233L));
    }

    public static Instance instance(String sourceType, String sinkType, Map<String, String> labels) {
        return new Instance(
                "prod-connect/inventory-cdc/0",
                "prod-connect/inventory-cdc",
                "3.0.8.Final",
                "",
                "linux/amd64",
                "sha256:00",
                sourceType,
                sinkType,
                "kafka-connect",
                "3.9.0",
                "0.1.0",
                labels);
    }

    public static Bundle sourceBundle() {
        return new Bundle(
                T,
                60,
                T,
                instance("postgres", "kafka", Map.of("region", "eu_west")),
                process(),
                new Pipeline("running", T.minusSeconds(600), 0L, 55_000, 55_000, 0, null,
                        55_000, 55_000, 0, T, T, null),
                null);
    }

    public static Bundle sinkBundle() {
        return new Bundle(
                T,
                60,
                T,
                instance("kafka", "jdbc", null),
                process(),
                new Pipeline("running", T.minusSeconds(600), 2L, 126_624, 126_624, 0, 410L,
                        126_624, 126_350, 0, T, T, null),
                null);
    }
}
```

`src/test/java/io/turbolytics/turbostats/connect/wire/BundleSchemaTest.java`:

```java
package io.turbolytics.turbostats.connect.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class BundleSchemaTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static Set<ValidationMessage> validate(String json) throws Exception {
        try (InputStream in = BundleSchemaTest.class.getResourceAsStream("/schema/bundle.schema.json")) {
            JsonSchema schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(in);
            return schema.validate(MAPPER.readTree(json));
        }
    }

    // Every bundle shape the reporter sends must be one the contract
    // accepts. A Java reporter cannot share the Go types, so the published
    // schema is what holds the two together.
    @Test
    void aSourceTaskBundleValidates() throws Exception {
        assertEquals(Set.of(), validate(Fixtures.sourceBundle().toJson()));
    }

    @Test
    void aSinkTaskBundleValidates() throws Exception {
        assertEquals(Set.of(), validate(Fixtures.sinkBundle().toJson()));
    }

    @Test
    void anExitBundleValidatesAndSaysStopped() throws Exception {
        Bundle exit = Fixtures.sourceBundle().withExit(new Exit("connector_deleted", 0), Fixtures.T.plusSeconds(60));
        assertEquals(Set.of(), validate(exit.toJson()));
        JsonNode doc = MAPPER.readTree(exit.toJson());
        assertEquals("stopped", doc.at("/pipeline/state").asText());
        assertEquals("connector_deleted", doc.at("/exit/reason").asText());
    }

    // Review focus: operator text in a label must not break the document.
    @Test
    void aLabelWithEscapesValidates() throws Exception {
        Bundle b = Fixtures.sourceBundle();
        Bundle hostile = new Bundle(b.sentAt(), b.intervalSeconds(), b.lastActivityAt(),
                Fixtures.instance("postgres", "kafka", Map.of("tenant", "a\"b\\c\n東京")),
                b.process(), b.pipeline(), null);
        assertEquals(Set.of(), validate(hostile.toJson()));
    }

    // A source task has no flush count. The key is absent, not zero.
    @Test
    void aSourceTaskSendsNoFlushCount() throws Exception {
        JsonNode doc = MAPPER.readTree(Fixtures.sourceBundle().toJson());
        assertFalse(doc.get("pipeline").has("sink_flush_count"));
        assertFalse(doc.get("process").has("goroutines"));
        assertEquals("kafka-connect", doc.at("/instance/runtime").asText());
    }

    // The schema is not vacuous: a pipeline without a required counter fails.
    @Test
    void aBundleMissingARequiredCounterIsRejected() throws Exception {
        ObjectNode doc = (ObjectNode) MAPPER.readTree(Fixtures.sourceBundle().toJson());
        ((ObjectNode) doc.get("pipeline")).remove("sink_rows_written");
        assertTrue(validate(doc.toString()).size() > 0);
    }
}
```

- [ ] **Step 3: Run them to verify they fail**

Run: `scripts/mvn -q test -Dtest=BundleSchemaTest`
Expected: FAIL to compile with `cannot find symbol: class Bundle`.

- [ ] **Step 4: Implement the records**

`Instance.java`:

```java
package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;
import java.util.Map;

/**
 * What the build and the operator say this task is. version, commit, arch
 * and config_hash are required by the contract, so they are written even
 * when empty; everything else is absent when null.
 */
public record Instance(
        String id,
        String name,
        String version,
        String commit,
        String arch,
        String configHash,
        String sourceType,
        String sinkType,
        String runtime,
        String runtimeVersion,
        String reporterVersion,
        Map<String, String> labels) {

    public JsonObject toJson() {
        return new JsonObject()
                .put("id", id)
                .put("name", name)
                .put("version", version == null ? "" : version)
                .put("commit", commit == null ? "" : commit)
                .put("arch", arch == null ? "" : arch)
                .put("config_hash", configHash == null ? "" : configHash)
                .put("source_type", sourceType)
                .put("sink_type", sinkType)
                .put("runtime", runtime)
                .put("runtime_version", runtimeVersion)
                .put("reporter_version", reporterVersion)
                .putMap("labels", labels);
    }
}
```

`Memory.java`:

```java
package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;

/** process.memory: a garbage-collected runtime's view of its own memory. */
public record Memory(String runtime, Long retainedBytes, Long liveBytes, Long heapLimitBytes, long gcCount) {

    public JsonObject toJson() {
        return new JsonObject()
                .put("runtime", runtime)
                .put("retained_bytes", retainedBytes)
                .put("live_bytes", liveBytes)
                .put("heap_limit_bytes", heapLimitBytes)
                .put("gc_count", gcCount);
    }
}
```

`ProcessInfo.java`:

```java
package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;
import java.time.Instant;

/**
 * The bundle's process section: the worker JVM. Named ProcessInfo because
 * java.lang.Process is taken. goroutines is never sent: a JVM has none, and
 * the contract made the field omittable for that reason.
 */
public record ProcessInfo(
        String id,
        String host,
        Instant startedAt,
        Long uptimeSeconds,
        Long rssBytes,
        Long memoryLimitBytes,
        Memory memory) {

    public ProcessInfo withHost(String newHost) {
        return new ProcessInfo(id, newHost, startedAt, uptimeSeconds, rssBytes, memoryLimitBytes, memory);
    }

    public JsonObject toJson() {
        return new JsonObject()
                .put("id", id)
                .put("host", host)
                .put("started_at", startedAt)
                .put("uptime_seconds", uptimeSeconds)
                .put("rss_bytes", rssBytes)
                .put("memory_limit_bytes", memoryLimitBytes)
                .put("memory", memory == null ? null : memory.toJson());
    }
}
```

`Pipeline.java`:

```java
package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;
import java.time.Instant;

/**
 * The pipeline section for one task. The six primitive counters are
 * required by v1. sinkFlushCount is null for a source task, which never
 * flushes in batches.
 */
public record Pipeline(
        String state,
        Instant startedAt,
        Long restartCount,
        long messageCount,
        long handlerRowsRead,
        long errorCount,
        Long sinkFlushCount,
        long sinkRowsAccepted,
        long sinkRowsWritten,
        long stateCommitCount,
        Instant lastMessageAt,
        Instant lastSinkWriteAt,
        Instant lastErrorAt) {

    public Pipeline withState(String newState) {
        return new Pipeline(newState, startedAt, restartCount, messageCount, handlerRowsRead, errorCount,
                sinkFlushCount, sinkRowsAccepted, sinkRowsWritten, stateCommitCount, lastMessageAt,
                lastSinkWriteAt, lastErrorAt);
    }

    public JsonObject toJson() {
        return new JsonObject()
                .put("state", state)
                .put("started_at", startedAt)
                .put("restart_count", restartCount)
                .put("message_count", messageCount)
                .put("handler_rows_read", handlerRowsRead)
                .put("error_count", errorCount)
                .put("sink_flush_count", sinkFlushCount)
                .put("sink_rows_accepted", sinkRowsAccepted)
                .put("sink_rows_written", sinkRowsWritten)
                .put("state_commit_count", stateCommitCount)
                .put("last_message_at", lastMessageAt)
                .put("last_sink_write_at", lastSinkWriteAt)
                .put("last_error_at", lastErrorAt);
    }
}
```

`Exit.java`:

```java
package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;

/** How a stopped or deleted connector's task ended. */
public record Exit(String reason, int code) {

    public JsonObject toJson() {
        return new JsonObject().put("reason", reason).put("code", (long) code);
    }
}
```

`Bundle.java`:

```java
package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;
import java.time.Instant;

/** One report about one task. */
public record Bundle(
        Instant sentAt,
        int intervalSeconds,
        Instant lastActivityAt,
        Instance instance,
        ProcessInfo process,
        Pipeline pipeline,
        Exit exit) {

    public static final int VERSION = 1;

    /**
     * The final bundle for a task whose connector was stopped or deleted. It
     * repeats the last counts, says stopped, and carries the exit.
     */
    public Bundle withExit(Exit e, Instant at) {
        return new Bundle(at, intervalSeconds, lastActivityAt, instance, process, pipeline.withState("stopped"), e);
    }

    public String toJson() {
        return new JsonObject()
                .put("v", (long) VERSION)
                .put("sent_at", sentAt)
                .put("interval_seconds", (long) intervalSeconds)
                .put("last_activity_at", lastActivityAt)
                .put("instance", instance.toJson())
                .put("process", process.toJson())
                .put("pipeline", pipeline.toJson())
                .put("exit", exit == null ? null : exit.toJson())
                .toJson();
    }
}
```

- [ ] **Step 5: Run them**

Run: `scripts/mvn -q test -Dtest=BundleSchemaTest`
Expected: PASS, 6 tests.

- [ ] **Step 6: Commit**

```bash
git add scripts/sync-schema src
git commit -m "wire: the bundle a task reports, validated against the contract's schema

The Java reporter cannot share the Go types, so the published JSON Schema
holds the two together. Source, sink and exit bundles validate; a source
task sends no flush count and no goroutines; a bundle missing a required
counter is rejected. scripts/sync-schema copies the schema from sql-flow at
a ref."
```

---

### Task 4: The credential and the signature

**Files:**
- Create: `src/main/java/io/turbolytics/turbostats/connect/sign/Credential.java`
- Create: `src/main/java/io/turbolytics/turbostats/connect/sign/Signer.java`
- Create: `src/test/resources/vectors.json` (copied)
- Test: `src/test/java/io/turbolytics/turbostats/connect/sign/SignerTest.java`

**Interfaces:**
- Produces:
  - `Credential.parse(String credential): Credential`, throwing `IllegalArgumentException` whose message never contains the credential.
  - `Credential.keyId(): String`, `Credential.publicKey(): byte[]` (32 raw bytes), `Credential.sign(byte[] message): byte[]`.
  - `Signer.canonical(String method, String path, long unixSeconds, byte[] body): String`.
  - `Signer.requestPath(String rawPath): String` (empty becomes `/`).
  - `Signer.headers(Credential c, String method, String path, long unixSeconds, byte[] body): Map<String,String>` with the three `X-Turbostats-*` headers.
  - Constants `Signer.MEDIA_TYPE = "application/vnd.turbolytics.turbostats.v1+json"`.

- [ ] **Step 1: Copy the vectors**

Run: `cp ../sql-flow/turbostats/wire/testdata/vectors.json src/test/resources/vectors.json`
Expected: the file holds `seed_hex`, `credential`, `public_key_hex`, `key_id`, `method`, `path`, `timestamp`, `body`, `body_sha256` and `signature_b64`.

- [ ] **Step 2: Write the failing tests**

```java
package io.turbolytics.turbostats.connect.sign;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SignerTest {
    static JsonNode vectors() throws Exception {
        try (InputStream in = SignerTest.class.getResourceAsStream("/vectors.json")) {
            return new ObjectMapper().readTree(in);
        }
    }

    // The Go reference produced these bytes. A signature that differs by
    // one byte is refused by every control plane.
    @Test
    void signsExactlyAsTheGoReference() throws Exception {
        JsonNode v = vectors();
        Credential c = Credential.parse(v.get("credential").asText());
        assertEquals(v.get("public_key_hex").asText(), HexFormat.of().formatHex(c.publicKey()));
        assertEquals(v.get("key_id").asText(), c.keyId());

        byte[] body = v.get("body").asText().getBytes(StandardCharsets.UTF_8);
        String canonical = Signer.canonical(v.get("method").asText(), v.get("path").asText(),
                v.get("timestamp").asLong(), body);
        assertTrue(canonical.endsWith(v.get("body_sha256").asText()));

        Map<String, String> h = Signer.headers(c, v.get("method").asText(), v.get("path").asText(),
                v.get("timestamp").asLong(), body);
        assertEquals(v.get("key_id").asText(), h.get("X-Turbostats-Key-Id"));
        assertEquals(v.get("timestamp").asText(), h.get("X-Turbostats-Timestamp"));
        assertEquals(v.get("signature_b64").asText(), h.get("X-Turbostats-Signature"));
    }

    @Test
    void anEmptyPathSignsAsSlash() {
        assertEquals("/", Signer.requestPath(""));
        assertEquals("/v1/turbostats", Signer.requestPath("/v1/turbostats"));
    }

    // A bad credential names what is wrong and never echoes the secret.
    @Test
    void aBadCredentialNeverEchoesItself() {
        String secret = "sfc_not!base64url!at!all";
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Credential.parse(secret));
        assertFalse(e.getMessage().contains("not!base64url"));

        String publicHalf = "sfp_A6EHv_POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg";
        IllegalArgumentException p = assertThrows(IllegalArgumentException.class, () -> Credential.parse(publicHalf));
        assertTrue(p.getMessage().contains("public"));

        String shortSeed = "sfc_" + Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[16]);
        assertThrows(IllegalArgumentException.class, () -> Credential.parse(shortSeed));
    }
}
```

- [ ] **Step 3: Run them to verify they fail**

Run: `scripts/mvn -q test -Dtest=SignerTest`
Expected: FAIL to compile with `cannot find symbol: class Credential`.

- [ ] **Step 4: Implement**

`Credential.java`:

```java
package io.turbolytics.turbostats.connect.sign;

import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.NamedParameterSpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;

/**
 * The private half of an Ed25519 key, as the sfc_ string an operator holds.
 * The public key and the key id derive from it, as in the Go reference.
 */
public final class Credential {
    public static final String PREFIX = "sfc_";
    private static final String PUBLIC_PREFIX = "sfp_";
    private static final int SEED_SIZE = 32;

    private final PrivateKey privateKey;
    private final byte[] publicKey;
    private final String keyId;

    private Credential(PrivateKey privateKey, byte[] publicKey) {
        this.privateKey = privateKey;
        this.publicKey = publicKey;
        this.keyId = keyIdOf(publicKey);
    }

    /** Messages never contain the input: it is a secret. */
    public static Credential parse(String s) {
        if (s == null) {
            throw new IllegalArgumentException("a credential is required");
        }
        if (s.startsWith(PUBLIC_PREFIX)) {
            throw new IllegalArgumentException("that is the public key; turbostats.key takes the sfc_ credential");
        }
        if (!s.startsWith(PREFIX)) {
            throw new IllegalArgumentException("a credential starts with " + PREFIX);
        }
        byte[] seed;
        try {
            seed = Base64.getUrlDecoder().decode(s.substring(PREFIX.length()));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("a credential's seed is unpadded base64url");
        }
        if (seed.length != SEED_SIZE) {
            throw new IllegalArgumentException("a credential's seed is " + seed.length + " bytes, not 32");
        }
        try {
            KeyPairGenerator g = KeyPairGenerator.getInstance("Ed25519");
            g.initialize(NamedParameterSpec.ED25519, new SeedRandom(seed));
            KeyPair kp = g.generateKeyPair();
            byte[] encoded = kp.getPublic().getEncoded();
            // An Ed25519 X.509 encoding is a fixed 12-byte prefix and the
            // 32-byte raw key.
            byte[] raw = Arrays.copyOfRange(encoded, encoded.length - 32, encoded.length);
            return new Credential(kp.getPrivate(), raw);
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("this JVM cannot make Ed25519 keys: " + e.getClass().getSimpleName());
        }
    }

    public String keyId() {
        return keyId;
    }

    public byte[] publicKey() {
        return publicKey.clone();
    }

    public byte[] sign(byte[] message) {
        try {
            Signature s = Signature.getInstance("Ed25519");
            s.initSign(privateKey);
            s.update(message);
            return s.sign();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("signing failed: " + e.getClass().getSimpleName());
        }
    }

    @Override
    public String toString() {
        // A credential logged by accident shows its key id, never its seed.
        return "Credential(" + keyId + ")";
    }

    /** 16 hex characters of the public key's SHA-256, as the receiver files it. */
    static String keyIdOf(byte[] publicKey) {
        try {
            byte[] sum = MessageDigest.getInstance("SHA-256").digest(publicKey);
            return HexFormat.of().formatHex(sum).substring(0, 16);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Hands the key generator the seed instead of random bytes. The JDK has
     * no API that derives an Ed25519 public key from a seed; the generator
     * draws exactly the 32 private-key bytes from its random source.
     */
    private static final class SeedRandom extends SecureRandom {
        private static final long serialVersionUID = 1L;
        private final byte[] seed;

        SeedRandom(byte[] seed) {
            this.seed = seed.clone();
        }

        @Override
        public void nextBytes(byte[] bytes) {
            System.arraycopy(seed, 0, bytes, 0, Math.min(seed.length, bytes.length));
        }
    }
}
```

`Signer.java`:

```java
package io.turbolytics.turbostats.connect.sign;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/** Signs a request the way turbostats/wire/sign.go does. */
public final class Signer {
    public static final String MEDIA_TYPE = "application/vnd.turbolytics.turbostats.v1+json";
    public static final String HEADER_KEY_ID = "X-Turbostats-Key-Id";
    public static final String HEADER_TIMESTAMP = "X-Turbostats-Timestamp";
    public static final String HEADER_SIGNATURE = "X-Turbostats-Signature";

    private Signer() {
    }

    /** The text a signature covers. The body is hashed, not included. */
    public static String canonical(String method, String path, long unixSeconds, byte[] body) {
        try {
            byte[] sum = MessageDigest.getInstance("SHA-256").digest(body);
            return "v1\n" + method + "\n" + path + "\n" + unixSeconds + "\n" + HexFormat.of().formatHex(sum);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * HTTP sends "/" for a URL written without a path, and the receiver
     * verifies against what it reads, so the empty path signs as "/".
     */
    public static String requestPath(String rawPath) {
        return rawPath == null || rawPath.isEmpty() ? "/" : rawPath;
    }

    public static Map<String, String> headers(Credential c, String method, String path, long unixSeconds, byte[] body) {
        byte[] sig = c.sign(canonical(method, path, unixSeconds, body).getBytes(StandardCharsets.UTF_8));
        Map<String, String> h = new LinkedHashMap<>();
        h.put(HEADER_KEY_ID, c.keyId());
        h.put(HEADER_TIMESTAMP, Long.toString(unixSeconds));
        h.put(HEADER_SIGNATURE, Base64.getEncoder().encodeToString(sig));
        return h;
    }
}
```

- [ ] **Step 5: Run them**

Run: `scripts/mvn -q test -Dtest=SignerTest`
Expected: PASS, 3 tests. If `signsExactlyAsTheGoReference` fails only on the public key, the JDK's generator drew the seed differently; derive the public key by scalar multiplication instead, and keep the vectors as the test.

- [ ] **Step 6: Commit**

```bash
git add src
git commit -m "sign: Ed25519 signatures identical to the Go reference

A control plane refuses a signature that differs by one byte from what
turbostats/wire/sign.go would make. The vectors file from sql-flow is the
test: key id, public key, canonical string and signature all match. A bad
credential's error never echoes the secret."
```

---

### Task 5: Configuration

**Files:**
- Create: `src/main/java/io/turbolytics/turbostats/connect/config/ReporterConfig.java`
- Test: `src/test/java/io/turbolytics/turbostats/connect/config/ReporterConfigTest.java`

**Interfaces:**
- Consumes: `Credential.parse` (Task 4).
- Produces:
  - `record ReporterConfig(URI reportTo, Credential credential, String cluster, int intervalSeconds, int timeoutSeconds, Map<String,String> labels)`.
  - `ReporterConfig.parse(Map<String, ?> workerProps): ReporterConfig.Parsed`, which never throws.
  - `record Parsed(ReporterConfig config, List<String> errors, List<String> warnings)` with `boolean enabled()`, true when `config` is non-null.
  - `ReporterConfig.RESERVED_LABELS: Set<String>`.

- [ ] **Step 1: Write the failing tests**

```java
package io.turbolytics.turbostats.connect.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ReporterConfigTest {
    static final String KEY = "sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

    static Map<String, Object> valid() {
        Map<String, Object> p = new HashMap<>();
        p.put("group.id", "prod-connect");
        p.put("turbostats.report.to", "https://control.turbolytics.io/v1/turbostats");
        p.put("turbostats.key", KEY);
        return p;
    }

    @Test
    void defaultsAreSixtySecondsAndTen() {
        ReporterConfig.Parsed p = ReporterConfig.parse(valid());
        assertTrue(p.enabled(), p.errors().toString());
        assertEquals(60, p.config().intervalSeconds());
        assertEquals(10, p.config().timeoutSeconds());
        assertEquals("prod-connect", p.config().cluster());
    }

    // Without a destination the reporter is off, and that is not an error.
    @Test
    void noReportToIsOffWithoutError() {
        ReporterConfig.Parsed p = ReporterConfig.parse(Map.of("group.id", "prod-connect"));
        assertFalse(p.enabled());
        assertEquals(0, p.errors().size());
    }

    // A signed bundle still crosses the wire in the clear, so plaintext is
    // allowed to the loopback only, as SQLFlow requires.
    @Test
    void plaintextIsRefusedOffTheLoopback() {
        Map<String, Object> p = valid();
        p.put("turbostats.report.to", "http://control.example.com/v1/turbostats");
        assertFalse(ReporterConfig.parse(p).enabled());
        p.put("turbostats.report.to", "http://127.0.0.1:8080/v1/turbostats");
        assertTrue(ReporterConfig.parse(p).enabled());
        p.put("turbostats.report.to", "http://localhost:8080/v1/turbostats");
        assertTrue(ReporterConfig.parse(p).enabled());
    }

    @Test
    void aMissingOrBadKeyIsAnErrorThatNeverEchoesIt() {
        Map<String, Object> p = valid();
        p.remove("turbostats.key");
        assertFalse(ReporterConfig.parse(p).enabled());
        p.put("turbostats.key", "sfc_secret!value");
        ReporterConfig.Parsed parsed = ReporterConfig.parse(p);
        assertFalse(parsed.enabled());
        assertFalse(parsed.errors().toString().contains("secret!value"));
    }

    @Test
    void theTimeoutMustBeShorterThanTheInterval() {
        Map<String, Object> p = valid();
        p.put("turbostats.interval.seconds", "5");
        p.put("turbostats.timeout.seconds", "5");
        assertFalse(ReporterConfig.parse(p).enabled());
        p.put("turbostats.timeout.seconds", "4");
        assertTrue(ReporterConfig.parse(p).enabled());
        p.put("turbostats.interval.seconds", "soon");
        assertFalse(ReporterConfig.parse(p).enabled());
    }

    @Test
    void clusterOverridesGroupIdAndAStockGroupIdWarns() {
        Map<String, Object> p = valid();
        p.put("turbostats.cluster", "eu-connect");
        assertEquals("eu-connect", ReporterConfig.parse(p).config().cluster());

        Map<String, Object> stock = valid();
        stock.put("group.id", "connect-cluster");
        ReporterConfig.Parsed parsed = ReporterConfig.parse(stock);
        assertTrue(parsed.enabled());
        assertEquals(1, parsed.warnings().size());

        Map<String, Object> slash = valid();
        slash.put("turbostats.cluster", "a/b");
        assertFalse(ReporterConfig.parse(slash).enabled());
    }

    @Test
    void labelsFollowSqlFlowsRules() {
        Map<String, Object> p = valid();
        p.put("turbostats.label.region", "eu-west");
        p.put("turbostats.label.env", "prod");
        assertEquals(Map.of("region", "eu-west", "env", "prod"), ReporterConfig.parse(p).config().labels());

        for (String bad : new String[] {"Region", "1region", "k".repeat(33), "version", "runtime"}) {
            Map<String, Object> q = valid();
            q.put("turbostats.label." + bad, "x");
            assertFalse(ReporterConfig.parse(q).enabled(), bad);
        }
        Map<String, Object> longValue = valid();
        longValue.put("turbostats.label.region", "v".repeat(65));
        assertFalse(ReporterConfig.parse(longValue).enabled());

        Map<String, Object> tooMany = valid();
        for (int i = 0; i < 11; i++) {
            tooMany.put("turbostats.label.k" + i, "v");
        }
        assertFalse(ReporterConfig.parse(tooMany).enabled());
    }

    // configure() must never throw, whatever the worker hands it.
    @Test
    void garbageNeverThrows() {
        Map<String, Object> p = new HashMap<>();
        p.put("turbostats.report.to", "::not a url::");
        p.put("turbostats.key", 42);
        p.put("turbostats.interval.seconds", null);
        assertFalse(ReporterConfig.parse(p).enabled());
        assertFalse(ReporterConfig.parse(null).enabled());
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `scripts/mvn -q test -Dtest=ReporterConfigTest`
Expected: FAIL to compile with `cannot find symbol: class ReporterConfig`.

- [ ] **Step 3: Implement**

```java
package io.turbolytics.turbostats.connect.config;

import io.turbolytics.turbostats.connect.sign.Credential;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * The reporter's settings, from the worker's turbostats.* properties.
 *
 * parse never throws. An exception out of the extension's configure stops
 * the worker's REST server, and a monitoring jar must never be the reason a
 * worker is down; a bad setting is an error message and reporting stays off.
 */
public record ReporterConfig(
        URI reportTo,
        Credential credential,
        String cluster,
        int intervalSeconds,
        int timeoutSeconds,
        Map<String, String> labels) {

    public static final String PREFIX = "turbostats.";
    public static final String LABEL_PREFIX = "turbostats.label.";
    public static final int DEFAULT_INTERVAL_SECONDS = 60;
    public static final int DEFAULT_TIMEOUT_SECONDS = 10;
    public static final int MAX_LABELS = 10;
    public static final int MAX_LABEL_KEY = 32;
    public static final int MAX_LABEL_VALUE = 64;
    public static final int MAX_CLUSTER = 64;

    /** The instance field names a label may not shadow, as SQLFlow refuses them. */
    public static final Set<String> RESERVED_LABELS = Set.of("id", "name", "version", "commit", "arch",
            "config_hash", "source_type", "sink_type", "handler_type", "runtime", "runtime_version",
            "reporter_version");

    /** group.id values that installs keep from examples, and so collide across clusters. */
    static final Set<String> STOCK_GROUP_IDS = Set.of("connect-cluster", "connect", "compose-connect-group");

    private static final Pattern LABEL_KEY = Pattern.compile("[a-z][a-z0-9_]*");

    public record Parsed(ReporterConfig config, List<String> errors, List<String> warnings) {
        public boolean enabled() {
            return config != null;
        }
    }

    public static Parsed parse(Map<String, ?> props) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        try {
            return parse(props == null ? Map.of() : props, errors, warnings);
        } catch (RuntimeException e) {
            errors.add("turbostats settings could not be read: " + e.getClass().getSimpleName());
            return new Parsed(null, errors, warnings);
        }
    }

    private static Parsed parse(Map<String, ?> props, List<String> errors, List<String> warnings) {
        String reportTo = string(props, "turbostats.report.to");
        if (reportTo == null || reportTo.isBlank()) {
            return new Parsed(null, List.of(), List.of());
        }
        URI uri = destination(reportTo, errors);

        Credential credential = null;
        String key = string(props, "turbostats.key");
        if (key == null || key.isBlank()) {
            errors.add("turbostats.key is required with turbostats.report.to");
        } else {
            try {
                credential = Credential.parse(key);
            } catch (IllegalArgumentException e) {
                errors.add("turbostats.key: " + e.getMessage());
            }
        }

        String cluster = string(props, "turbostats.cluster");
        if (cluster == null || cluster.isBlank()) {
            cluster = string(props, "group.id");
            if (cluster != null && STOCK_GROUP_IDS.contains(cluster)) {
                warnings.add("turbostats.cluster defaults to group.id \"" + cluster
                        + "\", which many installs share; set turbostats.cluster so instance ids stay unique");
            }
        }
        if (cluster == null || cluster.isBlank()) {
            errors.add("turbostats.cluster is required when the worker has no group.id");
        } else if (cluster.contains("/") || cluster.length() > MAX_CLUSTER) {
            errors.add("turbostats.cluster may not contain / and is at most " + MAX_CLUSTER + " characters");
        }

        int interval = positiveInt(props, "turbostats.interval.seconds", DEFAULT_INTERVAL_SECONDS, errors);
        int timeout = positiveInt(props, "turbostats.timeout.seconds", DEFAULT_TIMEOUT_SECONDS, errors);
        if (interval > 0 && timeout > 0 && timeout >= interval) {
            errors.add("turbostats.timeout.seconds must be less than turbostats.interval.seconds");
        }

        Map<String, String> labels = labels(props, errors);

        if (!errors.isEmpty()) {
            return new Parsed(null, errors, warnings);
        }
        return new Parsed(new ReporterConfig(uri, credential, cluster, interval, timeout, labels), errors, warnings);
    }

    private static URI destination(String raw, List<String> errors) {
        URI uri;
        try {
            uri = URI.create(raw.trim());
        } catch (IllegalArgumentException e) {
            errors.add("turbostats.report.to is not a URL");
            return null;
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (uri.getHost() == null) {
            errors.add("turbostats.report.to has no host");
        } else if (scheme.equals("http")) {
            if (!loopback(uri.getHost())) {
                errors.add("turbostats.report.to is plaintext to a public address; use https");
            }
        } else if (!scheme.equals("https")) {
            errors.add("turbostats.report.to must be http or https");
        }
        return uri;
    }

    /** Literal addresses only: a name other than localhost would need DNS to judge. */
    static boolean loopback(String host) {
        String h = host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
        if (h.equalsIgnoreCase("localhost")) {
            return true;
        }
        if (!h.matches("[0-9.]+") && !h.contains(":")) {
            return false;
        }
        try {
            return InetAddress.getByName(h).isLoopbackAddress();
        } catch (Exception e) {
            return false;
        }
    }

    private static Map<String, String> labels(Map<String, ?> props, List<String> errors) {
        Map<String, String> out = new TreeMap<>();
        for (Map.Entry<String, ?> e : props.entrySet()) {
            if (e.getKey() == null || !e.getKey().startsWith(LABEL_PREFIX)) {
                continue;
            }
            String k = e.getKey().substring(LABEL_PREFIX.length());
            String v = e.getValue() == null ? "" : String.valueOf(e.getValue());
            if (!LABEL_KEY.matcher(k).matches() || k.length() > MAX_LABEL_KEY) {
                errors.add("label key \"" + k + "\" must match [a-z][a-z0-9_]* and be at most " + MAX_LABEL_KEY
                        + " characters");
            } else if (RESERVED_LABELS.contains(k)) {
                errors.add("label key \"" + k + "\" is a field the report already carries");
            } else if (v.length() > MAX_LABEL_VALUE) {
                errors.add("label \"" + k + "\" is longer than " + MAX_LABEL_VALUE + " characters");
            } else {
                out.put(k, v);
            }
        }
        if (out.size() > MAX_LABELS) {
            errors.add("at most " + MAX_LABELS + " labels");
        }
        return Collections.unmodifiableMap(out);
    }

    private static int positiveInt(Map<String, ?> props, String name, int dflt, List<String> errors) {
        String raw = string(props, name);
        if (raw == null || raw.isBlank()) {
            return dflt;
        }
        try {
            int v = Integer.parseInt(raw.trim());
            if (v < 1) {
                errors.add(name + " must be at least 1");
                return -1;
            }
            return v;
        } catch (NumberFormatException e) {
            errors.add(name + " is not a whole number of seconds");
            return -1;
        }
    }

    private static String string(Map<String, ?> props, String name) {
        Object v = props.get(name);
        return v == null ? null : String.valueOf(v);
    }
}
```

- [ ] **Step 4: Run them**

Run: `scripts/mvn -q test -Dtest=ReporterConfigTest`
Expected: PASS, 8 tests.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "config: turbostats.* worker properties, parsed without ever throwing

An exception out of the extension's configure stops the worker's REST
server. parse returns errors instead and reporting stays off. Defaults are
a 60 s interval and a 10 s timeout; plaintext is allowed to the loopback
only; labels follow SQLFlow's rules; a stock group.id warns."
```

---

### Task 6: Identity and derived metadata

**Files:**
- Create: `src/main/java/io/turbolytics/turbostats/connect/collect/TaskKey.java`
- Create: `src/main/java/io/turbolytics/turbostats/connect/collect/Identity.java`
- Test: `src/test/java/io/turbolytics/turbostats/connect/collect/TaskKeyTest.java`, `IdentityTest.java`

**Interfaces:**
- Produces:
  - `record TaskKey(String connector, int task)` with `static Optional<TaskKey> fromClientId(String clientId)`. It accepts `connector-producer-<connector>-<task>` and `connector-consumer-<connector>-<task>` only.
  - `Identity.instanceId(String cluster, TaskKey k)`, `Identity.instanceName(String cluster, String connector)`, `Identity.arch(String osName, String osArch)`, `Identity.typeOf(String connectorClass)`, `Identity.configHash(Map<String,String> config)`.

- [ ] **Step 1: Write the failing tests**

`TaskKeyTest.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class TaskKeyTest {
    // Connect names a task's clients connector-producer-<connector>-<task>;
    // the spike saw connector-producer-inventory-cdc-0.
    @Test
    void readsTheTaskFromAProducerOrConsumerClientId() {
        assertEquals(Optional.of(new TaskKey("inventory-cdc", 0)), TaskKey.fromClientId("connector-producer-inventory-cdc-0"));
        assertEquals(Optional.of(new TaskKey("customers-sink", 12)), TaskKey.fromClientId("connector-consumer-customers-sink-12"));
    }

    // Review focus: the task is after the last hyphen; everything before it
    // is the connector, hyphens and all.
    @Test
    void aConnectorNameWithHyphensKeepsThem() {
        assertEquals(Optional.of(new TaskKey("orders-cdc-v2", 3)), TaskKey.fromClientId("connector-producer-orders-cdc-v2-3"));
    }

    // The worker's own clients and a DLQ producer are not a task's: the DLQ
    // producer's acknowledgments are failures landing, not output.
    @Test
    void otherClientsAreNotTasks() {
        assertTrue(TaskKey.fromClientId("spike-connect-statuses").isEmpty());
        assertTrue(TaskKey.fromClientId("connector-dlq-producer-inventory-cdc-0").isEmpty());
        assertTrue(TaskKey.fromClientId("connector-producer-noTask").isEmpty());
        assertTrue(TaskKey.fromClientId(null).isEmpty());
    }
}
```

`IdentityTest.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IdentityTest {
    @Test
    void idsNameTheClusterConnectorAndTask() {
        assertEquals("prod-connect/inventory-cdc/0", Identity.instanceId("prod-connect", new TaskKey("inventory-cdc", 0)));
        assertEquals("prod-connect/inventory-cdc", Identity.instanceName("prod-connect", "inventory-cdc"));
    }

    // arch is spelled the way Go spells it, so a fleet groups by one value.
    @Test
    void archUsesGoSpelling() {
        assertEquals("linux/amd64", Identity.arch("Linux", "amd64"));
        assertEquals("linux/amd64", Identity.arch("Linux", "x86_64"));
        assertEquals("linux/arm64", Identity.arch("Linux", "aarch64"));
        assertEquals("darwin/arm64", Identity.arch("Mac OS X", "aarch64"));
    }

    // The connector class names the end of the pipeline it talks to.
    @Test
    void typeComesFromTheConnectorClass() {
        assertEquals("postgres", Identity.typeOf("io.debezium.connector.postgresql.PostgresConnector"));
        assertEquals("mysql", Identity.typeOf("io.debezium.connector.mysql.MySqlConnector"));
        assertEquals("jdbc", Identity.typeOf("io.debezium.connector.jdbc.JdbcSinkConnector"));
        assertEquals("s3", Identity.typeOf("io.confluent.connect.s3.S3SinkConnector"));
        assertEquals("mirror", Identity.typeOf("org.apache.kafka.connect.mirror.MirrorSourceConnector"));
        assertNull(Identity.typeOf(null));
    }

    // The hash changes with any setting and never carries one: a connector's
    // config holds its database password in plain text.
    @Test
    void theConfigHashIsStableAndRevealsNothing() {
        Map<String, String> a = new HashMap<>(Map.of("connector.class", "x", "database.password", "hunter2"));
        Map<String, String> b = new HashMap<>(Map.of("database.password", "hunter2", "connector.class", "x"));
        assertEquals(Identity.configHash(a), Identity.configHash(b));
        assertTrue(Identity.configHash(a).startsWith("sha256:"));
        assertTrue(!Identity.configHash(a).contains("hunter2"));
        b.put("database.password", "hunter3");
        assertNotEquals(Identity.configHash(a), Identity.configHash(b));
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `scripts/mvn -q test -Dtest='TaskKeyTest,IdentityTest'`
Expected: FAIL to compile with `cannot find symbol: class TaskKey`.

- [ ] **Step 3: Implement**

`TaskKey.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import java.util.Optional;

/** One connector task, the unit a bundle reports. */
public record TaskKey(String connector, int task) {
    private static final String PRODUCER = "connector-producer-";
    private static final String CONSUMER = "connector-consumer-";

    /**
     * Reads the task from the client.id Connect gives a task's producer or
     * consumer. The task is the part after the last hyphen, because a
     * connector name may contain hyphens and a task number cannot.
     */
    public static Optional<TaskKey> fromClientId(String clientId) {
        if (clientId == null) {
            return Optional.empty();
        }
        String rest;
        if (clientId.startsWith(PRODUCER)) {
            rest = clientId.substring(PRODUCER.length());
        } else if (clientId.startsWith(CONSUMER)) {
            rest = clientId.substring(CONSUMER.length());
        } else {
            return Optional.empty();
        }
        int dash = rest.lastIndexOf('-');
        if (dash <= 0 || dash == rest.length() - 1) {
            return Optional.empty();
        }
        try {
            return Optional.of(new TaskKey(rest.substring(0, dash), Integer.parseInt(rest.substring(dash + 1))));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
```

`Identity.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/** The instance fields the reporter derives rather than reads from settings. */
public final class Identity {
    private Identity() {
    }

    /** One stream of reports per task, unique within an org because the cluster is. */
    public static String instanceId(String cluster, TaskKey k) {
        return cluster + "/" + k.connector() + "/" + k.task();
    }

    /** The logical pipeline: a connector's tasks share it. */
    public static String instanceName(String cluster, String connector) {
        return cluster + "/" + connector;
    }

    /** os/arch in Go's spelling, as SQLFlow sends it, so a fleet groups by one value. */
    public static String arch(String osName, String osArch) {
        String os = osName == null ? "" : osName.toLowerCase(Locale.ROOT);
        if (os.startsWith("mac")) {
            os = "darwin";
        } else if (os.startsWith("windows")) {
            os = "windows";
        }
        String a = osArch == null ? "" : osArch.toLowerCase(Locale.ROOT);
        a = switch (a) {
            case "x86_64", "amd64" -> "amd64";
            case "aarch64", "arm64" -> "arm64";
            default -> a;
        };
        return os + "/" + a;
    }

    /**
     * The connector class's simple name, less its Connector suffix,
     * lowercased: PostgresConnector is postgres, JdbcSinkConnector is jdbc.
     * One rule rather than a table, so a connector nobody listed still gets
     * a readable type.
     */
    public static String typeOf(String connectorClass) {
        if (connectorClass == null || connectorClass.isBlank()) {
            return null;
        }
        String simple = connectorClass.substring(connectorClass.lastIndexOf('.') + 1);
        for (String suffix : new String[] {"SinkConnector", "SourceConnector", "Connector"}) {
            if (simple.endsWith(suffix) && simple.length() > suffix.length()) {
                simple = simple.substring(0, simple.length() - suffix.length());
                break;
            }
        }
        return simple.toLowerCase(Locale.ROOT);
    }

    /**
     * sha256 over the sorted key=value lines. The config carries secrets in
     * plain text, so only the hash leaves the worker; it still changes when
     * any setting does, which is what a receiver needs.
     */
    public static String configHash(Map<String, String> config) {
        StringBuilder b = new StringBuilder();
        new TreeMap<>(config).forEach((k, v) -> b.append(k).append('=').append(v).append('\n'));
        try {
            byte[] sum = MessageDigest.getInstance("SHA-256").digest(b.toString().getBytes(StandardCharsets.UTF_8));
            return "sha256:" + HexFormat.of().formatHex(sum);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
```

- [ ] **Step 4: Run them**

Run: `scripts/mvn -q test -Dtest='TaskKeyTest,IdentityTest'`
Expected: PASS, 7 tests.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "collect: a task's id, name, arch, types and config hash

The task comes from the client.id Connect gives its producer or consumer,
read after the last hyphen so connector names keep theirs; the DLQ
producer is not a task. The config hash is the only trace of a config that
holds database passwords in plain text."
```

---

### Task 7: The interceptors

**Files:**
- Create: `src/main/java/io/turbolytics/turbostats/connect/intercept/TaskCounters.java`
- Create: `src/main/java/io/turbolytics/turbostats/connect/intercept/AckInterceptor.java`
- Create: `src/main/java/io/turbolytics/turbostats/connect/intercept/ConsumeInterceptor.java`
- Test: `src/test/java/io/turbolytics/turbostats/connect/intercept/InterceptorTest.java`

**Interfaces:**
- Consumes: `TaskKey` (Task 6).
- Produces:
  - `TaskCounters.of(TaskKey): TaskCounters`, `TaskCounters.find(TaskKey): Optional<TaskCounters>`, `TaskCounters.snapshot(): TaskCounters.Snapshot`, and test-only `TaskCounters.clear()`.
  - `record Snapshot(long starts, long lastStartMillis, long acked, long lastAckMillis, long lastBatchMillis)`.
  - `AckInterceptor implements ProducerInterceptor<Object,Object>`, `ConsumeInterceptor implements ConsumerInterceptor<Object,Object>`.

- [ ] **Step 1: Write the failing tests**

```java
package io.turbolytics.turbostats.connect.intercept;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.collect.TaskKey;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InterceptorTest {
    static final TaskKey SOURCE = new TaskKey("inventory-cdc", 0);
    static final TaskKey SINK = new TaskKey("customers-sink", 0);

    @BeforeEach
    void reset() {
        TaskCounters.clear();
    }

    static AckInterceptor producerFor(String clientId) {
        AckInterceptor i = new AckInterceptor();
        i.configure(Map.of("client.id", clientId));
        return i;
    }

    // Acknowledged means the broker has the record. A failed send is not a
    // write.
    @Test
    void countsAcknowledgmentsNotFailures() {
        AckInterceptor i = producerFor("connector-producer-inventory-cdc-0");
        i.onAcknowledgement(null, null);
        i.onAcknowledgement(null, null);
        i.onAcknowledgement(null, new RuntimeException("broker gone"));
        TaskCounters.Snapshot s = TaskCounters.find(SOURCE).orElseThrow().snapshot();
        assertEquals(2, s.acked());
        assertTrue(s.lastAckMillis() > 0);
    }

    // Each task start builds a new producer, and configure runs once for it.
    // A restart starts a new epoch: the count restarts as Connect's do.
    @Test
    void eachConfigureIsAStartAndResetsTheEpoch() {
        AckInterceptor first = producerFor("connector-producer-inventory-cdc-0");
        first.onAcknowledgement(null, null);
        AckInterceptor second = producerFor("connector-producer-inventory-cdc-0");
        producerFor("connector-producer-inventory-cdc-0");
        TaskCounters.Snapshot s = TaskCounters.find(SOURCE).orElseThrow().snapshot();
        assertEquals(3, s.starts());
        assertEquals(0, s.acked());
        second.onAcknowledgement(null, null);
        assertEquals(1, TaskCounters.find(SOURCE).orElseThrow().snapshot().acked());
    }

    // The worker's own producers and the DLQ producer pass through untouched.
    @Test
    void otherClientsAreIgnoredWithoutError() {
        AckInterceptor i = producerFor("spike-connect-statuses");
        assertDoesNotThrow(() -> i.onAcknowledgement(null, null));
        assertTrue(TaskCounters.find(SOURCE).isEmpty());
    }

    // An interceptor that throws would fail the task it sits in.
    @Test
    void nothingThrows() {
        AckInterceptor i = new AckInterceptor();
        assertDoesNotThrow(() -> i.configure(null));
        assertDoesNotThrow(() -> i.onAcknowledgement(null, null));
        assertDoesNotThrow(() -> i.onSend(null));
        ConsumeInterceptor c = new ConsumeInterceptor();
        assertDoesNotThrow(() -> c.configure(null));
        assertDoesNotThrow(() -> c.onConsume(null));
    }

    @Test
    void aSinkTaskCountsStartsAndBatchesWithRecords() {
        ConsumeInterceptor c = new ConsumeInterceptor();
        c.configure(Map.of("client.id", "connector-consumer-customers-sink-0"));
        TopicPartition tp = new TopicPartition("t", 0);
        c.onConsume(new ConsumerRecords<>(Map.of()));
        assertEquals(0, TaskCounters.find(SINK).orElseThrow().snapshot().lastBatchMillis());
        c.onConsume(new ConsumerRecords<>(Map.of(tp, List.of(new ConsumerRecord<>("t", 0, 0L, "k", "v")))));
        TaskCounters.Snapshot s = TaskCounters.find(SINK).orElseThrow().snapshot();
        assertEquals(1, s.starts());
        assertTrue(s.lastBatchMillis() > 0);
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `scripts/mvn -q test -Dtest=InterceptorTest`
Expected: FAIL to compile with `cannot find symbol: class AckInterceptor`.

- [ ] **Step 3: Implement**

`TaskCounters.java`:

```java
package io.turbolytics.turbostats.connect.intercept;

import io.turbolytics.turbostats.connect.collect.TaskKey;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What the interceptors saw of one task, shared with the reporter.
 *
 * A static registry works because the jar sits on the worker classpath:
 * the extension and both interceptors load from the application
 * classloader, so they see one class and one map. The spike confirmed the
 * loader for all three.
 */
public final class TaskCounters {
    private static final ConcurrentHashMap<TaskKey, TaskCounters> REGISTRY = new ConcurrentHashMap<>();

    private final AtomicLong starts = new AtomicLong();
    private final AtomicLong acked = new AtomicLong();
    private volatile long lastStartMillis;
    private volatile long lastAckMillis;
    private volatile long lastBatchMillis;

    public record Snapshot(long starts, long lastStartMillis, long acked, long lastAckMillis, long lastBatchMillis) {
    }

    public static TaskCounters of(TaskKey k) {
        return REGISTRY.computeIfAbsent(k, x -> new TaskCounters());
    }

    public static Optional<TaskCounters> find(TaskKey k) {
        return Optional.ofNullable(REGISTRY.get(k));
    }

    /** Tests only: the registry outlives a test otherwise. */
    public static void clear() {
        REGISTRY.clear();
    }

    /**
     * A new producer or consumer means the task started. Its counts restart
     * with it, as Connect's own counters do, so they share one epoch.
     */
    void started(long nowMillis) {
        acked.set(0);
        lastAckMillis = 0;
        lastBatchMillis = 0;
        lastStartMillis = nowMillis;
        starts.incrementAndGet();
    }

    void acked(long nowMillis) {
        acked.incrementAndGet();
        lastAckMillis = nowMillis;
    }

    void batch(long nowMillis) {
        lastBatchMillis = nowMillis;
    }

    public Snapshot snapshot() {
        return new Snapshot(starts.get(), lastStartMillis, acked.get(), lastAckMillis, lastBatchMillis);
    }
}
```

`AckInterceptor.java`:

```java
package io.turbolytics.turbostats.connect.intercept;

import io.turbolytics.turbostats.connect.collect.TaskKey;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Counts each source task's acknowledged records.
 *
 * Connect's source-record-write-total counts when a record is sent, before
 * the broker has it; onAcknowledgement is the only per-record
 * acknowledgment the worker exposes. This runs on the producer's I/O
 * thread for every record: one increment, no allocation, and no exception
 * may escape, because a failing interceptor fails the task.
 */
public final class AckInterceptor implements ProducerInterceptor<Object, Object> {
    private volatile TaskCounters counters;

    @Override
    public void configure(Map<String, ?> configs) {
        try {
            Object id = configs == null ? null : configs.get("client.id");
            TaskKey.fromClientId(id == null ? null : String.valueOf(id)).ifPresent(k -> {
                TaskCounters c = TaskCounters.of(k);
                c.started(System.currentTimeMillis());
                counters = c;
            });
        } catch (Throwable ignored) {
            // Never fail the task over a monitoring counter.
        }
    }

    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        try {
            TaskCounters c = counters;
            if (c != null && exception == null) {
                c.acked(System.currentTimeMillis());
            }
        } catch (Throwable ignored) {
            // Never fail the task over a monitoring counter.
        }
    }

    @Override
    public void close() {
    }
}
```

`ConsumeInterceptor.java`:

```java
package io.turbolytics.turbostats.connect.intercept;

import io.turbolytics.turbostats.connect.collect.TaskKey;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Notes each sink task's start and its last batch with records. Plan B
 * adds the newest record timestamp per batch, for event lag. Runs once per
 * poll on the task's thread; no exception may escape.
 */
public final class ConsumeInterceptor implements ConsumerInterceptor<Object, Object> {
    private volatile TaskCounters counters;

    @Override
    public void configure(Map<String, ?> configs) {
        try {
            Object id = configs == null ? null : configs.get("client.id");
            TaskKey.fromClientId(id == null ? null : String.valueOf(id)).ifPresent(k -> {
                TaskCounters c = TaskCounters.of(k);
                c.started(System.currentTimeMillis());
                counters = c;
            });
        } catch (Throwable ignored) {
            // Never fail the task over a monitoring counter.
        }
    }

    @Override
    public ConsumerRecords<Object, Object> onConsume(ConsumerRecords<Object, Object> records) {
        try {
            TaskCounters c = counters;
            if (c != null && records != null && !records.isEmpty()) {
                c.batch(System.currentTimeMillis());
            }
        } catch (Throwable ignored) {
            // Never fail the task over a monitoring counter.
        }
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void close() {
    }
}
```

- [ ] **Step 4: Run them**

Run: `scripts/mvn -q test -Dtest=InterceptorTest`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "intercept: count each task's starts, acknowledgments and batches

Connect counts a source task's writes on send, before the broker has
them. The producer interceptor counts acknowledgments; each configure is a
task start, the only restart signal that fires once per start (task
metrics re-registered about eight times per restart in the spike). No
exception escapes either interceptor, because one would fail the task."
```

---

### Task 8: The worker process

**Files:**
- Create: `src/main/java/io/turbolytics/turbostats/connect/collect/Cgroup.java`
- Create: `src/main/java/io/turbolytics/turbostats/connect/collect/JvmProcess.java`
- Test: `src/test/java/io/turbolytics/turbostats/connect/collect/CgroupTest.java`, `JvmProcessTest.java`

**Interfaces:**
- Consumes: `ProcessInfo`, `Memory` (Task 3).
- Produces:
  - `Cgroup.memoryLimit(Path root): OptionalLong`.
  - `JvmProcess.read(Path cgroupRoot, Path procStatus): ProcessInfo`, with `host` null; the collector sets it per task.
  - Package-private helpers `JvmProcess.liveBytes(Map<String, Long> afterGcByPool): Long`, `JvmProcess.gcCount(Map<String, Long> countsByCollector): long`, `JvmProcess.rssBytes(Path procStatus): Long`.
  - `JvmProcess.PROCESS_ID`: 32 hex characters, drawn once per JVM.

- [ ] **Step 1: Write the failing tests**

`CgroupTest.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CgroupTest {
    @TempDir
    Path root;

    Path write(String rel, String content) throws Exception {
        Path p = root.resolve(rel);
        Files.createDirectories(p.getParent());
        Files.writeString(p, content);
        return root;
    }

    // The same rules as SQLFlow's reader: no limit is not a limit of zero.
    @Test
    void readsV2AndV1AndTreatsUnlimitedAsAbsent() throws Exception {
        assertEquals(OptionalLong.of(536870912L), Cgroup.memoryLimit(write("memory.max", "536870912\n")));
    }

    @Test
    void v2MaxIsAbsent() throws Exception {
        assertTrue(Cgroup.memoryLimit(write("memory.max", "max\n")).isEmpty());
    }

    @Test
    void v1LimitAndItsUnlimitedSentinel() throws Exception {
        assertEquals(OptionalLong.of(536870912L), Cgroup.memoryLimit(write("memory/memory.limit_in_bytes", "536870912\n")));
    }

    @Test
    void v1SentinelIsAbsent() throws Exception {
        assertTrue(Cgroup.memoryLimit(write("memory/memory.limit_in_bytes", "9223372036854771712\n")).isEmpty());
    }

    @Test
    void noCgroupIsAbsent() {
        assertTrue(Cgroup.memoryLimit(root).isEmpty());
    }
}
```

`JvmProcessTest.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.wire.ProcessInfo;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JvmProcessTest {
    @TempDir
    Path dir;

    // The spike leaked 2 MiB/s under three collectors. ZGC's heap and G1's
    // old generation tracked it after collection; Parallel's old generation
    // stayed flat. A collector not verified sends no live_bytes, because a
    // flat reading during a leak is the worst kind of field.
    @Test
    void liveBytesOnlyUnderVerifiedCollectors() {
        assertEquals(287L << 20, JvmProcess.liveBytes(Map.of("G1 Eden Space", 0L, "G1 Old Gen", 287L << 20)));
        assertEquals(190L << 20, JvmProcess.liveBytes(Map.of("ZHeap", 190L << 20)));
        assertNull(JvmProcess.liveBytes(Map.of("PS Old Gen", 33L << 20, "PS Eden Space", 0L)));
        assertNull(JvmProcess.liveBytes(Map.of("Tenured Gen", 1L)));
        assertNull(JvmProcess.liveBytes(Map.of("ZGC Old Generation", 1L)));
    }

    // ZGC reports its pauses as a second collector; counting them would
    // count each cycle several times.
    @Test
    void gcCountLeavesOutPauseCounters() {
        assertEquals(44, JvmProcess.gcCount(Map.of("ZGC Cycles", 44L, "ZGC Pauses", 132L)));
        assertEquals(41, JvmProcess.gcCount(Map.of("G1 Young Generation", 35L, "G1 Concurrent GC", 6L, "G1 Old Generation", 0L)));
    }

    @Test
    void rssComesFromProcStatus() throws Exception {
        Path status = dir.resolve("status");
        Files.writeString(status, "Name:\tjava\nVmRSS:\t  891234 kB\nThreads:\t80\n");
        assertEquals(891234L * 1024, JvmProcess.rssBytes(status));
        assertNull(JvmProcess.rssBytes(dir.resolve("missing")));
    }

    @Test
    void readsThisJvm() {
        ProcessInfo p = JvmProcess.read(dir, dir.resolve("missing"));
        assertEquals(32, p.id().length());
        assertTrue(p.startedAt() != null);
        assertTrue(p.uptimeSeconds() >= 0);
        assertEquals("jvm", p.memory().runtime());
        assertTrue(p.memory().retainedBytes() > 0);
        assertNull(p.memoryLimitBytes());
        assertNull(p.host());
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `scripts/mvn -q test -Dtest='CgroupTest,JvmProcessTest'`
Expected: FAIL to compile with `cannot find symbol: class Cgroup`.

- [ ] **Step 3: Implement**

`Cgroup.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;

/** The container's memory limit, read the way SQLFlow reads it. */
public final class Cgroup {
    public static final Path ROOT = Path.of("/sys/fs/cgroup");

    private Cgroup() {
    }

    /**
     * Empty when there is no cgroup filesystem, no limit, or a file this does
     * not understand. Read on every report, because an orchestrator can
     * resize a running container.
     */
    public static OptionalLong memoryLimit(Path root) {
        for (Path p : new Path[] {root.resolve("memory.max"), root.resolve("memory").resolve("memory.limit_in_bytes")}) {
            if (Files.isReadable(p)) {
                try {
                    return parse(Files.readString(p));
                } catch (IOException e) {
                    return OptionalLong.empty();
                }
            }
        }
        return OptionalLong.empty();
    }

    /** cgroup v1 spells unlimited as the largest page-aligned long; 2^62 and up is no limit. */
    static OptionalLong parse(String raw) {
        String s = raw.trim();
        if (s.equals("max")) {
            return OptionalLong.empty();
        }
        try {
            long n = Long.parseLong(s);
            return n <= 0 || n >= (1L << 62) ? OptionalLong.empty() : OptionalLong.of(n);
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }
}
```

`JvmProcess.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import io.turbolytics.turbostats.connect.wire.Memory;
import io.turbolytics.turbostats.connect.wire.ProcessInfo;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;

/** The worker JVM, as the bundle's process section. */
public final class JvmProcess {
    /** 16 random bytes per JVM: every bundle this worker sends carries the same id. */
    public static final String PROCESS_ID = newId();

    /**
     * Pools whose after-collection usage tracked a leak in the spike: G1's
     * old generation in steps, non-generational ZGC's heap exactly.
     * Parallel's old generation stayed flat through a 140 MiB leak.
     */
    static final Set<String> LIVE_POOLS = Set.of("G1 Old Gen", "ZHeap");

    private JvmProcess() {
    }

    public static ProcessInfo read(Path cgroupRoot, Path procStatus) {
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        long limit = Cgroup.memoryLimit(cgroupRoot).orElse(-1);
        return new ProcessInfo(
                PROCESS_ID,
                null,
                Instant.ofEpochMilli(rt.getStartTime()),
                rt.getUptime() / 1000,
                rssBytes(procStatus),
                limit > 0 ? limit : null,
                memory());
    }

    static Memory memory() {
        MemoryMXBean m = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = m.getHeapMemoryUsage();
        MemoryUsage nonHeap = m.getNonHeapMemoryUsage();
        Map<String, Long> afterGc = new HashMap<>();
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            MemoryUsage u = pool.getCollectionUsage();
            if (pool.getType() == MemoryType.HEAP && u != null) {
                afterGc.put(pool.getName(), u.getUsed());
            }
        }
        Map<String, Long> counts = new HashMap<>();
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            counts.put(gc.getName(), Math.max(0, gc.getCollectionCount()));
        }
        long max = heap.getMax();
        return new Memory(
                "jvm",
                heap.getCommitted() + nonHeap.getCommitted(),
                liveBytes(afterGc),
                max > 0 ? max : null,
                gcCount(counts));
    }

    /** After-collection usage of the verified pools, or null under any other collector. */
    static Long liveBytes(Map<String, Long> afterGcByPool) {
        Long sum = null;
        for (Map.Entry<String, Long> e : afterGcByPool.entrySet()) {
            if (LIVE_POOLS.contains(e.getKey())) {
                sum = (sum == null ? 0 : sum) + e.getValue();
            }
        }
        return sum;
    }

    /** Collections, without the beans that count pauses within a collection. */
    static long gcCount(Map<String, Long> countsByCollector) {
        long n = 0;
        for (Map.Entry<String, Long> e : countsByCollector.entrySet()) {
            if (!e.getKey().endsWith("Pauses")) {
                n += e.getValue();
            }
        }
        return n;
    }

    /** VmRSS from /proc/self/status; null off Linux or when unreadable. */
    static Long rssBytes(Path procStatus) {
        try {
            for (String line : Files.readAllLines(procStatus)) {
                if (line.startsWith("VmRSS:")) {
                    String[] parts = line.substring(6).trim().split("\\s+");
                    return Long.parseLong(parts[0]) * 1024;
                }
            }
        } catch (IOException | RuntimeException e) {
            return null;
        }
        return null;
    }

    private static String newId() {
        byte[] b = new byte[16];
        new SecureRandom().nextBytes(b);
        return HexFormat.of().formatHex(b);
    }
}
```

- [ ] **Step 4: Run them**

Run: `scripts/mvn -q test -Dtest='CgroupTest,JvmProcessTest'`
Expected: PASS, 9 tests.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "collect: the worker JVM's start, uptime, RSS, container limit and memory

live_bytes is sent only under G1 and non-generational ZGC, the collectors
whose after-collection usage tracked a 2 MiB/s leak in the spike; Parallel
stayed flat through 140 MiB and sends none. gc_count leaves out ZGC's
pause counter. The cgroup reader follows SQLFlow's: unlimited is absent."
```

---

### Task 9: Reading Connect

**Files:**
- Create: `src/main/java/io/turbolytics/turbostats/connect/collect/Jmx.java`, `PlatformJmx.java`, `ConnectMetrics.java`, `ClusterView.java`, `TaskHealth.java`, `ConnectClusterView.java`
- Test: `src/test/java/io/turbolytics/turbostats/connect/collect/ConnectMetricsTest.java`, `FakeJmx.java`

**Interfaces:**
- Consumes: `TaskKey` (Task 6).
- Produces:
  - `interface Jmx { List<ObjectName> query(String pattern); Object attribute(ObjectName name, String attribute); }`, with `attribute` returning null when absent or unreadable.
  - `PlatformJmx implements Jmx` over the platform MBean server.
  - `ConnectMetrics(Jmx)` with `List<TaskKey> localTasks()`, `OptionalLong total(String type, TaskKey k, String attribute)`, and `Optional<String> connectorVersion(String connector)`.
  - `record TaskHealth(String state, String workerId, String connectorType)`, with the type `source`, `sink` or `unknown`.
  - `interface ClusterView { Optional<TaskHealth> task(TaskKey k); Optional<String> connectorState(String connector); Map<String,String> config(String connector); }`.
  - `ConnectClusterView(ConnectClusterState) implements ClusterView`.
  - Test helper `FakeJmx` with `FakeJmx put(String objectName, String attribute, Object value)`.

- [ ] **Step 1: Write the failing tests**

`FakeJmx.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/** A map of MBeans, for tests that need Connect's metrics without a worker. */
public final class FakeJmx implements Jmx {
    private final Map<ObjectName, Map<String, Object>> beans = new HashMap<>();

    public FakeJmx put(String objectName, String attribute, Object value) {
        try {
            beans.computeIfAbsent(new ObjectName(objectName), n -> new HashMap<>()).put(attribute, value);
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException(e);
        }
        return this;
    }

    public FakeJmx remove(String objectName) {
        try {
            beans.remove(new ObjectName(objectName));
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException(e);
        }
        return this;
    }

    @Override
    public List<ObjectName> query(String pattern) {
        List<ObjectName> out = new ArrayList<>();
        try {
            ObjectName p = new ObjectName(pattern);
            for (ObjectName n : beans.keySet()) {
                if (p.apply(n)) {
                    out.add(n);
                }
            }
        } catch (MalformedObjectNameException e) {
            return List.of();
        }
        return out;
    }

    @Override
    public Object attribute(ObjectName name, String attribute) {
        Map<String, Object> attrs = beans.get(name);
        return attrs == null ? null : attrs.get(attribute);
    }
}
```

`ConnectMetricsTest.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class ConnectMetricsTest {
    static final TaskKey K = new TaskKey("inventory-cdc", 0);

    // A task is local when its task metrics are registered in this JVM:
    // Connect registers them only on the worker that runs the task.
    @Test
    void localTasksAreTheRegisteredTaskMetrics() {
        FakeJmx jmx = new FakeJmx()
                .put("kafka.connect:type=connector-task-metrics,connector=inventory-cdc,task=0", "status", "running")
                .put("kafka.connect:type=connector-task-metrics,connector=customers-sink,task=2", "status", "running")
                .put("kafka.connect:type=connect-worker-metrics", "task-count", 2.0);
        assertEquals(List.of(new TaskKey("customers-sink", 2), K),
                new ConnectMetrics(jmx).localTasks().stream().sorted(
                        (a, b) -> a.connector().compareTo(b.connector())).toList());
    }

    // Review focus: Kafka quotes a value with JMX-special characters, and
    // the reporter must read the connector's real name back.
    @Test
    void aQuotedConnectorNameIsUnquoted() {
        FakeJmx jmx = new FakeJmx()
                .put("kafka.connect:type=connector-task-metrics,connector=\"a:b,c\",task=0", "status", "running")
                .put("kafka.connect:type=source-task-metrics,connector=\"a:b,c\",task=0", "source-record-poll-total", 7.0);
        ConnectMetrics m = new ConnectMetrics(jmx);
        TaskKey k = m.localTasks().get(0);
        assertEquals("a:b,c", k.connector());
        assertEquals(OptionalLong.of(7), m.total("source-task-metrics", k, "source-record-poll-total"));
    }

    // Kafka keeps totals as doubles. Absent and NaN are not zero.
    @Test
    void totalsAreLongsAndMissingIsEmpty() {
        FakeJmx jmx = new FakeJmx()
                .put("kafka.connect:type=source-task-metrics,connector=inventory-cdc,task=0", "source-record-poll-total", 55000.0)
                .put("kafka.connect:type=source-task-metrics,connector=inventory-cdc,task=0", "source-record-active-count", Double.NaN);
        ConnectMetrics m = new ConnectMetrics(jmx);
        assertEquals(OptionalLong.of(55000), m.total("source-task-metrics", K, "source-record-poll-total"));
        assertTrue(m.total("source-task-metrics", K, "source-record-active-count").isEmpty());
        assertTrue(m.total("source-task-metrics", K, "source-record-write-total").isEmpty());
        assertTrue(m.total("sink-task-metrics", K, "sink-record-read-total").isEmpty());
    }

    @Test
    void theConnectorVersionComesFromConnectorMetrics() {
        FakeJmx jmx = new FakeJmx()
                .put("kafka.connect:type=connector-metrics,connector=inventory-cdc", "connector-version", "3.0.8.Final");
        assertEquals(Optional.of("3.0.8.Final"), new ConnectMetrics(jmx).connectorVersion("inventory-cdc"));
        assertTrue(new ConnectMetrics(jmx).connectorVersion("other").isEmpty());
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `scripts/mvn -q test -Dtest=ConnectMetricsTest`
Expected: FAIL to compile with `cannot find symbol: class Jmx`.

- [ ] **Step 3: Implement**

`Jmx.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import java.util.List;
import javax.management.ObjectName;

/** The MBeans the reporter reads, behind an interface so tests need no worker. */
public interface Jmx {
    List<ObjectName> query(String pattern);

    /** null when the MBean or the attribute is absent or unreadable. */
    Object attribute(ObjectName name, String attribute);
}
```

`PlatformJmx.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/** The worker's platform MBean server, where Connect and Debezium register. */
public final class PlatformJmx implements Jmx {
    private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

    @Override
    public List<ObjectName> query(String pattern) {
        try {
            return new ArrayList<>(server.queryNames(new ObjectName(pattern), null));
        } catch (Exception e) {
            return List.of();
        }
    }

    @Override
    public Object attribute(ObjectName name, String attribute) {
        try {
            return server.getAttribute(name, attribute);
        } catch (Exception e) {
            return null;
        }
    }
}
```

`ConnectMetrics.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import javax.management.ObjectName;

/** Connect's own metrics for the tasks this worker runs. */
public final class ConnectMetrics {
    private static final String DOMAIN = "kafka.connect";
    private final Jmx jmx;

    public ConnectMetrics(Jmx jmx) {
        this.jmx = jmx;
    }

    /** Connect registers a task's metrics only on the worker that runs it. */
    public List<TaskKey> localTasks() {
        List<TaskKey> out = new ArrayList<>();
        for (ObjectName n : jmx.query(DOMAIN + ":type=connector-task-metrics,*")) {
            String connector = unquote(n.getKeyProperty("connector"));
            String task = unquote(n.getKeyProperty("task"));
            if (connector == null || task == null) {
                continue;
            }
            try {
                out.add(new TaskKey(connector, Integer.parseInt(task)));
            } catch (NumberFormatException ignored) {
                // Not a task metrics name this reporter understands.
            }
        }
        return out;
    }

    /** A cumulative metric as a long; empty when absent or NaN, never a false zero. */
    public OptionalLong total(String type, TaskKey k, String attribute) {
        ObjectName n = find(type, k.connector(), Integer.toString(k.task()));
        if (n == null) {
            return OptionalLong.empty();
        }
        Object v = jmx.attribute(n, attribute);
        if (!(v instanceof Number num) || Double.isNaN(num.doubleValue())) {
            return OptionalLong.empty();
        }
        return OptionalLong.of((long) num.doubleValue());
    }

    /** Registered on the worker that runs the connector itself, which may be another. */
    public Optional<String> connectorVersion(String connector) {
        ObjectName n = find("connector-metrics", connector, null);
        Object v = n == null ? null : jmx.attribute(n, "connector-version");
        return v == null ? Optional.empty() : Optional.of(String.valueOf(v));
    }

    /**
     * Matches on unquoted key values rather than building a name, because
     * Kafka quotes a value that holds a JMX-special character.
     */
    private ObjectName find(String type, String connector, String task) {
        for (ObjectName n : jmx.query(DOMAIN + ":type=" + type + ",*")) {
            if (connector.equals(unquote(n.getKeyProperty("connector")))
                    && (task == null || task.equals(unquote(n.getKeyProperty("task"))))) {
                return n;
            }
        }
        return null;
    }

    static String unquote(String v) {
        if (v != null && v.length() >= 2 && v.startsWith("\"") && v.endsWith("\"")) {
            try {
                return ObjectName.unquote(v);
            } catch (IllegalArgumentException e) {
                return v;
            }
        }
        return v;
    }
}
```

`TaskHealth.java`:

```java
package io.turbolytics.turbostats.connect.collect;

/** What the cluster says about one task: its state, its worker, and its connector's type. */
public record TaskHealth(String state, String workerId, String connectorType) {
}
```

`ClusterView.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import java.util.Map;
import java.util.Optional;

/** The cluster's view of connectors and tasks, behind an interface so tests need no worker. */
public interface ClusterView {
    Optional<TaskHealth> task(TaskKey k);

    /** The connector's state, such as RUNNING or STOPPED; empty when it was deleted. */
    Optional<String> connectorState(String connector);

    /** The connector's config, empty when unknown. Holds secrets: hash it, never send it. */
    Map<String, String> config(String connector);
}
```

`ConnectClusterView.java`:

```java
package io.turbolytics.turbostats.connect.collect;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.health.ConnectorHealth;
import org.apache.kafka.connect.health.TaskState;

/**
 * Reads ConnectClusterState, which a REST extension receives. Every call
 * swallows failures: an unknown connector throws, and a race with a
 * rebalance can too.
 */
public final class ConnectClusterView implements ClusterView {
    private final ConnectClusterState state;

    public ConnectClusterView(ConnectClusterState state) {
        this.state = state;
    }

    @Override
    public Optional<TaskHealth> task(TaskKey k) {
        try {
            ConnectorHealth h = state.connectorHealth(k.connector());
            TaskState t = h.tasksState().get(k.task());
            if (t == null) {
                return Optional.empty();
            }
            String type = h.type() == null ? "unknown" : h.type().toString().toLowerCase(Locale.ROOT);
            return Optional.of(new TaskHealth(t.state(), t.workerId(), type));
        } catch (Throwable e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<String> connectorState(String connector) {
        try {
            return Optional.of(state.connectorHealth(connector).connectorState().state());
        } catch (Throwable e) {
            return Optional.empty();
        }
    }

    @Override
    public Map<String, String> config(String connector) {
        try {
            Map<String, String> c = state.connectorConfig(connector);
            return c == null ? Map.of() : c;
        } catch (Throwable e) {
            return Map.of();
        }
    }
}
```

- [ ] **Step 4: Run them**

Run: `scripts/mvn -q test -Dtest=ConnectMetricsTest`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "collect: Connect's metrics and cluster state for this worker's tasks

A task is local when its task metrics are registered in this JVM. Totals
read as longs; absent or NaN is empty, never zero. Names match on unquoted
key values, because Kafka quotes a connector name that holds a JMX-special
character. Cluster calls swallow failures, as a rebalance can race them."
```

---

### Task 10: One bundle per local task

**Files:**
- Create: `src/main/java/io/turbolytics/turbostats/connect/collect/TaskCollector.java`
- Test: `src/test/java/io/turbolytics/turbostats/connect/collect/TaskCollectorTest.java`

**Interfaces:**
- Consumes: `ReporterConfig` (Task 5), `TaskKey`, `Identity` (Task 6), `TaskCounters` (Task 7), `ProcessInfo` (Task 3), `ConnectMetrics`, `ClusterView`, `TaskHealth` (Task 9).
- Produces: `TaskCollector(ReporterConfig config, ConnectMetrics metrics, ClusterView cluster, Supplier<ProcessInfo> process, String runtimeVersion, Consumer<String> warn)` with `List<Bundle> collect(Instant now)`. It returns one bundle per reportable local task, plus one exit bundle per task whose connector was stopped or deleted since the last call.

- [ ] **Step 1: Write the failing tests**

```java
package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.config.ReporterConfig;
import io.turbolytics.turbostats.connect.intercept.AckInterceptor;
import io.turbolytics.turbostats.connect.intercept.TaskCounters;
import io.turbolytics.turbostats.connect.wire.Bundle;
import io.turbolytics.turbostats.connect.wire.Fixtures;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TaskCollectorTest {
    static final TaskKey SRC = new TaskKey("inventory-cdc", 0);
    static final TaskKey SNK = new TaskKey("customers-sink", 0);
    static final Instant NOW = Instant.parse("2026-10-04T10:00:00Z");
    static final String SRC_TASK = "kafka.connect:type=connector-task-metrics,connector=inventory-cdc,task=0";
    static final String SRC_METRICS = "kafka.connect:type=source-task-metrics,connector=inventory-cdc,task=0";
    static final String SNK_TASK = "kafka.connect:type=connector-task-metrics,connector=customers-sink,task=0";
    static final String SNK_METRICS = "kafka.connect:type=sink-task-metrics,connector=customers-sink,task=0";

    static class Cluster implements ClusterView {
        final Map<TaskKey, TaskHealth> tasks = new HashMap<>();
        final Map<String, String> connectorStates = new HashMap<>();

        @Override
        public Optional<TaskHealth> task(TaskKey k) {
            return Optional.ofNullable(tasks.get(k));
        }

        @Override
        public Optional<String> connectorState(String connector) {
            return Optional.ofNullable(connectorStates.get(connector));
        }

        @Override
        public Map<String, String> config(String connector) {
            return Map.of("connector.class", connector.equals("customers-sink")
                    ? "io.debezium.connector.jdbc.JdbcSinkConnector"
                    : "io.debezium.connector.postgresql.PostgresConnector");
        }
    }

    FakeJmx jmx;
    Cluster cluster;
    List<String> warnings;

    @BeforeEach
    void setUp() {
        TaskCounters.clear();
        jmx = new FakeJmx();
        cluster = new Cluster();
        warnings = new ArrayList<>();
    }

    TaskCollector collector() {
        Map<String, Object> props = new HashMap<>();
        props.put("group.id", "prod-connect");
        props.put("turbostats.report.to", "https://control.turbolytics.io/v1/turbostats");
        props.put("turbostats.key", "sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8");
        ReporterConfig cfg = ReporterConfig.parse(props).config();
        return new TaskCollector(cfg, new ConnectMetrics(jmx), cluster, () -> Fixtures.process().withHost(null),
                "3.9.0", warnings::add);
    }

    void runningSource(long poll, long write, long active) {
        jmx.put(SRC_TASK, "status", "running")
                .put(SRC_METRICS, "source-record-poll-total", (double) poll)
                .put(SRC_METRICS, "source-record-write-total", (double) write)
                .put(SRC_METRICS, "source-record-active-count", (double) active);
        cluster.tasks.put(SRC, new TaskHealth("RUNNING", "10.0.3.7:8083", "source"));
        cluster.connectorStates.put("inventory-cdc", "RUNNING");
    }

    // Review focus: a worker with no tasks sends nothing.
    @Test
    void aWorkerWithNoTasksSendsNothing() {
        assertEquals(List.of(), collector().collect(NOW));
    }

    // Review focus: a starting or moving task has no metrics yet. Zeros
    // would claim it has done nothing; it is skipped until it has some.
    @Test
    void aTaskWithoutMetricsIsSkipped() {
        jmx.put(SRC_TASK, "status", "running");
        cluster.tasks.put(SRC, new TaskHealth("RUNNING", "10.0.3.7:8083", "source"));
        assertEquals(List.of(), collector().collect(NOW));
    }

    @Test
    void aSourceTaskReportsIdentityStateAndAcknowledgedWrites() {
        new AckInterceptor().configure(Map.of("client.id", "connector-producer-inventory-cdc-0"));
        AckInterceptor ack = new AckInterceptor();
        ack.configure(Map.of("client.id", "connector-producer-inventory-cdc-0"));
        for (int i = 0; i < 40; i++) {
            ack.onAcknowledgement(null, null);
        }
        runningSource(50, 50, 10);

        Bundle b = collector().collect(NOW).get(0);
        assertEquals("prod-connect/inventory-cdc/0", b.instance().id());
        assertEquals("prod-connect/inventory-cdc", b.instance().name());
        assertEquals("postgres", b.instance().sourceType());
        assertEquals("kafka", b.instance().sinkType());
        assertEquals("kafka-connect", b.instance().runtime());
        assertEquals("10.0.3.7:8083", b.process().host());
        assertEquals("running", b.pipeline().state());
        assertEquals(50, b.pipeline().messageCount());
        assertEquals(50, b.pipeline().sinkRowsAccepted());
        assertEquals(40, b.pipeline().sinkRowsWritten());
        assertNull(b.pipeline().sinkFlushCount());
        assertEquals(0, b.pipeline().stateCommitCount());
        assertEquals(1L, b.pipeline().restartCount());
        assertTrue(b.pipeline().lastSinkWriteAt() != null);
    }

    // Without the interceptor's counts, written is records written less
    // records in flight, and there is no acknowledgment time to report.
    @Test
    void withoutTheInterceptorWrittenFallsBackToConnectsCounts() {
        runningSource(50, 50, 10);
        Bundle b = collector().collect(NOW).get(0);
        assertEquals(40, b.pipeline().sinkRowsWritten());
        assertNull(b.pipeline().lastSinkWriteAt());
        assertNull(b.pipeline().restartCount());
        assertNull(b.pipeline().startedAt());
    }

    // The spike once saw acknowledgments stop while writes rose. Writes
    // rising, acknowledgments flat and nothing in flight means the counter
    // is broken: fall back and say so once.
    @Test
    void aStalledAcknowledgmentCountFallsBackAndWarnsOnce() {
        AckInterceptor ack = new AckInterceptor();
        ack.configure(Map.of("client.id", "connector-producer-inventory-cdc-0"));
        ack.onAcknowledgement(null, null);
        TaskCollector c = collector();
        runningSource(10, 10, 9);
        c.collect(NOW);
        runningSource(30, 30, 0);
        Bundle b = c.collect(NOW.plusSeconds(60)).get(0);
        assertEquals(30, b.pipeline().sinkRowsWritten());
        assertNull(b.pipeline().lastSinkWriteAt());
        // Still broken while acknowledgments stay flat: no fall back to the
        // stale count of 1, and no second warning.
        assertEquals(30, c.collect(NOW.plusSeconds(120)).get(0).pipeline().sinkRowsWritten());
        assertEquals(1, warnings.size());
    }

    @Test
    void aSinkTaskCountsWrittenAsReadLessInFlightAndFlushesAsCommits() {
        jmx.put(SNK_TASK, "status", "running")
                .put(SNK_METRICS, "sink-record-read-total", 1000.0)
                .put(SNK_METRICS, "sink-record-send-total", 1000.0)
                .put(SNK_METRICS, "sink-record-active-count", 274.0)
                .put(SNK_METRICS, "offset-commit-completion-total", 12.0);
        cluster.tasks.put(SNK, new TaskHealth("RUNNING", "10.0.3.8:8083", "sink"));
        cluster.connectorStates.put("customers-sink", "RUNNING");

        Bundle b = collector().collect(NOW).get(0);
        assertEquals("kafka", b.instance().sourceType());
        assertEquals("jdbc", b.instance().sinkType());
        assertEquals(1000, b.pipeline().messageCount());
        assertEquals(726, b.pipeline().sinkRowsWritten());
        assertEquals(12L, b.pipeline().sinkFlushCount());
    }

    // A failed task on a live worker is the failure this reporter exists for.
    @Test
    void connectStatesMapToTheContractsStates() {
        runningSource(1, 1, 0);
        for (String[] pair : new String[][] {{"RUNNING", "running"}, {"PAUSED", "paused"},
                {"FAILED", "failed"}, {"RESTARTING", "starting"}}) {
            cluster.tasks.put(SRC, new TaskHealth(pair[0], "10.0.3.7:8083", "source"));
            assertEquals(pair[1], collector().collect(NOW).get(0).pipeline().state());
        }
        cluster.tasks.put(SRC, new TaskHealth("UNASSIGNED", "10.0.3.7:8083", "source"));
        assertEquals(List.of(), collector().collect(NOW));
    }

    // A deleted connector's tasks vanish; its last bundle goes out once
    // more, stopped, with an exit. A task that moved sends nothing.
    @Test
    void aDeletedConnectorSendsAnExitAndAMovedTaskSendsNothing() {
        runningSource(5, 5, 0);
        TaskCollector c = collector();
        c.collect(NOW);

        jmx.remove(SRC_TASK).remove(SRC_METRICS);
        cluster.tasks.remove(SRC);
        cluster.connectorStates.remove("inventory-cdc");
        List<Bundle> out = c.collect(NOW.plusSeconds(60));
        assertEquals(1, out.size());
        assertEquals("connector_deleted", out.get(0).exit().reason());
        assertEquals("stopped", out.get(0).pipeline().state());
        assertEquals(List.of(), c.collect(NOW.plusSeconds(120)));

        runningSource(5, 5, 0);
        TaskCollector moved = collector();
        moved.collect(NOW);
        jmx.remove(SRC_TASK).remove(SRC_METRICS);
        assertEquals(List.of(), moved.collect(NOW.plusSeconds(60)));
    }

    @Test
    void aStoppedConnectorSendsAnExit() {
        runningSource(5, 5, 0);
        TaskCollector c = collector();
        c.collect(NOW);
        jmx.remove(SRC_TASK).remove(SRC_METRICS);
        cluster.connectorStates.put("inventory-cdc", "STOPPED");
        assertEquals("connector_stopped", c.collect(NOW.plusSeconds(60)).get(0).exit().reason());
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `scripts/mvn -q test -Dtest=TaskCollectorTest`
Expected: FAIL to compile with `cannot find symbol: class TaskCollector`.

- [ ] **Step 3: Implement**

```java
package io.turbolytics.turbostats.connect.collect;

import io.turbolytics.turbostats.connect.ReporterVersion;
import io.turbolytics.turbostats.connect.config.ReporterConfig;
import io.turbolytics.turbostats.connect.intercept.TaskCounters;
import io.turbolytics.turbostats.connect.wire.Bundle;
import io.turbolytics.turbostats.connect.wire.Exit;
import io.turbolytics.turbostats.connect.wire.Instance;
import io.turbolytics.turbostats.connect.wire.Pipeline;
import io.turbolytics.turbostats.connect.wire.ProcessInfo;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Builds one bundle per task this worker runs, and the exit bundle for a
 * task whose connector was stopped or deleted. Called from the reporter's
 * one thread only, so its memory of the last call needs no locking.
 */
public final class TaskCollector {
    private static final String SOURCE = "source-task-metrics";
    private static final String SINK = "sink-task-metrics";
    private static final String ERRORS = "task-error-metrics";

    private final ReporterConfig config;
    private final ConnectMetrics metrics;
    private final ClusterView cluster;
    private final Supplier<ProcessInfo> process;
    private final String runtimeVersion;
    private final Consumer<String> warn;

    /** The last bundle per task, for exits, and what the last call saw, for change times. */
    private final Map<TaskKey, Bundle> last = new HashMap<>();
    private final Map<TaskKey, Seen> seen = new HashMap<>();
    private final Set<TaskKey> warnedBrokenAcks = new HashSet<>();

    private record Seen(long input, Instant inputChangedAt, long output, Instant outputChangedAt,
            long written, long acked) {
    }

    public TaskCollector(ReporterConfig config, ConnectMetrics metrics, ClusterView cluster,
            Supplier<ProcessInfo> process, String runtimeVersion, Consumer<String> warn) {
        this.config = config;
        this.metrics = metrics;
        this.cluster = cluster;
        this.process = process;
        this.runtimeVersion = runtimeVersion;
        this.warn = warn;
    }

    public List<Bundle> collect(Instant now) {
        List<Bundle> out = new ArrayList<>();
        Set<TaskKey> local = new HashSet<>(metrics.localTasks());
        ProcessInfo jvm = local.isEmpty() ? null : process.get();
        for (TaskKey k : local) {
            Bundle b = bundle(k, jvm, now);
            if (b != null) {
                out.add(b);
                last.put(k, b);
            }
        }
        for (TaskKey gone : new ArrayList<>(last.keySet())) {
            if (local.contains(gone)) {
                continue;
            }
            Bundle prior = last.remove(gone);
            seen.remove(gone);
            exitFor(gone).ifPresent(e -> out.add(prior.withExit(e, now)));
        }
        return out;
    }

    /**
     * A connector deleted or stopped ends its tasks; a task that moved, or
     * whose worker is rebalancing, reports from its new worker instead and
     * sends nothing from here.
     */
    private Optional<Exit> exitFor(TaskKey k) {
        Optional<String> state = cluster.connectorState(k.connector());
        if (state.isEmpty()) {
            return Optional.of(new Exit("connector_deleted", 0));
        }
        if ("STOPPED".equals(state.get())) {
            return Optional.of(new Exit("connector_stopped", 0));
        }
        return Optional.empty();
    }

    private Bundle bundle(TaskKey k, ProcessInfo jvm, Instant now) {
        Optional<TaskHealth> health = cluster.task(k);
        if (health.isEmpty() || "UNASSIGNED".equals(health.get().state())) {
            return null;
        }
        TaskHealth h = health.get();
        boolean sink = "sink".equals(h.connectorType());
        String type = sink ? SINK : SOURCE;

        OptionalLong input = metrics.total(type, k, sink ? "sink-record-read-total" : "source-record-poll-total");
        OptionalLong accepted = metrics.total(type, k, sink ? "sink-record-send-total" : "source-record-write-total");
        if (input.isEmpty() || accepted.isEmpty()) {
            // Starting or moving: no counts yet, and zeros would claim some.
            return null;
        }
        long active = metrics.total(type, k, sink ? "sink-record-active-count" : "source-record-active-count").orElse(0);
        Optional<TaskCounters.Snapshot> counters = TaskCounters.find(k).map(TaskCounters::snapshot)
                .filter(s -> s.starts() > 0);

        Seen prev = seen.get(k);
        Instant inputChangedAt = changedAt(prev == null ? null : prev.input(), prev == null ? null : prev.inputChangedAt(),
                input.getAsLong(), now);

        long written;
        Instant lastWrite;
        Long flushes = null;
        Instant flushChangedAt = null;
        long flushTotal = 0;
        if (sink) {
            written = Math.max(0, input.getAsLong() - active);
            OptionalLong commits = metrics.total(SINK, k, "offset-commit-completion-total");
            if (commits.isPresent()) {
                flushTotal = commits.getAsLong();
                flushes = flushTotal;
                flushChangedAt = changedAt(prev == null ? null : prev.output(),
                        prev == null ? null : prev.outputChangedAt(), flushTotal, now);
            }
            lastWrite = flushChangedAt;
        } else {
            long fallback = Math.max(0, accepted.getAsLong() - active);
            // Broken once writes rose with nothing in flight and no new
            // acknowledgment, and still broken until acknowledgments move:
            // reverting to a stale count would report writes going backwards.
            boolean acksFlat = counters.isPresent() && prev != null && counters.get().acked() == prev.acked();
            if (!acksFlat) {
                warnedBrokenAcks.remove(k);
            }
            boolean broken = acksFlat
                    && ((accepted.getAsLong() > prev.written() && active == 0) || warnedBrokenAcks.contains(k));
            if (counters.isPresent() && !broken) {
                written = counters.get().acked();
                lastWrite = counters.get().lastAckMillis() > 0 ? Instant.ofEpochMilli(counters.get().lastAckMillis()) : null;
            } else {
                if (broken && warnedBrokenAcks.add(k)) {
                    warn.accept("turbostats: acknowledgments for " + k.connector() + "/" + k.task()
                            + " stopped while writes continued; reporting Connect's written count instead");
                }
                written = fallback;
                lastWrite = null;
            }
        }
        seen.put(k, new Seen(input.getAsLong(), inputChangedAt, flushTotal, flushChangedAt,
                accepted.getAsLong(), counters.map(TaskCounters.Snapshot::acked).orElse(0L)));

        Instant lastMessage = inputChangedAt;
        if (sink && counters.isPresent() && counters.get().lastBatchMillis() > 0) {
            lastMessage = Instant.ofEpochMilli(counters.get().lastBatchMillis());
        }

        long errors = metrics.total(ERRORS, k, "total-record-errors").orElse(0);
        OptionalLong lastErrorMillis = metrics.total(ERRORS, k, "last-error-timestamp");
        Instant lastError = lastErrorMillis.isPresent() && lastErrorMillis.getAsLong() > 0
                ? Instant.ofEpochMilli(lastErrorMillis.getAsLong())
                : null;

        Map<String, String> connectorConfig = cluster.config(k.connector());
        String typeOfConnector = Identity.typeOf(connectorConfig.get("connector.class"));
        Instance instance = new Instance(
                Identity.instanceId(config.cluster(), k),
                Identity.instanceName(config.cluster(), k.connector()),
                metrics.connectorVersion(k.connector()).orElse(""),
                "",
                Identity.arch(System.getProperty("os.name"), System.getProperty("os.arch")),
                Identity.configHash(connectorConfig),
                sink ? "kafka" : typeOfConnector,
                sink ? typeOfConnector : "kafka",
                "kafka-connect",
                runtimeVersion,
                ReporterVersion.get(),
                config.labels());

        Pipeline pipeline = new Pipeline(
                state(h.state()),
                counters.map(s -> Instant.ofEpochMilli(s.lastStartMillis())).orElse(null),
                counters.map(s -> s.starts() - 1).orElse(null),
                input.getAsLong(),
                input.getAsLong(),
                errors,
                flushes,
                accepted.getAsLong(),
                written,
                0,
                lastMessage,
                lastWrite,
                lastError);

        return new Bundle(now, config.intervalSeconds(), lastMessage, instance, jvm.withHost(h.workerId()), pipeline,
                null);
    }

    /** When a counter last rose, as this reporter saw it: accurate to one interval. */
    private static Instant changedAt(Long before, Instant beforeAt, long current, Instant now) {
        if (before == null) {
            return null;
        }
        return current > before ? now : beforeAt;
    }

    /** A value this map does not know reads as unknown, never as running. */
    static String state(String connectState) {
        if (connectState == null) {
            return "unknown";
        }
        return switch (connectState) {
            case "RUNNING" -> "running";
            case "PAUSED" -> "paused";
            case "FAILED" -> "failed";
            case "RESTARTING" -> "starting";
            case "STOPPED" -> "stopped";
            default -> "unknown";
        };
    }
}
```

- [ ] **Step 4: Run them**

Run: `scripts/mvn -q test -Dtest=TaskCollectorTest`
Expected: PASS, 9 tests.

- [ ] **Step 5: Validate what the collector builds against the schema**

In `BundleSchemaTest`, make the class and `validate` public:

```java
public class BundleSchemaTest {
```

```java
    public static Set<ValidationMessage> validate(String json) throws Exception {
```

Add to `TaskCollectorTest` (import `io.turbolytics.turbostats.connect.wire.BundleSchemaTest`):

```java
    // Whatever the collector builds must validate, not just hand-made
    // fixtures: a source bundle, a sink bundle and an exit bundle.
    @Test
    void collectedBundlesValidate() throws Exception {
        runningSource(50, 50, 10);
        jmx.put(SNK_TASK, "status", "running")
                .put(SNK_METRICS, "sink-record-read-total", 1000.0)
                .put(SNK_METRICS, "sink-record-send-total", 1000.0)
                .put(SNK_METRICS, "offset-commit-completion-total", 12.0);
        cluster.tasks.put(SNK, new TaskHealth("RUNNING", "10.0.3.8:8083", "sink"));
        cluster.connectorStates.put("customers-sink", "RUNNING");
        TaskCollector c = collector();
        List<Bundle> out = new ArrayList<>(c.collect(NOW));
        jmx.remove(SNK_TASK).remove(SNK_METRICS);
        cluster.connectorStates.remove("customers-sink");
        out.addAll(c.collect(NOW.plusSeconds(60)));
        assertEquals(3, out.size());
        for (Bundle b : out) {
            assertEquals(java.util.Set.of(), BundleSchemaTest.validate(b.toJson()), b.toJson());
        }
    }
```

Run: `scripts/mvn -q test -Dtest='TaskCollectorTest,BundleSchemaTest'`
Expected: PASS, 16 tests.

- [ ] **Step 6: Commit**

```bash
git add src
git commit -m "collect: one bundle per local task, and an exit when its connector ends

A source task reports acknowledged writes from the interceptor, and falls
back to written less in flight when the counter is absent or stalls,
warning once; a sink task reports read less in flight and its offset
commits as flushes. A task without metrics is skipped rather than sent as
zeros. A deleted or stopped connector's task sends its last bundle once
more, stopped, with an exit; a moved task sends nothing."
```

---

### Task 11: Sending

**Files:**
- Create: `src/main/java/io/turbolytics/turbostats/connect/report/Log.java`
- Create: `src/main/java/io/turbolytics/turbostats/connect/report/Sender.java`
- Create: `src/main/java/io/turbolytics/turbostats/connect/report/Reporter.java`
- Test: `src/test/java/io/turbolytics/turbostats/connect/report/SenderTest.java`, `ReporterTest.java`

**Interfaces:**
- Consumes: `Bundle` (Task 3), `Credential`, `Signer` (Task 4), `ReporterConfig` (Task 5), `TaskCollector` (Task 10).
- Produces:
  - `interface Log { void info(String m); void warn(String m); void debug(String m); }` and `Log.slf4j(Class<?>)`.
  - `Sender(URI reportTo, Credential credential, Duration timeout)` with `CompletableFuture<Integer> send(Bundle b, Instant now)`, completing with the HTTP status, or exceptionally.
  - `interface Sender.Port { CompletableFuture<Integer> send(Bundle b, Instant now); }`, implemented by `Sender`.
  - `Reporter(Supplier<List<Bundle>> collect, Sender.Port sender, Log log, Clock clock)` with `void tick()` and `void start(ScheduledExecutorService s, int intervalSeconds)`.

- [ ] **Step 1: Write the failing tests**

`SenderTest.java`:

```java
package io.turbolytics.turbostats.connect.report;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import io.turbolytics.turbostats.connect.sign.Credential;
import io.turbolytics.turbostats.connect.sign.Signer;
import io.turbolytics.turbostats.connect.wire.Fixtures;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class SenderTest {
    static final String KEY = "sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

    static PublicKey publicKey(byte[] raw) throws Exception {
        byte[] prefix = HexFormat.of().parseHex("302a300506032b6570032100");
        byte[] der = new byte[prefix.length + raw.length];
        System.arraycopy(prefix, 0, der, 0, prefix.length);
        System.arraycopy(raw, 0, der, prefix.length, raw.length);
        return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(der));
    }

    // The receiver verifies what arrives with the public key alone, as a
    // control plane does.
    @Test
    void postsASignedBundleTheReceiverCanVerify() throws Exception {
        Map<String, String> got = new ConcurrentHashMap<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/turbostats", ex -> {
            got.put("body", new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            got.put("type", ex.getRequestHeaders().getFirst("Content-Type"));
            got.put("ts", ex.getRequestHeaders().getFirst(Signer.HEADER_TIMESTAMP));
            got.put("sig", ex.getRequestHeaders().getFirst(Signer.HEADER_SIGNATURE));
            got.put("kid", ex.getRequestHeaders().getFirst(Signer.HEADER_KEY_ID));
            byte[] reply = "{\"v\":1,\"commands\":[]}".getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, reply.length);
            ex.getResponseBody().write(reply);
            ex.close();
        });
        server.start();
        try {
            Credential c = Credential.parse(KEY);
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/v1/turbostats");
            int status = new Sender(uri, c, Duration.ofSeconds(5))
                    .send(Fixtures.sourceBundle(), Instant.ofEpochSecond(1789848000)).get(10, TimeUnit.SECONDS);
            assertEquals(200, status);
            assertEquals(Signer.MEDIA_TYPE, got.get("type"));
            assertEquals(c.keyId(), got.get("kid"));
            assertEquals("1789848000", got.get("ts"));

            Signature v = Signature.getInstance("Ed25519");
            v.initVerify(publicKey(c.publicKey()));
            v.update(Signer.canonical("POST", "/v1/turbostats", 1789848000,
                    got.get("body").getBytes(StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8));
            assertTrue(v.verify(Base64.getDecoder().decode(got.get("sig"))));
        } finally {
            server.stop(0);
        }
    }
}
```

`ReporterTest.java`:

```java
package io.turbolytics.turbostats.connect.report;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.turbolytics.turbostats.connect.wire.Bundle;
import io.turbolytics.turbostats.connect.wire.Fixtures;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ReporterTest {
    static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-04T10:00:00Z"), ZoneOffset.UTC);

    static class RecordingLog implements Log {
        final List<String> warns = new ArrayList<>();
        final List<String> infos = new ArrayList<>();

        @Override
        public void info(String m) {
            infos.add(m);
        }

        @Override
        public void warn(String m) {
            warns.add(m);
        }

        @Override
        public void debug(String m) {
        }
    }

    static class StubSender implements Sender.Port {
        final List<CompletableFuture<Integer>> calls = new ArrayList<>();
        int status = 200;
        boolean hang;

        @Override
        public CompletableFuture<Integer> send(Bundle b, Instant now) {
            CompletableFuture<Integer> f = hang ? new CompletableFuture<>() : CompletableFuture.completedFuture(status);
            calls.add(f);
            return f;
        }
    }

    @Test
    void sendsEveryCollectedBundle() {
        StubSender s = new StubSender();
        new Reporter(() -> List.of(Fixtures.sourceBundle(), Fixtures.sinkBundle()), s, new RecordingLog(), CLOCK).tick();
        assertEquals(2, s.calls.size());
    }

    // A hung receiver delays nothing: an interval whose posts are still in
    // flight is skipped, not queued behind them.
    @Test
    void anIntervalStillInFlightIsSkipped() {
        StubSender s = new StubSender();
        s.hang = true;
        Reporter r = new Reporter(() -> List.of(Fixtures.sourceBundle()), s, new RecordingLog(), CLOCK);
        r.tick();
        r.tick();
        assertEquals(1, s.calls.size());
        s.calls.get(0).complete(200);
        r.tick();
        assertEquals(2, s.calls.size());
    }

    // Review focus: an unregistered key is refused forever. One warning when
    // it starts, one line when it recovers, and the worker stays healthy.
    @Test
    void aRefusingReceiverWarnsOnce() {
        StubSender s = new StubSender();
        s.status = 401;
        RecordingLog log = new RecordingLog();
        Reporter r = new Reporter(() -> List.of(Fixtures.sourceBundle()), s, log, CLOCK);
        for (int i = 0; i < 5; i++) {
            r.tick();
        }
        assertEquals(1, log.warns.size());
        s.status = 200;
        r.tick();
        assertEquals(1, log.infos.size());
    }

    // A collector that throws must not kill the scheduled thread, or
    // reporting stops for the life of the worker.
    @Test
    void aFailingCollectIsLoggedNotThrown() {
        RecordingLog log = new RecordingLog();
        Reporter r = new Reporter(() -> {
            throw new IllegalStateException("boom");
        }, new StubSender(), log, CLOCK);
        r.tick();
        r.tick();
        assertEquals(1, log.warns.size());
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `scripts/mvn -q test -Dtest='SenderTest,ReporterTest'`
Expected: FAIL to compile with `cannot find symbol: class Sender`.

- [ ] **Step 3: Implement**

`Log.java`:

```java
package io.turbolytics.turbostats.connect.report;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The three levels the reporter uses, behind an interface so tests can count them. */
public interface Log {
    void info(String message);

    void warn(String message);

    void debug(String message);

    static Log slf4j(Class<?> owner) {
        Logger l = LoggerFactory.getLogger(owner);
        return new Log() {
            @Override
            public void info(String m) {
                l.info(m);
            }

            @Override
            public void warn(String m) {
                l.warn(m);
            }

            @Override
            public void debug(String m) {
                l.debug(m);
            }
        };
    }
}
```

`Sender.java`:

```java
package io.turbolytics.turbostats.connect.report;

import io.turbolytics.turbostats.connect.sign.Credential;
import io.turbolytics.turbostats.connect.sign.Signer;
import io.turbolytics.turbostats.connect.wire.Bundle;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/** Posts one signed bundle. The response's commands are ignored in v1. */
public final class Sender implements Sender.Port {
    public interface Port {
        CompletableFuture<Integer> send(Bundle b, Instant now);
    }

    private final URI reportTo;
    private final Credential credential;
    private final Duration timeout;
    private final HttpClient client;

    public Sender(URI reportTo, Credential credential, Duration timeout) {
        this.reportTo = reportTo;
        this.credential = credential;
        this.timeout = timeout;
        this.client = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    @Override
    public CompletableFuture<Integer> send(Bundle b, Instant now) {
        byte[] body = b.toJson().getBytes(StandardCharsets.UTF_8);
        String path = Signer.requestPath(reportTo.getRawPath());
        HttpRequest.Builder req = HttpRequest.newBuilder(reportTo)
                .timeout(timeout)
                .header("Content-Type", Signer.MEDIA_TYPE)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        Signer.headers(credential, "POST", path, now.getEpochSecond(), body).forEach(req::header);
        // The body is discarded: a v1 reporter acts on no command, and
        // reading it would only hold the connection longer.
        return client.sendAsync(req.build(), HttpResponse.BodyHandlers.discarding()).thenApply(HttpResponse::statusCode);
    }
}
```

`Reporter.java`:

```java
package io.turbolytics.turbostats.connect.report;

import io.turbolytics.turbostats.connect.wire.Bundle;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Collects and sends every interval, on one thread.
 *
 * Posts go out asynchronously, and an interval whose posts are still in
 * flight is skipped: a hung receiver delays neither the worker nor the next
 * collection. Failures log one warning when they begin and one info line
 * when they end, as SQLFlow's reporter does; an unregistered key fails
 * forever, and a line per attempt would bury the worker's log.
 */
public final class Reporter {
    private final Supplier<List<Bundle>> collect;
    private final Sender.Port sender;
    private final Log log;
    private final Clock clock;
    private final AtomicBoolean inFlight = new AtomicBoolean();
    private volatile boolean failing;

    public Reporter(Supplier<List<Bundle>> collect, Sender.Port sender, Log log, Clock clock) {
        this.collect = collect;
        this.sender = sender;
        this.log = log;
        this.clock = clock;
    }

    public void start(ScheduledExecutorService scheduler, int intervalSeconds) {
        scheduler.scheduleAtFixedRate(this::tick, 0, intervalSeconds, TimeUnit.SECONDS);
    }

    /** Never throws: an exception would cancel the scheduled task for good. */
    public void tick() {
        if (!inFlight.compareAndSet(false, true)) {
            log.debug("turbostats: previous reports still in flight; skipping this interval");
            return;
        }
        try {
            Instant now = clock.instant();
            List<CompletableFuture<Integer>> posts = new ArrayList<>();
            for (Bundle b : collect.get()) {
                posts.add(sender.send(b, now).handle((status, err) -> {
                    if (err != null) {
                        fail("posting failed: " + err.getClass().getSimpleName());
                    } else if (status < 200 || status >= 300) {
                        fail("receiver answered " + status);
                    } else {
                        succeed();
                    }
                    return status;
                }));
            }
            CompletableFuture.allOf(posts.toArray(new CompletableFuture[0]))
                    .whenComplete((v, e) -> inFlight.set(false));
        } catch (Throwable t) {
            fail("collecting failed: " + t.getClass().getSimpleName() + ": " + t.getMessage());
            inFlight.set(false);
        }
    }

    private synchronized void fail(String why) {
        if (!failing) {
            failing = true;
            log.warn("turbostats reporting is failing: " + why);
        } else {
            log.debug("turbostats reporting still failing: " + why);
        }
    }

    private synchronized void succeed() {
        if (failing) {
            failing = false;
            log.info("turbostats reporting recovered");
        }
    }
}
```

- [ ] **Step 4: Run them**

Run: `scripts/mvn -q test -Dtest='SenderTest,ReporterTest'`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "report: post signed bundles asynchronously, one interval at a time

A receiver verifies what arrives with the public key alone, as a control
plane does. Posts are asynchronous and an interval still in flight is
skipped, so a hung receiver delays nothing. A refusing receiver logs one
warning, and a recovery one info line. A collector that throws is logged,
never allowed to cancel the schedule."
```

---

### Task 12: The REST extension

**Files:**
- Create: `src/main/java/io/turbolytics/turbostats/connect/TurboStatsExtension.java`
- Create: `src/main/resources/META-INF/services/org.apache.kafka.connect.rest.ConnectRestExtension`
- Test: `src/test/java/io/turbolytics/turbostats/connect/TurboStatsExtensionTest.java`

**Interfaces:**
- Consumes: everything above.
- Produces: `TurboStatsExtension implements ConnectRestExtension`, the class a worker names in `rest.extension.classes`; `static boolean loadedFromPluginPath(ClassLoader)`.

- [ ] **Step 1: Write the failing tests**

```java
package io.turbolytics.turbostats.connect;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TurboStatsExtensionTest {
    // A worker's REST server stops if configure throws. Nothing it is handed
    // may make it throw.
    @Test
    void configureNeverThrows() {
        Map<String, Object> garbage = new HashMap<>();
        garbage.put("turbostats.report.to", "http://public.example.com");
        garbage.put("turbostats.key", "nope");
        garbage.put("turbostats.label.Bad", "x");
        TurboStatsExtension e = new TurboStatsExtension();
        assertDoesNotThrow(() -> e.configure(garbage));
        assertDoesNotThrow(() -> e.configure(null));
        assertDoesNotThrow(() -> e.register(null));
        assertDoesNotThrow(e::close);
    }

    /** Stands in for Connect's isolated plugin loader, matched by name. */
    static final class PluginClassLoader extends ClassLoader {
    }

    // In the plugin path, the interceptors cannot load for any task, and
    // every source task on the worker fails. The extension says so loudly.
    @Test
    void detectsAPluginPathInstall() {
        assertTrue(TurboStatsExtension.loadedFromPluginPath(new PluginClassLoader()));
        assertFalse(TurboStatsExtension.loadedFromPluginPath(ClassLoader.getSystemClassLoader()));
        assertFalse(TurboStatsExtension.loadedFromPluginPath(null));
    }

    @Test
    void theServiceFileNamesTheExtension() throws Exception {
        try (var in = getClass().getResourceAsStream(
                "/META-INF/services/org.apache.kafka.connect.rest.ConnectRestExtension")) {
            assertTrue(new String(in.readAllBytes()).trim().equals(TurboStatsExtension.class.getName()));
        }
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `scripts/mvn -q test -Dtest=TurboStatsExtensionTest`
Expected: FAIL to compile with `cannot find symbol: class TurboStatsExtension`.

- [ ] **Step 3: Implement**

`src/main/resources/META-INF/services/org.apache.kafka.connect.rest.ConnectRestExtension`:

```
io.turbolytics.turbostats.connect.TurboStatsExtension
```

`TurboStatsExtension.java`:

```java
package io.turbolytics.turbostats.connect;

import io.turbolytics.turbostats.connect.collect.Cgroup;
import io.turbolytics.turbostats.connect.collect.ConnectClusterView;
import io.turbolytics.turbostats.connect.collect.ConnectMetrics;
import io.turbolytics.turbostats.connect.collect.JvmProcess;
import io.turbolytics.turbostats.connect.collect.PlatformJmx;
import io.turbolytics.turbostats.connect.collect.TaskCollector;
import io.turbolytics.turbostats.connect.config.ReporterConfig;
import io.turbolytics.turbostats.connect.report.Log;
import io.turbolytics.turbostats.connect.report.Reporter;
import io.turbolytics.turbostats.connect.report.Sender;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

/**
 * The entry point a worker names in rest.extension.classes.
 *
 * Every method catches everything. An exception from configure or register
 * stops the worker's REST server, and the monitoring jar must never be why
 * a worker or its connectors are down. A worker shutting down sends no exit
 * bundles: its tasks move to another worker, which reports them under the
 * same instance ids.
 */
public final class TurboStatsExtension implements ConnectRestExtension {
    private static final Log LOG = Log.slf4j(TurboStatsExtension.class);

    private ReporterConfig.Parsed parsed;
    private ScheduledExecutorService scheduler;

    @Override
    public void configure(Map<String, ?> configs) {
        try {
            if (loadedFromPluginPath(getClass().getClassLoader())) {
                LOG.warn("turbostats: this jar was loaded from the plugin path. Move it to the worker's classpath "
                        + "(/kafka/libs in the Debezium image): from the plugin path the interceptors cannot load, "
                        + "and every source task on this worker fails to build its producer");
            }
            parsed = ReporterConfig.parse(configs);
            parsed.warnings().forEach(w -> LOG.warn("turbostats: " + w));
            parsed.errors().forEach(e -> LOG.warn("turbostats: " + e));
            if (!parsed.errors().isEmpty()) {
                LOG.warn("turbostats: reporting is off until the settings above are fixed; connectors are unaffected");
            } else if (!parsed.enabled()) {
                LOG.info("turbostats: reporting is off; set turbostats.report.to and turbostats.key to turn it on");
            }
        } catch (Throwable t) {
            parsed = null;
            LOG.warn("turbostats: settings could not be read, reporting is off: " + t.getClass().getSimpleName());
        }
    }

    @Override
    public void register(ConnectRestExtensionContext ctx) {
        try {
            if (ctx == null || parsed == null || !parsed.enabled()) {
                return;
            }
            ReporterConfig cfg = parsed.config();
            TaskCollector collector = new TaskCollector(
                    cfg,
                    new ConnectMetrics(new PlatformJmx()),
                    new ConnectClusterView(ctx.clusterState()),
                    () -> JvmProcess.read(Cgroup.ROOT, Path.of("/proc/self/status")),
                    AppInfoParser.getVersion(),
                    LOG::warn);
            Sender sender = new Sender(cfg.reportTo(), cfg.credential(), Duration.ofSeconds(cfg.timeoutSeconds()));
            Reporter reporter = new Reporter(() -> collector.collect(Clock.systemUTC().instant()), sender, LOG,
                    Clock.systemUTC());
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "turbostats-reporter");
                t.setDaemon(true);
                return t;
            });
            reporter.start(scheduler, cfg.intervalSeconds());
            LOG.info("turbostats: reporting every " + cfg.intervalSeconds() + "s to " + cfg.reportTo()
                    + " as " + cfg.credential() + " for cluster " + cfg.cluster());
        } catch (Throwable t) {
            LOG.warn("turbostats: reporting could not start: " + t.getClass().getSimpleName());
        }
    }

    @Override
    public void close() {
        try {
            if (scheduler != null) {
                scheduler.shutdownNow();
            }
        } catch (Throwable ignored) {
            // Shutting down; nothing to report to.
        }
    }

    @Override
    public String version() {
        return ReporterVersion.get();
    }

    /** Connect's isolated loader is org.apache.kafka.connect.runtime.isolation.PluginClassLoader. */
    static boolean loadedFromPluginPath(ClassLoader cl) {
        return cl != null && cl.getClass().getName().endsWith("PluginClassLoader");
    }
}
```

- [ ] **Step 4: Run them, then the whole unit suite**

Run: `scripts/mvn -q test`
Expected: PASS, every unit test.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "extension: the entry point a worker names in rest.extension.classes

configure and register catch everything, because an exception there stops
the worker's REST server. A plugin-path install logs a warning that names
the fix: from there the interceptors cannot load and every source task
fails. A worker shutting down sends no exits; its tasks report from their
new worker."
```

---

### Task 13: Against a real worker

**Files:**
- Create: `src/test/java/io/turbolytics/turbostats/connect/WorkerIT.java`

**Interfaces:**
- Consumes: the built jar `target/kafka-connect-turbostats-0.1.0-SNAPSHOT.jar` (the failsafe plugin runs after `package`), `Credential` and `Signer` (Task 4), the vendored schema (Task 3).

- [ ] **Step 1: Write the integration test**

```java
package io.turbolytics.turbostats.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.turbolytics.turbostats.connect.sign.Credential;
import io.turbolytics.turbostats.connect.sign.Signer;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

/**
 * Drives the built jar inside a real Kafka Connect worker. A unit test
 * cannot prove the jar loads on the worker classpath, that Connect calls the
 * interceptors, or that a worker's real metrics map onto the contract.
 */
class WorkerIT {
    static final ObjectMapper MAPPER = new ObjectMapper();
    static final String KEY = "sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    static final Path JAR = Path.of("target/kafka-connect-turbostats-0.1.0-SNAPSHOT.jar");

    // Prints each post as one JSON line. It shares the worker's network
    // namespace, so the worker reports to the loopback, the only place the
    // reporter allows plaintext.
    static final String RECEIVER = String.join("\n",
            "import json",
            "from http.server import BaseHTTPRequestHandler, HTTPServer",
            "class H(BaseHTTPRequestHandler):",
            "    def do_POST(self):",
            "        body = self.rfile.read(int(self.headers.get('Content-Length', 0)))",
            "        print(json.dumps({'path': self.path,",
            "            'key_id': self.headers.get('X-Turbostats-Key-Id'),",
            "            'timestamp': self.headers.get('X-Turbostats-Timestamp'),",
            "            'signature': self.headers.get('X-Turbostats-Signature'),",
            "            'raw': body.decode('utf-8')}), flush=True)",
            "        self.send_response(200)",
            "        self.end_headers()",
            "        self.wfile.write(b'{\"v\":1,\"commands\":[]}')",
            "    def log_message(self, *args):",
            "        pass",
            "HTTPServer(('127.0.0.1', 8080), H).serve_forever()");

    static Network network;
    static GenericContainer<?> kafka;
    static GenericContainer<?> postgres;
    static GenericContainer<?> connect;
    static GenericContainer<?> receiver;
    static HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    static void start() {
        network = Network.newNetwork();
        kafka = new GenericContainer<>("apache/kafka:3.8.0")
                .withNetwork(network).withNetworkAliases("kafka")
                .withEnv("KAFKA_NODE_ID", "1")
                .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                .withEnv("KAFKA_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka:9092")
                .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka:9093")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
        kafka.start();
        postgres = new GenericContainer<>("postgres:16")
                .withNetwork(network).withNetworkAliases("postgres")
                .withEnv("POSTGRES_PASSWORD", "postgres")
                .withCommand("postgres", "-c", "wal_level=logical")
                .waitingFor(Wait.forLogMessage(".*ready to accept connections.*\\n", 2));
        postgres.start();
        connect = new GenericContainer<>("quay.io/debezium/connect:3.0")
                .withNetwork(network)
                .withCopyFileToContainer(MountableFile.forHostPath(JAR), "/kafka/libs/kafka-connect-turbostats.jar")
                .withEnv("BOOTSTRAP_SERVERS", "kafka:9092")
                .withEnv("GROUP_ID", "itest-connect")
                .withEnv("CONFIG_STORAGE_TOPIC", "itest_configs")
                .withEnv("OFFSET_STORAGE_TOPIC", "itest_offsets")
                .withEnv("STATUS_STORAGE_TOPIC", "itest_statuses")
                .withEnv("CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("STATUS_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_REST_EXTENSION_CLASSES", "io.turbolytics.turbostats.connect.TurboStatsExtension")
                .withEnv("CONNECT_PRODUCER_INTERCEPTOR_CLASSES", "io.turbolytics.turbostats.connect.intercept.AckInterceptor")
                .withEnv("CONNECT_CONSUMER_INTERCEPTOR_CLASSES", "io.turbolytics.turbostats.connect.intercept.ConsumeInterceptor")
                .withEnv("CONNECT_CONFIG_PROVIDERS", "env")
                .withEnv("CONNECT_CONFIG_PROVIDERS_ENV_CLASS", "org.apache.kafka.common.config.provider.EnvVarConfigProvider")
                .withEnv("CONNECT_TURBOSTATS_REPORT_TO", "http://127.0.0.1:8080/v1/turbostats")
                .withEnv("CONNECT_TURBOSTATS_KEY", "${env:TURBOSTATS_KEY}")
                .withEnv("TURBOSTATS_KEY", KEY)
                .withEnv("CONNECT_TURBOSTATS_CLUSTER", "itest")
                .withEnv("CONNECT_TURBOSTATS_INTERVAL_SECONDS", "2")
                .withEnv("CONNECT_TURBOSTATS_TIMEOUT_SECONDS", "1")
                .withEnv("CONNECT_TURBOSTATS_LABEL_ENV", "itest")
                .withExposedPorts(8083)
                .waitingFor(Wait.forHttp("/connectors").forPort(8083).withStartupTimeout(Duration.ofMinutes(3)));
        connect.start();
        receiver = new GenericContainer<>("python:3.12-alpine")
                .withNetworkMode("container:" + connect.getContainerId())
                .withCommand("python", "-u", "-c", RECEIVER);
        receiver.start();
        exec(postgres, "psql", "-U", "postgres", "-c",
                "CREATE TABLE customers (id serial PRIMARY KEY, name text);"
                        + "INSERT INTO customers (name) SELECT 'c' || g FROM generate_series(1, 1000) g;");
    }

    @AfterAll
    static void stop() {
        for (GenericContainer<?> c : new GenericContainer<?>[] {receiver, connect, postgres, kafka}) {
            if (c != null) {
                c.stop();
            }
        }
        if (network != null) {
            network.close();
        }
    }

    static void exec(GenericContainer<?> c, String... cmd) {
        try {
            c.execInContainer(cmd);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static int rest(String method, String path, String json) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(
                URI.create("http://" + connect.getHost() + ":" + connect.getMappedPort(8083) + path))
                .header("Content-Type", "application/json");
        b = json == null ? b.method(method, HttpRequest.BodyPublishers.noBody())
                : b.method(method, HttpRequest.BodyPublishers.ofString(json));
        return http.send(b.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    static String connector(String name, String password) {
        return "{\"connector.class\":\"io.debezium.connector.postgresql.PostgresConnector\","
                + "\"database.hostname\":\"postgres\",\"database.port\":\"5432\",\"database.user\":\"postgres\","
                + "\"database.password\":\"" + password + "\",\"database.dbname\":\"postgres\","
                + "\"topic.prefix\":\"" + name + "\",\"plugin.name\":\"pgoutput\",\"slot.name\":\"" + name.replace('-', '_') + "\","
                + "\"table.include.list\":\"public.customers\",\"tasks.max\":\"1\"}";
    }

    /** Every post the receiver has printed, parsed. */
    static List<JsonNode> posts() throws Exception {
        List<JsonNode> out = new ArrayList<>();
        for (String line : receiver.getLogs().split("\n")) {
            if (line.startsWith("{")) {
                out.add(MAPPER.readTree(line));
            }
        }
        return out;
    }

    /** Waits up to two minutes for a post whose body matches. */
    static JsonNode await(Predicate<JsonNode> match) throws Exception {
        Instant deadline = Instant.now().plusSeconds(120);
        while (Instant.now().isBefore(deadline)) {
            for (JsonNode p : posts()) {
                if (match.test(MAPPER.readTree(p.get("raw").asText()))) {
                    return p;
                }
            }
            Thread.sleep(1000);
        }
        throw new AssertionError("no matching post in 120 s; receiver saw:\n" + receiver.getLogs());
    }

    static JsonNode body(JsonNode post) throws Exception {
        return MAPPER.readTree(post.get("raw").asText());
    }

    static boolean is(JsonNode b, String id) {
        return b.at("/instance/id").asText().equals(id);
    }

    @Test
    void aRunningConnectorReportsSignedValidBundles() throws Exception {
        assertEquals(201, rest("PUT", "/connectors/inventory-cdc/config", connector("inventory-cdc", "postgres")));
        JsonNode post = await(b -> is(b, "itest/inventory-cdc/0")
                && b.at("/pipeline/state").asText().equals("running")
                && b.at("/pipeline/sink_rows_written").asLong() >= 1000);
        JsonNode b = body(post);

        try (InputStream in = getClass().getResourceAsStream("/schema/bundle.schema.json")) {
            JsonSchema schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(in);
            assertEquals(0, schema.validate(b).size(), schema.validate(b).toString());
        }

        Credential c = Credential.parse(KEY);
        assertEquals(c.keyId(), post.get("key_id").asText());
        byte[] der = HexFormat.of().parseHex("302a300506032b6570032100" + HexFormat.of().formatHex(c.publicKey()));
        Signature v = Signature.getInstance("Ed25519");
        v.initVerify(KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(der)));
        v.update(Signer.canonical("POST", "/v1/turbostats", post.get("timestamp").asLong(),
                post.get("raw").asText().getBytes(StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8));
        assertTrue(v.verify(Base64.getDecoder().decode(post.get("signature").asText())));

        assertEquals("itest/inventory-cdc", b.at("/instance/name").asText());
        assertEquals("kafka-connect", b.at("/instance/runtime").asText());
        assertEquals("postgres", b.at("/instance/source_type").asText());
        assertEquals("itest", b.at("/instance/labels/env").asText());
        assertTrue(b.at("/instance/config_hash").asText().startsWith("sha256:"));
        assertTrue(!post.get("raw").asText().contains("\"postgres\",\"database.password"));
        assertEquals("jvm", b.at("/process/memory/runtime").asText());
        assertTrue(!b.get("pipeline").has("sink_flush_count"));
        assertEquals(0, b.at("/pipeline/restart_count").asLong());
        assertTrue(b.at("/pipeline/sink_rows_written").asLong() <= b.at("/pipeline/sink_rows_accepted").asLong());
    }

    // The failure this reporter exists for: a failed task on a live worker.
    @Test
    void aFailedTaskReportsFailed() throws Exception {
        rest("PUT", "/connectors/bad-cdc/config", connector("bad-cdc", "wrong-password"));
        await(b -> is(b, "itest/bad-cdc/0") && b.at("/pipeline/state").asText().equals("failed"));
    }

    // One REST restart is one restart, though Connect re-registers the
    // task's metrics several times for it.
    @Test
    void aRestartCountsOnce() throws Exception {
        rest("PUT", "/connectors/restart-cdc/config", connector("restart-cdc", "postgres"));
        await(b -> is(b, "itest/restart-cdc/0") && b.at("/pipeline/state").asText().equals("running"));
        rest("POST", "/connectors/restart-cdc/tasks/0/restart", null);
        JsonNode b = body(await(x -> is(x, "itest/restart-cdc/0") && x.at("/pipeline/restart_count").asLong() >= 1));
        assertEquals(1, b.at("/pipeline/restart_count").asLong());
    }

    @Test
    void aDeletedConnectorSendsAnExit() throws Exception {
        rest("PUT", "/connectors/gone-cdc/config", connector("gone-cdc", "postgres"));
        await(b -> is(b, "itest/gone-cdc/0") && b.at("/pipeline/state").asText().equals("running"));
        rest("DELETE", "/connectors/gone-cdc", null);
        JsonNode b = body(await(x -> is(x, "itest/gone-cdc/0") && x.has("exit")));
        assertEquals("connector_deleted", b.at("/exit/reason").asText());
        assertEquals("stopped", b.at("/pipeline/state").asText());
    }
}
```

- [ ] **Step 2: Run it**

Run: `scripts/mvn verify`
Expected: unit tests PASS, then `WorkerIT` PASS, 4 tests, in under ten minutes. If Testcontainers cannot reach Docker from inside the Maven container, run the same command with a local JDK 17 and Maven instead: `brew install openjdk@17 maven && mvn verify`.

If `aRestartCountsOnce` reports 2, Connect configured the producer twice for one start, as the spike saw once. Do not change the test: record the observed sequence of `configure` calls in the ledger and stop for a decision, because the restart signal is then unreliable.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/io/turbolytics/turbostats/connect/WorkerIT.java
git commit -m "test: the built jar inside a real Kafka Connect worker

A unit test cannot prove the jar loads on the worker classpath, that
Connect calls the interceptors, or that real metrics map onto the
contract. Against Debezium on Kafka Connect 3.9: a running connector posts
signed bundles that validate and verify; a bad password reports failed;
one REST restart counts one; a deleted connector sends an exit."
```

---

### Task 14: The README and a release

**Files:**
- Modify: `README.md`
- Create: `.github/workflows/release.yml`

- [ ] **Step 1: Write the README**

Replace `README.md` with:

````markdown
# kafka-connect-turbostats

Reports every Kafka Connect connector task to TurboStats: whether it is running, failed or restarting, how much it read and wrote, and the worker's memory. One jar, no dependencies.

## Install it on the worker's classpath

**Put the jar in the worker's classpath, not its plugin path.** In the Debezium image that is `/kafka/libs`. Connect builds each task's producer and consumer with the connector's own classloader, which cannot see another plugin: from the plugin path, every source task on the worker fails with `Failed to construct kafka producer`.

Then add to the worker's properties:

```properties
rest.extension.classes=io.turbolytics.turbostats.connect.TurboStatsExtension
producer.interceptor.classes=io.turbolytics.turbostats.connect.intercept.AckInterceptor
consumer.interceptor.classes=io.turbolytics.turbostats.connect.intercept.ConsumeInterceptor

turbostats.report.to=https://control.turbolytics.io/v1/turbostats
turbostats.key=${env:TURBOSTATS_KEY}
config.providers=env
config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
```

If the worker already sets interceptor classes, add these to its list, comma-separated.

## Settings

| Property | Default | Means |
|---|---|---|
| `turbostats.report.to` | none: reporting is off | Where reports go. `https`, or `http` to the loopback only. |
| `turbostats.key` | none | The `sfc_` credential. Supply it through a config provider, never in plain text. |
| `turbostats.cluster` | the worker's `group.id` | The first part of every instance id. Set it when `group.id` is a stock value such as `connect-cluster`. |
| `turbostats.interval.seconds` | 60 | How often each task reports. |
| `turbostats.timeout.seconds` | 10 | How long one report may take. Less than the interval. |
| `turbostats.label.<key>` | none | Your own labels: at most 10, keys `[a-z][a-z0-9_]*`. |

A bad setting turns reporting off and logs why. It never stops the worker or a connector.

## What a report carries

One report per task, under the id `<cluster>/<connector>/<task>`:

- The task's state: `running`, `paused`, `failed`, `starting` or `stopped`.
- Restarts since the worker started, and when the task last started.
- Records read and written. A source task's written count is what the broker acknowledged.
- The worker's uptime, memory, garbage collections and container limit.
- The connector's type and a hash of its config. The config itself never leaves the worker: it holds your database password.

When a connector is deleted or stopped, each of its tasks sends a final report saying so. A task that moves to another worker continues under the same id.
````

- [ ] **Step 2: Write the release workflow**

`.github/workflows/release.yml`:

```yaml
name: release
on:
  push:
    tags: ["v*"]
jobs:
  release:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "17"
      - run: mvn -B versions:set -DnewVersion="${GITHUB_REF_NAME#v}" && mvn -B verify
      - run: gh release create "$GITHUB_REF_NAME" target/kafka-connect-turbostats-*.jar --generate-notes
        env:
          GH_TOKEN: ${{ github.token }}
```

- [ ] **Step 3: Check the README's properties against the integration test**

Run: `grep -c 'io.turbolytics.turbostats.connect' README.md src/test/java/io/turbolytics/turbostats/connect/WorkerIT.java`
Expected: both files name the same three classes; the README's class names match `WorkerIT`'s `CONNECT_*_CLASSES` values exactly.

- [ ] **Step 4: Commit**

```bash
git add README.md .github/workflows/release.yml
git commit -m "docs: install on the classpath, settings, and what a report carries

The README leads with the one install mistake that takes connectors down:
the plugin path. A tag builds, tests and attaches the jar to a GitHub
release."
```

---

## After the last task

Open one pull request with the plan and the implementation, against `main`, after `scripts/sync-schema main` once sql-flow PR #435 has merged. Plan B follows on its own branch: Debezium's backfill, `source_connected` and event lag; sink event lag from the consumer interceptor; sink lag in messages from the broker; and wire bytes.
