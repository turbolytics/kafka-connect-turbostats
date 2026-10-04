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
import java.nio.file.Files;
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
 * The release test: the built jar, installed in the Debezium image as an
 * operator would install it, reporting to a fake control plane that records
 * every post. Neither a unit nor an integration test can prove the jar loads
 * on the worker classpath, that Connect calls the interceptors, or that a
 * worker's real metrics map onto the contract.
 */
class WorkerReleaseIT {
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
        // Recorded before anything stops, so a failure leaves the evidence:
        // every post the fake control plane received, one JSON line each.
        if (receiver != null) {
            try {
                Path out = Path.of("target/release/posts.jsonl");
                Files.createDirectories(out.getParent());
                Files.writeString(out, receiver.getLogs());
            } catch (Exception e) {
                System.err.println("could not record the posts: " + e);
            }
        }
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
    // The task must start and then fail: Debezium rejects a bad password
    // while validating the config, before any task exists. A transform that
    // casts the whole record envelope to int8 passes validation and fails
    // the task on its first record.
    @Test
    void aFailedTaskReportsFailed() throws Exception {
        String base = connector("bad-cdc", "postgres");
        String failing = base.substring(0, base.length() - 1)
                + ",\"transforms\":\"bad\","
                + "\"transforms.bad.type\":\"org.apache.kafka.connect.transforms.Cast$Value\","
                + "\"transforms.bad.spec\":\"int8\"}";
        assertEquals(201, rest("PUT", "/connectors/bad-cdc/config", failing));
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
