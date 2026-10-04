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

    static final byte[] KEY_A = "install-a".getBytes();
    static final byte[] KEY_B = "install-b".getBytes();

    // The hash changes with any setting and never carries one: a connector's
    // config holds its database password in plain text. It is keyed by the
    // install's credential, so it means nothing outside that install and
    // cannot be checked against a guessed password.
    @Test
    void theConfigHashIsStableKeyedAndRevealsNothing() {
        Map<String, String> a = new HashMap<>(Map.of("connector.class", "x", "database.password", "hunter2"));
        Map<String, String> b = new HashMap<>(Map.of("database.password", "hunter2", "connector.class", "x"));
        assertEquals(Identity.configHash(a, KEY_A), Identity.configHash(b, KEY_A));
        assertTrue(Identity.configHash(a, KEY_A).startsWith("hmac-sha256:"));
        assertTrue(!Identity.configHash(a, KEY_A).contains("hunter2"));
        assertNotEquals(Identity.configHash(a, KEY_A), Identity.configHash(a, KEY_B));
        b.put("database.password", "hunter3");
        assertNotEquals(Identity.configHash(a, KEY_A), Identity.configHash(b, KEY_A));
    }
}
