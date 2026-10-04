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
