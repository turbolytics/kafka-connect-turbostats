package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class AdminSettingsTest {
    // The worker's connection and security settings, with admin.* overrides,
    // and nothing else: the worker's other keys would only be logged as unused.
    @Test
    void keepsConnectionAndSecuritySettingsOnly() {
        Properties p = AdminSettings.from(Map.of("bootstrap.servers", "kafka:9092", "security.protocol", "SASL_SSL",
                "sasl.mechanism", "PLAIN", "ssl.truststore.location", "/t", "admin.request.timeout.ms", "5000",
                "group.id", "connect", "turbostats.key", "secret"));
        assertEquals("kafka:9092", p.get("bootstrap.servers"));
        assertEquals("SASL_SSL", p.get("security.protocol"));
        assertEquals("PLAIN", p.get("sasl.mechanism"));
        assertEquals("/t", p.get("ssl.truststore.location"));
        assertEquals("5000", p.get("request.timeout.ms"));
        assertFalse(p.containsKey("group.id"));
        assertFalse(p.containsKey("turbostats.key"));
        assertEquals("turbostats-reporter", p.get("client.id"));
    }
}
