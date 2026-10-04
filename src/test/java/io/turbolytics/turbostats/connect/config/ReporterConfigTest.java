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
