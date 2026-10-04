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
