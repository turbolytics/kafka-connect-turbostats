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
