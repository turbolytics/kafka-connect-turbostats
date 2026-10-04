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
