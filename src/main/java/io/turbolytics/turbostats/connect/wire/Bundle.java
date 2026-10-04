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
