package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;

/**
 * Bounded work inside the pipeline: a Debezium snapshot. state is none,
 * running, paused, completed, aborted, or unknown when the snapshot metrics
 * are missing or belong to an earlier run. blocksStream means nothing
 * unless the state is running or paused.
 */
public record Backfill(String state, boolean blocksStream, Long elapsedSeconds, String unit, Integer unitsTotal,
        Integer unitsLeft, Long rowsRead) {

    public static Backfill unknown() {
        return new Backfill("unknown", false, null, null, null, null, null);
    }

    public static Backfill none() {
        return new Backfill("none", false, null, null, null, null, null);
    }

    public JsonObject toJson() {
        return new JsonObject()
                .put("state", state)
                .put("blocks_stream", blocksStream)
                .put("elapsed_seconds", elapsedSeconds)
                .put("unit", unit)
                .put("units_total", unitsTotal == null ? null : (long) unitsTotal)
                .put("units_left", unitsLeft == null ? null : (long) unitsLeft)
                .put("rows_read", rowsRead);
    }
}
