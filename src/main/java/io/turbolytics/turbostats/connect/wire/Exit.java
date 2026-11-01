package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;

/** How a task's run on this worker ended: stopped, deleted, removed or handed off. */
public record Exit(String reason, int code) {

    public JsonObject toJson() {
        return new JsonObject().put("reason", reason).put("code", (long) code);
    }
}
