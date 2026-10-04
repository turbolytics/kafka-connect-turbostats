package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;

/** How a stopped or deleted connector's task ended. */
public record Exit(String reason, int code) {

    public JsonObject toJson() {
        return new JsonObject().put("reason", reason).put("code", (long) code);
    }
}
