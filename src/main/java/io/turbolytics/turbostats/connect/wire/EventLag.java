package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;
import java.time.Instant;

/**
 * How far behind the stream a task runs, in time. The four fields travel
 * together: a reading without its basis cannot be compared, and one without
 * its time cannot be judged for age.
 */
public record EventLag(Double seconds, Double maxSeconds, Instant observedAt, String basis) {

    void writeInto(JsonObject o) {
        o.put("event_lag_seconds", seconds)
                .put("event_lag_max_seconds", maxSeconds)
                .put("event_lag_observed_at", observedAt)
                .put("event_lag_basis", basis);
    }
}
