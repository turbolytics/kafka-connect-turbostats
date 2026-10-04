package io.turbolytics.turbostats.connect.collect;

/** What the cluster says about one task: its state, its worker, and its connector's type. */
public record TaskHealth(String state, String workerId, String connectorType) {
}
