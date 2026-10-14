package io.turbolytics.turbostats.connect.collect;

/**
 * A task as the cluster reports it. trace is Connect's stack trace for a
 * failed task, null otherwise. It holds the failure's message, which can
 * carry a row or a connection string, so only its exception class leaves
 * the worker.
 */
public record TaskHealth(String state, String workerId, String connectorType, String trace) {
    public TaskHealth(String state, String workerId, String connectorType) {
        this(state, workerId, connectorType, null);
    }
}
