package io.turbolytics.turbostats.connect.collect;

/**
 * What the cluster said about a connector. Not found and unknown are
 * different answers: only the first means the connector was deleted. A
 * timeout or a rebalance race is unknown, and is asked again next tick.
 */
public record ConnectorLookup(Answer answer, String state) {
    public enum Answer {
        FOUND,
        NOT_FOUND,
        UNKNOWN
    }

    public static ConnectorLookup of(String state) {
        return new ConnectorLookup(Answer.FOUND, state);
    }

    public static ConnectorLookup notFound() {
        return new ConnectorLookup(Answer.NOT_FOUND, null);
    }

    public static ConnectorLookup unknown() {
        return new ConnectorLookup(Answer.UNKNOWN, null);
    }
}
