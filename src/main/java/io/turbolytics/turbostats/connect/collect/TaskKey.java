package io.turbolytics.turbostats.connect.collect;

import java.util.Optional;

/** One connector task, the unit a bundle reports. */
public record TaskKey(String connector, int task) {
    private static final String PRODUCER = "connector-producer-";
    private static final String CONSUMER = "connector-consumer-";

    /**
     * Reads the task from the client.id Connect gives a task's producer or
     * consumer. The task is the part after the last hyphen, because a
     * connector name may contain hyphens and a task number cannot.
     */
    public static Optional<TaskKey> fromClientId(String clientId) {
        if (clientId == null) {
            return Optional.empty();
        }
        String rest;
        if (clientId.startsWith(PRODUCER)) {
            rest = clientId.substring(PRODUCER.length());
        } else if (clientId.startsWith(CONSUMER)) {
            rest = clientId.substring(CONSUMER.length());
        } else {
            return Optional.empty();
        }
        int dash = rest.lastIndexOf('-');
        if (dash <= 0 || dash == rest.length() - 1) {
            return Optional.empty();
        }
        try {
            return Optional.of(new TaskKey(rest.substring(0, dash), Integer.parseInt(rest.substring(dash + 1))));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
