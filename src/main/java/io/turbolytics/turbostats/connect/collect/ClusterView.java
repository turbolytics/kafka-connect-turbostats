package io.turbolytics.turbostats.connect.collect;

import java.util.Map;
import java.util.Optional;

/** The cluster's view of connectors and tasks, behind an interface so tests need no worker. */
public interface ClusterView {
    Optional<TaskHealth> task(TaskKey k);

    /** The connector's state, or whether it was not found, or no answer. */
    ConnectorLookup connector(String connector);

    /** The connector's config, empty when unknown. Holds secrets: hash it, never send it. */
    Map<String, String> config(String connector);
}
