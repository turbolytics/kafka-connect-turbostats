package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class TaskKeyTest {
    // Connect names a task's clients connector-producer-<connector>-<task>;
    // the spike saw connector-producer-inventory-cdc-0.
    @Test
    void readsTheTaskFromAProducerOrConsumerClientId() {
        assertEquals(Optional.of(new TaskKey("inventory-cdc", 0)), TaskKey.fromClientId("connector-producer-inventory-cdc-0"));
        assertEquals(Optional.of(new TaskKey("customers-sink", 12)), TaskKey.fromClientId("connector-consumer-customers-sink-12"));
    }

    // Review focus: the task is after the last hyphen; everything before it
    // is the connector, hyphens and all.
    @Test
    void aConnectorNameWithHyphensKeepsThem() {
        assertEquals(Optional.of(new TaskKey("orders-cdc-v2", 3)), TaskKey.fromClientId("connector-producer-orders-cdc-v2-3"));
    }

    // The worker's own clients and a DLQ producer are not a task's: the DLQ
    // producer's acknowledgments are failures landing, not output.
    @Test
    void otherClientsAreNotTasks() {
        assertTrue(TaskKey.fromClientId("spike-connect-statuses").isEmpty());
        assertTrue(TaskKey.fromClientId("connector-dlq-producer-inventory-cdc-0").isEmpty());
        assertTrue(TaskKey.fromClientId("connector-producer-noTask").isEmpty());
        assertTrue(TaskKey.fromClientId(null).isEmpty());
    }
}
