package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class ErrorCodeTest {
    static final String TRACE = """
            org.apache.kafka.connect.errors.ConnectException: Exiting WorkerSinkTask due to unrecoverable exception.
            \tat org.apache.kafka.connect.runtime.WorkerSinkTask.deliverMessages(WorkerSinkTask.java:632)
            Caused by: org.apache.kafka.connect.errors.ConnectException: java.sql.SQLException: insert failed
            \tat io.debezium.connector.jdbc.JdbcChangeEventSink.execute(JdbcChangeEventSink.java:80)
            Caused by: org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint
              Detail: Key (email)=(ana@example.com) already exists.
            \tat org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2725)
            \t... 14 more
            """;

    // The root cause names the failure; the top is Connect's wrapper.
    @Test
    void theCodeIsTheRootCausesClass() {
        assertEquals("org.postgresql.util.PSQLException", ErrorCode.fromTrace(TRACE));
    }

    @Test
    void aTraceWithoutACauseIsItsFirstClass() {
        assertEquals("java.lang.IllegalStateException",
                ErrorCode.fromTrace("java.lang.IllegalStateException: boom\n\tat a.B.c(B.java:1)"));
        assertEquals("java.lang.NullPointerException", ErrorCode.fromTrace("java.lang.NullPointerException"));
    }

    // Anything that is not a qualified class name is a message, which can
    // carry a row: no code rather than a guess.
    @Test
    void textThatIsNotAClassIsNoCode() {
        assertNull(ErrorCode.fromTrace(null));
        assertNull(ErrorCode.fromTrace(" "));
        assertNull(ErrorCode.fromTrace("Tolerance exceeded in error handler"));
        assertNull(ErrorCode.fromTrace("a.B: x\nCaused by: secret: hunter2"));
        assertNull(ErrorCode.fromTrace("a.B: x\nCaused by: Key (id)=(42) already exists"));
        assertNull(ErrorCode.fromTrace("a." + "b".repeat(ErrorCode.MAX_LENGTH)));
    }
}
