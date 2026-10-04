package io.turbolytics.turbostats.connect.report;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.wire.Bundle;
import io.turbolytics.turbostats.connect.wire.Fixtures;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ReporterTest {
    static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-04T10:00:00Z"), ZoneOffset.UTC);

    static class RecordingLog implements Log {
        final List<String> warns = new ArrayList<>();
        final List<String> infos = new ArrayList<>();

        @Override
        public void info(String m) {
            infos.add(m);
        }

        @Override
        public void warn(String m) {
            warns.add(m);
        }

        @Override
        public void debug(String m) {
        }
    }

    static class StubSender implements Sender.Port {
        final List<CompletableFuture<Integer>> calls = new ArrayList<>();
        int status = 200;
        boolean hang;

        @Override
        public CompletableFuture<Integer> send(Bundle b, Instant now) {
            CompletableFuture<Integer> f = hang ? new CompletableFuture<>() : CompletableFuture.completedFuture(status);
            calls.add(f);
            return f;
        }
    }

    @Test
    void sendsEveryCollectedBundle() {
        StubSender s = new StubSender();
        new Reporter(() -> List.of(Fixtures.sourceBundle(), Fixtures.sinkBundle()), s, new RecordingLog(), CLOCK).tick();
        assertEquals(2, s.calls.size());
    }

    // A hung receiver delays nothing: an interval whose posts are still in
    // flight is skipped, not queued behind them.
    @Test
    void anIntervalStillInFlightIsSkipped() {
        StubSender s = new StubSender();
        s.hang = true;
        Reporter r = new Reporter(() -> List.of(Fixtures.sourceBundle()), s, new RecordingLog(), CLOCK);
        r.tick();
        r.tick();
        assertEquals(1, s.calls.size());
        s.calls.get(0).complete(200);
        r.tick();
        assertEquals(2, s.calls.size());
    }

    // Review focus: an unregistered key is refused forever. One warning when
    // it starts, one line when it recovers, and the worker stays healthy.
    @Test
    void aRefusingReceiverWarnsOnce() {
        StubSender s = new StubSender();
        s.status = 401;
        RecordingLog log = new RecordingLog();
        Reporter r = new Reporter(() -> List.of(Fixtures.sourceBundle()), s, log, CLOCK);
        for (int i = 0; i < 5; i++) {
            r.tick();
        }
        assertEquals(1, log.warns.size());
        // Recovered only after a run of clean ticks, not the first.
        s.status = 200;
        r.tick();
        r.tick();
        assertEquals(0, log.infos.size());
        r.tick();
        assertEquals(1, log.infos.size());
    }

    // Live against Control, a 15 s interval met a two-a-minute limit: every
    // other tick was refused, and the log said failing, recovered, failing,
    // 105 lines in two hours. A receiver that refuses every other tick is
    // still failing.
    @Test
    void alternatingRefusalsDoNotFlapTheLog() {
        StubSender s = new StubSender();
        RecordingLog log = new RecordingLog();
        Reporter r = new Reporter(() -> List.of(Fixtures.sourceBundle()), s, log, CLOCK);
        for (int i = 0; i < 10; i++) {
            s.status = i % 2 == 0 ? 429 : 200;
            r.tick();
        }
        assertEquals(1, log.warns.size());
        assertEquals(0, log.infos.size());
    }

    // A 429 is the receiver's rate limit. The warning says what to change.
    @Test
    void aRateLimitNamesTheInterval() {
        StubSender s = new StubSender();
        s.status = 429;
        RecordingLog log = new RecordingLog();
        new Reporter(() -> List.of(Fixtures.sourceBundle()), s, log, CLOCK).tick();
        assertTrue(log.warns.get(0).contains("turbostats.interval.seconds"), log.warns.get(0));
    }

    // A receiver that accepts some bundles and refuses others is failing,
    // once: the log must not flap between warning and recovery every tick.
    @Test
    void aPartlyRefusingReceiverDoesNotFlap() {
        RecordingLog log = new RecordingLog();
        Sender.Port mixed = new Sender.Port() {
            int n;

            @Override
            public CompletableFuture<Integer> send(Bundle b, Instant now) {
                return CompletableFuture.completedFuture(n++ % 2 == 0 ? 200 : 429);
            }
        };
        Reporter r = new Reporter(() -> List.of(Fixtures.sourceBundle(), Fixtures.sinkBundle()), mixed, log, CLOCK);
        for (int i = 0; i < 5; i++) {
            r.tick();
        }
        assertEquals(1, log.warns.size());
        assertEquals(0, log.infos.size());
    }

    // A collector that throws must not kill the scheduled thread, or
    // reporting stops for the life of the worker.
    @Test
    void aFailingCollectIsLoggedNotThrown() {
        RecordingLog log = new RecordingLog();
        Reporter r = new Reporter(() -> {
            throw new IllegalStateException("boom");
        }, new StubSender(), log, CLOCK);
        r.tick();
        r.tick();
        assertEquals(1, log.warns.size());
    }
}
