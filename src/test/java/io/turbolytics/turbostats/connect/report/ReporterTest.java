package io.turbolytics.turbostats.connect.report;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        final List<CompletableFuture<Sender.Answer>> calls = new ArrayList<>();
        int status = 200;
        String reason;
        long retryAfter;
        boolean hang;

        @Override
        public CompletableFuture<Sender.Answer> send(Bundle b, Instant now) {
            CompletableFuture<Sender.Answer> f = hang ? new CompletableFuture<>()
                    : CompletableFuture.completedFuture(new Sender.Answer(status, reason, retryAfter));
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
        s.calls.get(0).complete(Sender.Answer.of(200));
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
            public CompletableFuture<Sender.Answer> send(Bundle b, Instant now) {
                return CompletableFuture.completedFuture(Sender.Answer.of(n++ % 2 == 0 ? 200 : 429));
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

    /** The sink fixture under its own id: the two fixtures share one. */
    static Bundle sink() {
        Bundle b = Fixtures.sinkBundle();
        io.turbolytics.turbostats.connect.wire.Instance i = b.instance();
        return new Bundle(b.sentAt(), b.intervalSeconds(), b.lastActivityAt(),
                new io.turbolytics.turbostats.connect.wire.Instance("prod-connect/customers-sink/0",
                        "prod-connect/customers-sink", i.version(), i.commit(), i.arch(), i.configHash(),
                        i.sourceType(), i.sinkType(), i.runtime(), i.runtimeVersion(), i.reporterVersion(),
                        i.labels()),
                b.process(), b.pipeline(), b.exit());
    }

    // A refusal says why, in the receiver's words: a 401 alone cannot tell
    // a revoked key from a skewed clock.
    @Test
    void aRefusalCarriesTheReceiversReason() {
        StubSender s = new StubSender();
        s.status = 401;
        s.reason = "timestamp is 7m from this clock";
        RecordingLog log = new RecordingLog();
        new Reporter(() -> List.of(Fixtures.sourceBundle()), s, log, CLOCK).tick();
        assertTrue(log.warns.get(0).contains("401: timestamp is 7m from this clock"), log.warns.get(0));
        assertTrue(log.warns.get(0).contains("turbostats.key"), log.warns.get(0));
    }

    // A 403 is the org's plan refusing a task, never the key or the clock:
    // Control verified the report before it counted it. Pointing at the key
    // sent operators to rotate a key that worked.
    @Test
    void aLimitRefusalNamesTheLimitNotTheKey() {
        StubSender s = new StubSender();
        s.status = 403;
        s.reason = "this org is at its limit of 3 pipelines; e2e/refunds-cdc is not one of them.";
        RecordingLog log = new RecordingLog();
        new Reporter(() -> List.of(Fixtures.sourceBundle()), s, log, CLOCK).tick();
        String warn = log.warns.get(0);
        assertTrue(warn.contains("403: this org is at its limit of 3 pipelines"), warn);
        assertFalse(warn.contains("turbostats.key"), warn);
        assertFalse(warn.contains("clock"), warn);
    }

    // A worker with many tasks and a bad key meets the receiver's limit
    // after its first refusals. The warning names the key, not the rate.
    @Test
    void aRefusedKeyOutranksARateLimit() {
        RecordingLog log = new RecordingLog();
        Sender.Port mixed = (b, now) -> CompletableFuture.completedFuture(
                b.instance().id().equals(sink().instance().id())
                        ? new Sender.Answer(401, "no such key", 0)
                        : new Sender.Answer(429, "too many requests", 30));
        new Reporter(() -> List.of(Fixtures.sourceBundle(), sink()), mixed, log, CLOCK).tick();
        assertTrue(log.warns.get(0).contains("401: no such key"), log.warns.get(0));
    }

    // A 429's Retry-After holds that instance back until it passes, and
    // only that instance. An exit is never held back: it is sent once.
    @Test
    void aRetryAfterHoldsBackOnlyItsInstance() {
        java.util.concurrent.atomic.AtomicReference<Instant> now =
                new java.util.concurrent.atomic.AtomicReference<>(CLOCK.instant());
        java.time.Clock clock = new java.time.Clock() {
            public java.time.ZoneId getZone() {
                return java.time.ZoneOffset.UTC;
            }

            public java.time.Clock withZone(java.time.ZoneId z) {
                return this;
            }

            public Instant instant() {
                return now.get();
            }
        };
        List<String> sent = new ArrayList<>();
        String source = Fixtures.sourceBundle().instance().id();
        Sender.Port port = (b, at) -> {
            sent.add(b.instance().id() + (b.exit() == null ? "" : " exit"));
            return CompletableFuture.completedFuture(b.instance().id().equals(source) && b.exit() == null
                    ? new Sender.Answer(429, null, 120) : Sender.Answer.of(200));
        };
        List<Bundle> bundles = new ArrayList<>(List.of(Fixtures.sourceBundle(), sink()));
        Reporter r = new Reporter(() -> bundles, port, new RecordingLog(), clock);
        r.tick();
        now.set(now.get().plusSeconds(60));
        r.tick();
        String sink = sink().instance().id();
        assertEquals(List.of(source, sink, sink), sent);

        bundles.set(0, Fixtures.sourceBundle().withExit(
                new io.turbolytics.turbostats.connect.wire.Exit("connector_deleted", 0), now.get()));
        r.tick();
        assertEquals(source + " exit", sent.get(3));

        bundles.set(0, Fixtures.sourceBundle());
        now.set(now.get().plusSeconds(61));
        r.tick();
        assertTrue(sent.subList(5, sent.size()).contains(source), sent.toString());
    }

    // A transport failure names its root cause and its message.
    @Test
    void aTransportFailureNamesItsCause() {
        Exception tls = new javax.net.ssl.SSLHandshakeException("handshake failed");
        tls.initCause(new IllegalStateException("PKIX path building failed"));
        assertEquals("SSLHandshakeException: IllegalStateException: PKIX path building failed",
                Reporter.cause(new java.util.concurrent.CompletionException(tls)));
        assertEquals("no answer within turbostats.timeout.seconds",
                Reporter.cause(new java.util.concurrent.CompletionException(new java.util.concurrent.TimeoutException())));
    }

    // A sender that throws costs only its own bundle.
    @Test
    void aSenderThatThrowsCostsOnlyItsBundle() {
        List<String> sent = new ArrayList<>();
        String source = Fixtures.sourceBundle().instance().id();
        Sender.Port port = (b, at) -> {
            if (b.instance().id().equals(source)) {
                throw new IllegalStateException("boom");
            }
            sent.add(b.instance().id());
            return CompletableFuture.completedFuture(Sender.Answer.of(200));
        };
        RecordingLog log = new RecordingLog();
        new Reporter(() -> List.of(Fixtures.sourceBundle(), sink()), port, log, CLOCK).tick();
        assertEquals(List.of(sink().instance().id()), sent);
        assertTrue(log.warns.get(0).contains("IllegalStateException: boom"), log.warns.get(0));
    }
}
