package io.turbolytics.turbostats.connect.report;

import io.turbolytics.turbostats.connect.wire.Bundle;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Collects and sends every interval, on one thread.
 *
 * Posts go out asynchronously, and an interval whose posts are still in
 * flight is skipped: a hung receiver delays neither the worker nor the next
 * collection. Failures log one warning when they begin and one info line
 * when they end, as SQLFlow's reporter does; an unregistered key fails
 * forever, and a line per attempt would bury the worker's log. A tick with
 * any failed post is a failing tick, so a receiver that refuses some
 * bundles does not flap the log between the two.
 *
 * The warning names the most actionable failure of the tick, with the
 * receiver's own reason: a refused key before a rate limit, since a worker
 * with many tasks and a bad key meets the receiver's per-address limit
 * after its first refusals, and "raise the interval" would be wrong advice.
 */
public final class Reporter {
    private final Supplier<List<Bundle>> collect;
    private final Sender.Port sender;
    private final Log log;
    private final Clock clock;
    private final AtomicBoolean inFlight = new AtomicBoolean();
    private volatile boolean failing;
    private int cleanTicks;
    // Instance id to when its receiver asked to hear again, from a 429's
    // Retry-After. Written on the HTTP client's threads.
    private final Map<String, Instant> quietUntil = new ConcurrentHashMap<>();

    /** A Retry-After longer than this is a receiver's mistake, not a plan. */
    static final long MAX_RETRY_AFTER_SECONDS = 3600;

    record Failure(int rank, String why) {
    }

    /** Clean ticks in a row before failing is declared over. */
    static final int RECOVERY_TICKS = 3;

    public Reporter(Supplier<List<Bundle>> collect, Sender.Port sender, Log log, Clock clock) {
        this.collect = collect;
        this.sender = sender;
        this.log = log;
        this.clock = clock;
    }

    public void start(ScheduledExecutorService scheduler, int intervalSeconds) {
        scheduler.scheduleAtFixedRate(this::tick, 0, intervalSeconds, TimeUnit.SECONDS);
    }

    /** Never throws: an exception would cancel the scheduled task for good. */
    public void tick() {
        if (!inFlight.compareAndSet(false, true)) {
            log.debug("turbostats: previous reports still in flight; skipping this interval");
            return;
        }
        try {
            Instant now = clock.instant();
            List<CompletableFuture<Failure>> posts = new ArrayList<>();
            for (Bundle b : collect.get()) {
                String id = b.instance().id();
                Instant quiet = quietUntil.get(id);
                // An exit is sent once, so it is never held back.
                if (quiet != null && now.isBefore(quiet) && b.exit() == null) {
                    log.debug("turbostats: " + id + " waits for the receiver's Retry-After");
                    continue;
                }
                quietUntil.remove(id);
                // Each post resolves to null on success or to why it failed,
                // so the tick decides failing or recovered once, not per post.
                CompletableFuture<Sender.Answer> sent;
                try {
                    sent = sender.send(b, now);
                } catch (RuntimeException e) {
                    // One bundle that cannot be sent must not cost the others.
                    sent = CompletableFuture.failedFuture(e);
                }
                posts.add(sent.handle((answer, err) -> {
                    if (err != null) {
                        return new Failure(1, "posting failed: " + cause(err));
                    }
                    if (answer.status() == 429 && answer.retryAfterSeconds() > 0) {
                        quietUntil.put(id, now.plusSeconds(
                                Math.min(answer.retryAfterSeconds(), MAX_RETRY_AFTER_SECONDS)));
                    }
                    return refusal(answer);
                }));
            }
            CompletableFuture.allOf(posts.toArray(new CompletableFuture[0])).whenComplete((v, e) -> {
                try {
                    Failure worst = null;
                    for (CompletableFuture<Failure> p : posts) {
                        Failure f = p.getNow(null);
                        if (f != null && (worst == null || f.rank() < worst.rank())) {
                            worst = f;
                        }
                    }
                    if (worst != null) {
                        fail(worst.why());
                    } else if (!posts.isEmpty()) {
                        succeed();
                    }
                } finally {
                    inFlight.set(false);
                }
            });
        } catch (Throwable t) {
            fail("collecting failed: " + t.getClass().getSimpleName() + ": " + t.getMessage());
            inFlight.set(false);
        }
    }

    /** Null for a success; otherwise why, ranked: lower is more actionable. */
    static Failure refusal(Sender.Answer a) {
        int s = a.status();
        if (s >= 200 && s < 300) {
            return null;
        }
        String said = a.reason() == null ? "" : ": " + a.reason();
        if (s == 401 || s == 403) {
            return new Failure(0, "receiver answered " + s + said + "; check turbostats.key and the worker's clock");
        }
        if (s == 429) {
            return new Failure(3, "receiver answered 429" + said
                    + "; if reports arrive more often than every 30s, raise turbostats.interval.seconds");
        }
        if (s == 404) {
            return new Failure(1, "receiver answered 404" + said + "; check turbostats.report.to");
        }
        return new Failure(s >= 500 ? 2 : 1, "receiver answered " + s + said);
    }

    /**
     * The failure and its root cause, with the root's message: an
     * SSLHandshakeException says nothing until its cause names the trust
     * store. Neither carries the key or the report.
     */
    static String cause(Throwable err) {
        Throwable top = err;
        while ((top instanceof CompletionException || top instanceof ExecutionException) && top.getCause() != null) {
            top = top.getCause();
        }
        if (top instanceof TimeoutException) {
            return "no answer within turbostats.timeout.seconds";
        }
        Throwable root = top;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String out = top.getClass().getSimpleName();
        if (root != top) {
            out += ": " + root.getClass().getSimpleName();
        }
        if (root.getMessage() != null) {
            out += ": " + root.getMessage();
        }
        return Sender.reason(out);
    }

    private synchronized void fail(String why) {
        cleanTicks = 0;
        if (!failing) {
            failing = true;
            log.warn("turbostats reporting is failing: " + why);
        } else {
            log.debug("turbostats reporting still failing: " + why);
        }
    }

    /**
     * Recovered only after RECOVERY_TICKS clean ticks in a row. A receiver
     * that refuses every other tick, as a rate limit does, is still failing;
     * declaring recovery on each clean tick logged a warning and a recovery
     * every interval.
     */
    private synchronized void succeed() {
        if (!failing) {
            return;
        }
        if (++cleanTicks >= RECOVERY_TICKS) {
            failing = false;
            cleanTicks = 0;
            log.info("turbostats reporting recovered");
        }
    }
}
