package io.turbolytics.turbostats.connect.report;

import io.turbolytics.turbostats.connect.wire.Bundle;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
 */
public final class Reporter {
    private final Supplier<List<Bundle>> collect;
    private final Sender.Port sender;
    private final Log log;
    private final Clock clock;
    private final AtomicBoolean inFlight = new AtomicBoolean();
    private volatile boolean failing;
    private int cleanTicks;

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
            List<CompletableFuture<String>> posts = new ArrayList<>();
            for (Bundle b : collect.get()) {
                // Each post resolves to null on success or to why it failed,
                // so the tick decides failing or recovered once, not per post.
                posts.add(sender.send(b, now).handle((status, err) -> {
                    if (err != null) {
                        return "posting failed: " + err.getClass().getSimpleName();
                    }
                    if (status == 429) {
                        return "receiver answered 429: reports arrive faster than it accepts them;"
                                + " raise turbostats.interval.seconds";
                    }
                    if (status < 200 || status >= 300) {
                        return "receiver answered " + status;
                    }
                    return null;
                }));
            }
            CompletableFuture.allOf(posts.toArray(new CompletableFuture[0])).whenComplete((v, e) -> {
                try {
                    String why = null;
                    for (CompletableFuture<String> p : posts) {
                        String r = p.getNow(null);
                        if (r != null) {
                            why = r;
                            break;
                        }
                    }
                    if (why != null) {
                        fail(why);
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
