package io.turbolytics.turbostats.connect;

import io.turbolytics.turbostats.connect.collect.AdminSettings;
import io.turbolytics.turbostats.connect.collect.BrokerLag;
import io.turbolytics.turbostats.connect.collect.Cgroup;
import io.turbolytics.turbostats.connect.collect.ConnectClusterView;
import io.turbolytics.turbostats.connect.collect.JvmProcess;
import io.turbolytics.turbostats.connect.collect.KafkaGroupAdmin;
import io.turbolytics.turbostats.connect.collect.PlatformJmx;
import io.turbolytics.turbostats.connect.collect.TaskCollector;
import io.turbolytics.turbostats.connect.config.ReporterConfig;
import io.turbolytics.turbostats.connect.report.Log;
import io.turbolytics.turbostats.connect.report.Reporter;
import io.turbolytics.turbostats.connect.report.Sender;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

/**
 * The entry point a worker names in rest.extension.classes.
 *
 * Every method catches everything. An exception from configure or register
 * stops the worker's REST server, and the monitoring jar must never be why
 * a worker or its connectors are down. A worker shutting down hands off
 * its tasks: they move to another worker, which reports them under the
 * same instance ids.
 */
public final class TurboStatsExtension implements ConnectRestExtension {
    private static final Log LOG = Log.slf4j(TurboStatsExtension.class);

    private ReporterConfig.Parsed parsed;
    private ScheduledExecutorService scheduler;
    private Map<String, ?> worker = Map.of();
    private BrokerLag brokerLag;
    private TaskCollector collector;
    private Reporter reporter;
    private Duration handOffWait;

    /**
     * The longest a stopping worker waits for its hand-offs. Docker gives a
     * container 10 s to stop and Kubernetes 30 s, and the tasks need most of
     * it to stop and commit their offsets.
     */
    static final int HAND_OFF_WAIT_SECONDS = 3;

    @Override
    public void configure(Map<String, ?> configs) {
        try {
            if (loadedFromPluginPath(getClass().getClassLoader())) {
                LOG.warn("turbostats: this jar was loaded from the plugin path. Move it to the worker's classpath "
                        + "(/kafka/libs in the Debezium image): from the plugin path the interceptors cannot load, "
                        + "and every source task on this worker fails to build its producer");
            }
            worker = configs == null ? Map.of() : new java.util.HashMap<>(configs);
            parsed = ReporterConfig.parse(configs);
            parsed.warnings().forEach(w -> LOG.warn("turbostats: " + w));
            parsed.errors().forEach(e -> LOG.warn("turbostats: " + e));
            if (!parsed.errors().isEmpty()) {
                LOG.warn("turbostats: reporting is off until the settings above are fixed; connectors are unaffected");
            } else if (!parsed.enabled()) {
                LOG.info("turbostats: reporting is off; set turbostats.report.to and turbostats.key to turn it on");
            }
        } catch (Throwable t) {
            parsed = null;
            LOG.warn("turbostats: settings could not be read, reporting is off: " + t.getClass().getSimpleName());
        }
    }

    @Override
    public void register(ConnectRestExtensionContext ctx) {
        try {
            if (ctx == null || parsed == null || !parsed.enabled()) {
                return;
            }
            ReporterConfig cfg = parsed.config();
            // Built on the first tick that needs it, so a worker without sink
            // connectors never opens a connection for it.
            Map<String, ?> settings = worker;
            brokerLag = new BrokerLag(() -> new KafkaGroupAdmin(AdminSettings.from(settings)),
                    Duration.ofSeconds(cfg.timeoutSeconds()), LOG::warn);
            collector = new TaskCollector(
                    cfg,
                    new PlatformJmx(),
                    new ConnectClusterView(ctx.clusterState()),
                    () -> JvmProcess.read(Cgroup.ROOT, Path.of("/proc/self/status")),
                    AppInfoParser.getVersion(),
                    LOG::warn,
                    brokerLag);
            Sender sender = new Sender(cfg.reportTo(), cfg.credential(), Duration.ofSeconds(cfg.timeoutSeconds()));
            reporter = new Reporter(() -> collector.collect(Clock.systemUTC().instant()), sender::send, LOG,
                    Clock.systemUTC());
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "turbostats-reporter");
                t.setDaemon(true);
                return t;
            });
            handOffWait = Duration.ofSeconds(Math.min(cfg.timeoutSeconds(), HAND_OFF_WAIT_SECONDS));
            reporter.start(scheduler, cfg.intervalSeconds());
            LOG.info("turbostats: reporting every " + cfg.intervalSeconds() + "s to " + cfg.reportTo()
                    + " as " + cfg.credential() + " for cluster " + cfg.cluster());
        } catch (Throwable t) {
            LOG.warn("turbostats: reporting could not start: " + t.getClass().getSimpleName());
        }
    }

    @Override
    public void close() {
        try {
            if (scheduler != null) {
                handOff();
                scheduler.shutdownNow();
            }
            if (brokerLag != null) {
                brokerLag.close();
            }
        } catch (Throwable ignored) {
            // Shutting down; nothing to report to.
        }
    }

    /**
     * Connect closes its REST extensions before it stops the worker's tasks,
     * so the hand-offs go out while the tasks still run here. They run on
     * the reporter's thread, the only one the collector allows, after any
     * tick in progress.
     */
    private void handOff() {
        Future<?> sent = scheduler.submit(
                () -> reporter.finish(collector.handOff(Clock.systemUTC().instant()), handOffWait));
        try {
            sent.get(handOffWait.toMillis() + 1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // A tick that hangs on the cluster or JMX: the worker stops anyway.
        }
    }

    @Override
    public String version() {
        return ReporterVersion.get();
    }

    /** Connect's isolated loader is org.apache.kafka.connect.runtime.isolation.PluginClassLoader. */
    static boolean loadedFromPluginPath(ClassLoader cl) {
        return cl != null && cl.getClass().getName().endsWith("PluginClassLoader");
    }
}
