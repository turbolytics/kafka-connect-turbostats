package io.turbolytics.turbostats.connect;

import io.turbolytics.turbostats.connect.collect.Cgroup;
import io.turbolytics.turbostats.connect.collect.ConnectClusterView;
import io.turbolytics.turbostats.connect.collect.ConnectMetrics;
import io.turbolytics.turbostats.connect.collect.JvmProcess;
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
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

/**
 * The entry point a worker names in rest.extension.classes.
 *
 * Every method catches everything. An exception from configure or register
 * stops the worker's REST server, and the monitoring jar must never be why
 * a worker or its connectors are down. A worker shutting down sends no exit
 * bundles: its tasks move to another worker, which reports them under the
 * same instance ids.
 */
public final class TurboStatsExtension implements ConnectRestExtension {
    private static final Log LOG = Log.slf4j(TurboStatsExtension.class);

    private ReporterConfig.Parsed parsed;
    private ScheduledExecutorService scheduler;

    @Override
    public void configure(Map<String, ?> configs) {
        try {
            if (loadedFromPluginPath(getClass().getClassLoader())) {
                LOG.warn("turbostats: this jar was loaded from the plugin path. Move it to the worker's classpath "
                        + "(/kafka/libs in the Debezium image): from the plugin path the interceptors cannot load, "
                        + "and every source task on this worker fails to build its producer");
            }
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
            TaskCollector collector = new TaskCollector(
                    cfg,
                    new ConnectMetrics(new PlatformJmx()),
                    new ConnectClusterView(ctx.clusterState()),
                    () -> JvmProcess.read(Cgroup.ROOT, Path.of("/proc/self/status")),
                    AppInfoParser.getVersion(),
                    LOG::warn);
            Sender sender = new Sender(cfg.reportTo(), cfg.credential(), Duration.ofSeconds(cfg.timeoutSeconds()));
            Reporter reporter = new Reporter(() -> collector.collect(Clock.systemUTC().instant()), sender::send, LOG,
                    Clock.systemUTC());
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "turbostats-reporter");
                t.setDaemon(true);
                return t;
            });
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
                scheduler.shutdownNow();
            }
        } catch (Throwable ignored) {
            // Shutting down; nothing to report to.
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
