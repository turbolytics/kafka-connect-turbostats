package io.turbolytics.turbostats.connect.collect;

import java.util.Map;
import java.util.Properties;

/**
 * The admin client's settings, from the worker's: how to reach and
 * authenticate to the broker, then admin.* overrides, as Connect applies
 * them to its own admin clients. Nothing else is copied, so the reporter
 * connects exactly as the worker does and logs no unused settings.
 */
public final class AdminSettings {
    private AdminSettings() {
    }

    public static Properties from(Map<String, ?> worker) {
        Properties p = new Properties();
        worker.forEach((k, v) -> {
            if (v != null && (k.equals("bootstrap.servers") || k.equals("security.protocol")
                    || k.equals("client.dns.lookup") || k.startsWith("sasl.") || k.startsWith("ssl."))) {
                p.put(k, String.valueOf(v));
            }
        });
        worker.forEach((k, v) -> {
            if (v != null && k.startsWith("admin.")) {
                p.put(k.substring("admin.".length()), String.valueOf(v));
            }
        });
        p.put("client.id", "turbostats-reporter");
        return p;
    }
}
