package io.turbolytics.turbostats.connect.collect;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.management.MBeanServer;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerNotification;
import javax.management.ObjectName;

/** The worker's platform MBean server, where Connect and Debezium register. */
public final class PlatformJmx implements Jmx {
    private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    private final Map<ObjectName, Long> registered = new ConcurrentHashMap<>();
    private final long since = System.currentTimeMillis();

    public PlatformJmx() {
        try {
            server.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, (n, hb) -> {
                if (n instanceof MBeanServerNotification m && m.getMBeanName().getDomain().startsWith("debezium")) {
                    if (MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(m.getType())) {
                        registered.put(m.getMBeanName(), System.currentTimeMillis());
                    } else {
                        registered.remove(m.getMBeanName());
                    }
                }
            }, null, null);
        } catch (Exception ignored) {
            // Without notifications every MBean reads as registered at
            // start: none is ever judged stale, as before this existed.
        }
    }

    @Override
    public List<ObjectName> query(String pattern) {
        try {
            return new ArrayList<>(server.queryNames(new ObjectName(pattern), null));
        } catch (Exception e) {
            return List.of();
        }
    }

    @Override
    public Object attribute(ObjectName name, String attribute) {
        try {
            return server.getAttribute(name, attribute);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public long registeredAt(ObjectName name) {
        return registered.getOrDefault(name, since);
    }
}
