package io.turbolytics.turbostats.connect.collect;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/** The worker's platform MBean server, where Connect and Debezium register. */
public final class PlatformJmx implements Jmx {
    private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

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
}
