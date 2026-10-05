package io.turbolytics.turbostats.connect.collect;

import java.util.List;
import javax.management.ObjectName;

/** The MBeans the reporter reads, behind an interface so tests need no worker. */
public interface Jmx {
    List<ObjectName> query(String pattern);

    /** null when the MBean or the attribute is absent or unreadable. */
    Object attribute(ObjectName name, String attribute);

    /**
     * When the MBean registered, in epoch milliseconds, or when listening
     * began for one registered before. A Debezium MBean registered before
     * its task's last start belongs to the run before it.
     */
    long registeredAt(ObjectName name);
}
