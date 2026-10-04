package io.turbolytics.turbostats.connect.collect;

import java.util.List;
import javax.management.ObjectName;

/** The MBeans the reporter reads, behind an interface so tests need no worker. */
public interface Jmx {
    List<ObjectName> query(String pattern);

    /** null when the MBean or the attribute is absent or unreadable. */
    Object attribute(ObjectName name, String attribute);
}
