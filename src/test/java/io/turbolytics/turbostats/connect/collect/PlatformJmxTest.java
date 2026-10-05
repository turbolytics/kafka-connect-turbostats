package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.junit.jupiter.api.Test;

class PlatformJmxTest {
    public interface ProbeMBean {
        int getValue();
    }

    public static final class Probe implements ProbeMBean {
        public int getValue() {
            return 1;
        }
    }

    // A restarted task's old Debezium MBeans outlived its start by 80 s in
    // the spike. Registration time is how the reporter tells them apart.
    @Test
    void recordsWhenAnMBeanRegistered() throws Exception {
        PlatformJmx jmx = new PlatformJmx();
        long before = System.currentTimeMillis();
        ObjectName n = new ObjectName("debezium.test:type=connector-metrics,context=streaming,server=pj");
        ManagementFactory.getPlatformMBeanServer().registerMBean(new StandardMBean(new Probe(), ProbeMBean.class), n);
        try {
            long at = jmx.registeredAt(n);
            assertTrue(at >= before && at <= System.currentTimeMillis(), "registeredAt " + at);
        } finally {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(n);
        }
    }
}
