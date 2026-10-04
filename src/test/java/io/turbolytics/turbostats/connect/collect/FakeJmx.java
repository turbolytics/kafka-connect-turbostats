package io.turbolytics.turbostats.connect.collect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/** A map of MBeans, for tests that need Connect's metrics without a worker. */
public final class FakeJmx implements Jmx {
    private final Map<ObjectName, Map<String, Object>> beans = new HashMap<>();

    public FakeJmx put(String objectName, String attribute, Object value) {
        try {
            beans.computeIfAbsent(new ObjectName(objectName), n -> new HashMap<>()).put(attribute, value);
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException(e);
        }
        return this;
    }

    public FakeJmx remove(String objectName) {
        try {
            beans.remove(new ObjectName(objectName));
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException(e);
        }
        return this;
    }

    @Override
    public List<ObjectName> query(String pattern) {
        List<ObjectName> out = new ArrayList<>();
        try {
            ObjectName p = new ObjectName(pattern);
            for (ObjectName n : beans.keySet()) {
                if (p.apply(n)) {
                    out.add(n);
                }
            }
        } catch (MalformedObjectNameException e) {
            return List.of();
        }
        return out;
    }

    @Override
    public Object attribute(ObjectName name, String attribute) {
        Map<String, Object> attrs = beans.get(name);
        return attrs == null ? null : attrs.get(attribute);
    }
}
