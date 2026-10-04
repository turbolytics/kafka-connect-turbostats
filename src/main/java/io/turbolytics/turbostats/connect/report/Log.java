package io.turbolytics.turbostats.connect.report;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The three levels the reporter uses, behind an interface so tests can count them. */
public interface Log {
    void info(String message);

    void warn(String message);

    void debug(String message);

    static Log slf4j(Class<?> owner) {
        Logger l = LoggerFactory.getLogger(owner);
        return new Log() {
            @Override
            public void info(String m) {
                l.info(m);
            }

            @Override
            public void warn(String m) {
                l.warn(m);
            }

            @Override
            public void debug(String m) {
                l.debug(m);
            }
        };
    }
}
