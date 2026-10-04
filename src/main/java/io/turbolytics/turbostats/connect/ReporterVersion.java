package io.turbolytics.turbostats.connect;

/** The reporter's own version, sent as instance.reporter_version. */
public final class ReporterVersion {
    private ReporterVersion() {
    }

    /**
     * The jar manifest's Implementation-Version. Outside a jar there is no
     * manifest, and "dev" says so rather than claiming a release.
     */
    public static String get() {
        String v = ReporterVersion.class.getPackage().getImplementationVersion();
        return v == null ? "dev" : v;
    }
}
