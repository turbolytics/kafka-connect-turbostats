package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;
import java.util.Map;

/**
 * What the build and the operator say this task is. version, commit, arch
 * and config_hash are required by the contract, so they are written even
 * when empty; everything else is absent when null.
 */
public record Instance(
        String id,
        String name,
        String version,
        String commit,
        String arch,
        String configHash,
        String sourceType,
        String sinkType,
        String runtime,
        String runtimeVersion,
        String reporterVersion,
        Map<String, String> labels) {

    public JsonObject toJson() {
        return new JsonObject()
                .put("id", id)
                .put("name", name)
                .put("version", version == null ? "" : version)
                .put("commit", commit == null ? "" : commit)
                .put("arch", arch == null ? "" : arch)
                .put("config_hash", configHash == null ? "" : configHash)
                .put("source_type", sourceType)
                .put("sink_type", sinkType)
                .put("runtime", runtime)
                .put("runtime_version", runtimeVersion)
                .put("reporter_version", reporterVersion)
                .putMap("labels", labels);
    }
}
