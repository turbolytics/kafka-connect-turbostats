package io.turbolytics.turbostats.connect.collect;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

/** What the reporter asks the broker about a sink connector's consumer group. */
public interface GroupAdmin extends AutoCloseable {
    /** Each member's client id and the partitions it holds. */
    Map<String, Set<TopicPartition>> assignments(String group, Duration timeout) throws Exception;

    /** The group's committed offsets; a partition with no commit is absent. */
    Map<TopicPartition, Long> committed(String group, Duration timeout) throws Exception;

    /** Each partition's end offset. */
    Map<TopicPartition, Long> ends(Set<TopicPartition> partitions, Duration timeout) throws Exception;

    @Override
    void close();
}
