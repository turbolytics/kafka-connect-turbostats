package io.turbolytics.turbostats.connect.collect;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

/** GroupAdmin over Kafka's admin client, every call bounded by its timeout. */
public final class KafkaGroupAdmin implements GroupAdmin {
    private final Admin admin;

    public KafkaGroupAdmin(Properties settings) {
        this.admin = Admin.create(settings);
    }

    @Override
    public Map<String, Set<TopicPartition>> assignments(String group, Duration t) throws Exception {
        ConsumerGroupDescription d = admin.describeConsumerGroups(List.of(group)).all()
                .get(t.toMillis(), TimeUnit.MILLISECONDS).get(group);
        Map<String, Set<TopicPartition>> out = new HashMap<>();
        if (d == null) {
            return out;
        }
        for (MemberDescription m : d.members()) {
            out.computeIfAbsent(m.clientId(), x -> new HashSet<>()).addAll(m.assignment().topicPartitions());
        }
        return out;
    }

    @Override
    public Map<TopicPartition, Long> committed(String group, Duration t) throws Exception {
        Map<TopicPartition, Long> out = new HashMap<>();
        admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(t.toMillis(), TimeUnit.MILLISECONDS)
                .forEach((tp, om) -> {
                    if (om != null) {
                        out.put(tp, om.offset());
                    }
                });
        return out;
    }

    @Override
    public Map<TopicPartition, Long> ends(Set<TopicPartition> tps, Duration t) throws Exception {
        Map<TopicPartition, OffsetSpec> spec = new HashMap<>();
        tps.forEach(tp -> spec.put(tp, OffsetSpec.latest()));
        Map<TopicPartition, Long> out = new HashMap<>();
        admin.listOffsets(spec).all().get(t.toMillis(), TimeUnit.MILLISECONDS)
                .forEach((tp, r) -> out.put(tp, r.offset()));
        return out;
    }

    @Override
    public void close() {
        admin.close(Duration.ofSeconds(5));
    }
}
