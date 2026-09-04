package com.sublite.billing.application;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A deliberately manual, operator-triggered replay - not an automatic
 * "consume the DLQ and immediately reprocess" loop. Auto-replaying would
 * either do nothing (the failure that dead-lettered the message in the
 * first place is usually still there a second later) or, worse, create a
 * silent retry storm indistinguishable from the original poison-pill
 * problem this whole mechanism exists to avoid. Replaying is something an
 * operator does after confirming whatever was actually wrong (a bad
 * message shape, a downstream outage) has been fixed.
 *
 * Bounded to a SNAPSHOT of the DLQ topic's end offsets taken at the start
 * of the call - not "keep reading until nothing new shows up". That
 * distinction is not cosmetic: this method's own output (records it
 * republishes onto sourceTopic) can itself immediately fail again and
 * land right back on the SAME DLQ topic it's still reading from, if
 * whatever was actually broken hasn't been fixed. A "stop when a poll
 * comes back empty" loop would keep discovering its own freshly-produced
 * failures and replay THOSE too, forever - an exponential runaway
 * reproduced live while building this (a single still-broken message
 * ballooned into hundreds of DLQ entries in seconds). Snapshotting the
 * end offset up front bounds this call to "whatever was already there",
 * full stop, regardless of what happens as a result of replaying it.
 */
@Service
public class DlqReplayService {

    private static final Logger log = LoggerFactory.getLogger(DlqReplayService.class);
    private static final String REPLAY_GROUP_ID = "billing-service-dlq-replay";

    private final String bootstrapServers;
    private final KafkaTemplate<String, String> dlqKafkaTemplate;

    public DlqReplayService(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            KafkaTemplate<String, String> dlqKafkaTemplate
    ) {
        this.bootstrapServers = bootstrapServers;
        this.dlqKafkaTemplate = dlqKafkaTemplate;
    }

    public int replay(String sourceTopic) {
        String dlqTopic = sourceTopic + ".DLQ";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, REPLAY_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        int replayed = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Manual assign(), not subscribe() - sidesteps consumer-group
            // rebalance/join timing entirely (the root cause of the
            // earlier "returns 0 on the first call" bug: a poll() right
            // after subscribe() can spend its whole timeout just joining
            // the group). Still honors this group's committed offsets -
            // and still resumable across calls via commitSync() below -
            // manual assignment only opts out of Kafka managing WHICH
            // partitions this consumer owns, not offset tracking.
            List<TopicPartition> partitions = consumer.partitionsFor(dlqTopic).stream()
                    .map(p -> new TopicPartition(dlqTopic, p.partition()))
                    .toList();
            consumer.assign(partitions);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            boolean reachedSnapshot;
            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    dlqKafkaTemplate.send(sourceTopic, record.key(), record.value());
                    replayed++;
                }
                reachedSnapshot = partitions.stream()
                        .allMatch(tp -> consumer.position(tp) >= endOffsets.get(tp));
            } while (!reachedSnapshot);

            consumer.commitSync();
        }

        log.info("Replayed {} message(s) from {} back onto {}", replayed, dlqTopic, sourceTopic);
        return replayed;
    }
}
