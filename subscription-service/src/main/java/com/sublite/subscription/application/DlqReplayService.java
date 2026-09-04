package com.sublite.subscription.application;

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
 * Same shape and reasoning as billing-service's own DlqReplayService -
 * manual, operator-triggered, bounded to a snapshot of the DLQ's end
 * offsets taken at the start of the call (see its javadoc for why: a
 * "keep polling until nothing new shows up" loop can runaway when the
 * replayed message is still broken and immediately lands back on the
 * same DLQ it's reading from).
 */
@Service
public class DlqReplayService {

    private static final Logger log = LoggerFactory.getLogger(DlqReplayService.class);
    private static final String REPLAY_GROUP_ID = "subscription-service-dlq-replay";

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
