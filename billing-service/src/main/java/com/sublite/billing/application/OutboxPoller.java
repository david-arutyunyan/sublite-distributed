package com.sublite.billing.application;

import com.sublite.billing.domain.OutboxEvent;
import com.sublite.billing.infrastructure.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Same shape as subscription-service's OutboxPoller - see its javadoc
 * for the full reasoning (FOR UPDATE SKIP LOCKED, why per-row failures
 * don't roll back the whole batch, why publishing is safe to retry
 * indefinitely).
 */
@Component
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);
    private static final int BATCH_SIZE = 50;
    private static final String INVOICE_AGGREGATE = "Invoice";
    private static final String REFUND_AGGREGATE = "Refund";

    private final OutboxEventRepository outbox;
    private final KafkaTemplate<String, EventEnvelope> kafka;
    private final Clock clock;

    public OutboxPoller(OutboxEventRepository outbox, KafkaTemplate<String, EventEnvelope> kafka, Clock clock) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${sublite.outbox.poll-interval-ms:1000}")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> batch = outbox.findBatchToPublish(BATCH_SIZE);
        for (OutboxEvent event : batch) {
            publishOne(event);
        }
    }

    private void publishOne(OutboxEvent event) {
        String topic = topicFor(event.getAggregateType());
        EventEnvelope envelope = new EventEnvelope(
                event.getId(),
                event.getEventType(),
                event.getAggregateId(),
                event.getCreatedAt(),
                event.getCorrelationId(),
                event.getPayload()
        );

        try {
            kafka.send(topic, event.getAggregateId().toString(), envelope).get(5, TimeUnit.SECONDS);
            event.markPublished(Instant.now(clock));
            log.info("Outbox event published: eventId={}, eventType={}, topic={}, aggregateId={}",
                    event.getId(), event.getEventType(), topic, event.getAggregateId());
        } catch (Exception e) {
            log.warn("Failed to publish outbox event, will retry on next poll: eventId={}, eventType={}",
                    event.getId(), event.getEventType(), e);
        }
    }

    private static String topicFor(String aggregateType) {
        // Both map to the same topic - a topic belongs to the PUBLISHING
        // domain (billing-service), not to any one aggregate type within
        // it (docs/architecture.md's topic-naming rule).
        if (INVOICE_AGGREGATE.equals(aggregateType) || REFUND_AGGREGATE.equals(aggregateType)) {
            return "billing.events";
        }
        throw new IllegalStateException("No topic mapping for aggregate type: " + aggregateType);
    }
}
