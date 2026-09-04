package com.sublite.subscription.application;

import com.sublite.subscription.domain.OutboxEvent;
import com.sublite.subscription.infrastructure.OutboxEventRepository;
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
 * Reads unpublished outbox rows, sends each to Kafka, marks it published -
 * the "eventually" half of the outbox pattern (the write side, in
 * SubscriptionPurchaseService, is what makes the row durable in the first
 * place; this just has to keep retrying until every row gets out).
 *
 * One poll = one @Transactional method, which matters for two reasons:
 * findBatchToPublish()'s FOR UPDATE SKIP LOCKED only holds its locks for
 * the life of a transaction, and letting failures for individual events
 * NOT roll back the whole batch (see the catch below) only works because
 * Hibernate's dirty-checking flushes markPublished() calls for the rows
 * that DID succeed at commit, regardless of a later row in the same
 * batch failing.
 */
@Component
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);
    private static final int BATCH_SIZE = 50;
    private static final String SUBSCRIPTION_AGGREGATE = "Subscription";

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
            // .get() with a timeout, not fire-and-forget: this is a
            // background poller, not a request path, so there's no harm
            // in waiting for the broker's ack before deciding whether to
            // mark the row published. If this throws (broker down, etc.),
            // the row is simply left unpublished for the next poll -
            // publishing is safe to retry indefinitely because a consumer
            // that's already seen this eventId is expected to dedup it.
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
        if (SUBSCRIPTION_AGGREGATE.equals(aggregateType)) {
            return "subscription.events";
        }
        throw new IllegalStateException("No topic mapping for aggregate type: " + aggregateType);
    }
}
