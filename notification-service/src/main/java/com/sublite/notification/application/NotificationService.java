package com.sublite.notification.application;

import com.sublite.notification.domain.Notification;
import com.sublite.notification.domain.NotificationSender;
import com.sublite.notification.infrastructure.NotificationRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;

/**
 * The idempotent-consumer mechanism here looks different from the two
 * Postgres-backed services' processed_messages table + "check, then act
 * in the same transaction" pattern - and deliberately so. There's no
 * separate business effect to guard here (unlike charging a payment or
 * transitioning a subscription's state): recording the notification IS
 * the entire effect. So instead of a check-then-act pair, this relies on
 * the notification document's own _id being the eventId - Mongo's unique
 * index on _id rejects a second insert for the same eventId outright
 * (DuplicateKeyException), which is a simpler and equally correct way to
 * get the same guarantee an explicit dedup table would give, with one
 * fewer moving part. A genuine platform difference, not an inconsistency.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository notifications;
    private final NotificationSender sender;
    private final Clock clock;
    private final MeterRegistry meterRegistry;

    public NotificationService(NotificationRepository notifications, NotificationSender sender, Clock clock, MeterRegistry meterRegistry) {
        this.notifications = notifications;
        this.sender = sender;
        this.clock = clock;
        this.meterRegistry = meterRegistry;
    }

    public void record(String eventId, String customerId, String subscriptionId, String type, String message) {
        Notification notification = new Notification(eventId, customerId, subscriptionId, type, message, Instant.now(clock));
        try {
            notifications.insert(notification);
        } catch (DuplicateKeyException e) {
            log.info("Skipping already-processed event: eventId={}, type={}", eventId, type);
            return;
        }
        sender.send(customerId, message);
        meterRegistry.counter("notifications.recorded", "type", type).increment();
    }
}
