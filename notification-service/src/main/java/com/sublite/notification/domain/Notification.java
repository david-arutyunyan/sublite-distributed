package com.sublite.notification.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * One row of notification history - "what we would have told the
 * customer", not a real send (see NotificationSender). The document's
 * own _id IS the source event's eventId, stored as a String rather than
 * a native UUID - a deliberate idempotency choice, see NotificationService's
 * javadoc for why that's enough on its own, no separate processed_messages
 * collection needed here the way the two Postgres-backed services have one.
 */
@Document(collection = "notifications")
public class Notification {

    @Id
    private String eventId;

    private String customerId;
    private String subscriptionId;
    private String type;
    private String message;
    private Instant createdAt;

    protected Notification() {
        // Spring Data
    }

    public Notification(String eventId, String customerId, String subscriptionId, String type, String message, Instant createdAt) {
        this.eventId = eventId;
        this.customerId = customerId;
        this.subscriptionId = subscriptionId;
        this.type = type;
        this.message = message;
        this.createdAt = createdAt;
    }

    public String getEventId() {
        return eventId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public String getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
