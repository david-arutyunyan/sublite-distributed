package com.sublite.notification.api.dto;

import com.sublite.notification.domain.Notification;

import java.time.Instant;

public record NotificationResponse(
        String subscriptionId,
        String type,
        String message,
        Instant createdAt
) {
    public static NotificationResponse from(Notification notification) {
        return new NotificationResponse(
                notification.getSubscriptionId(),
                notification.getType(),
                notification.getMessage(),
                notification.getCreatedAt()
        );
    }
}
