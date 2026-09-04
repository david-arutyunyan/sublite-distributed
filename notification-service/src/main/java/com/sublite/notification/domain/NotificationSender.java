package com.sublite.notification.domain;

/**
 * A real implementation (email/push/SMS provider) is out of scope for
 * this project - the point of notification-service is the event-driven
 * plumbing, not another external integration. Mirrors billing-service's
 * own PaymentGateway/RandomPaymentGateway split: an interface here, a
 * fake-but-realistic implementation underneath.
 */
public interface NotificationSender {
    void send(String customerId, String message);
}
