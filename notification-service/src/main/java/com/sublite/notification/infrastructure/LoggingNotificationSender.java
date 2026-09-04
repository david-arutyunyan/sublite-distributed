package com.sublite.notification.infrastructure;

import com.sublite.notification.domain.NotificationSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class LoggingNotificationSender implements NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(LoggingNotificationSender.class);

    @Override
    public void send(String customerId, String message) {
        log.info("Would send notification to customerId={}: {}", customerId, message);
    }
}
