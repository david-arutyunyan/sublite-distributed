package com.sublite.notification.api;

import com.sublite.notification.api.dto.NotificationResponse;
import com.sublite.notification.infrastructure.NotificationRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class NotificationController {

    private final NotificationRepository notifications;

    public NotificationController(NotificationRepository notifications) {
        this.notifications = notifications;
    }

    @GetMapping("/notifications/{customerId}")
    public List<NotificationResponse> byCustomer(@PathVariable String customerId) {
        return notifications.findByCustomerIdOrderByCreatedAtDesc(customerId).stream()
                .map(NotificationResponse::from)
                .toList();
    }
}
