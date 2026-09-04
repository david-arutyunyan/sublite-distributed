package com.sublite.notification.infrastructure;

import com.sublite.notification.domain.Notification;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface NotificationRepository extends MongoRepository<Notification, String> {
    List<Notification> findByCustomerIdOrderByCreatedAtDesc(String customerId);
}
