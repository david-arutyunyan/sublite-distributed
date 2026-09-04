package com.sublite.notification.infrastructure;

import com.sublite.notification.domain.SubscriptionProjection;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface SubscriptionProjectionRepository extends MongoRepository<SubscriptionProjection, String> {
}
