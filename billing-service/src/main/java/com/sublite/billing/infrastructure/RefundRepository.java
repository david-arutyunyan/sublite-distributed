package com.sublite.billing.infrastructure;

import com.sublite.billing.domain.Refund;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface RefundRepository extends JpaRepository<Refund, UUID> {
    List<Refund> findBySubscriptionId(UUID subscriptionId);
}
