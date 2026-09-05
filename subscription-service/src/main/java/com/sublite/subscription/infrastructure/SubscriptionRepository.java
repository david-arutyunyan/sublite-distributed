package com.sublite.subscription.infrastructure;

import com.sublite.subscription.domain.Subscription;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface SubscriptionRepository extends JpaRepository<Subscription, UUID> {

    // Same LazyInitializationException gotcha as PlanPriceRepository's own
    // findByIdWithPlan (see its comment) - needed by any caller that
    // returns the Subscription straight to an HTTP response
    // (SubscriptionResponse.from() reads planPrice.getPlan().getCode()),
    // since that read happens after the @Transactional method that
    // loaded it has already returned.
    @Query("SELECT s FROM Subscription s JOIN FETCH s.planPrice pp JOIN FETCH pp.plan WHERE s.id = :id")
    Optional<Subscription> findByIdWithPlan(@Param("id") UUID id);
}
