package com.sublite.subscription.infrastructure;

import com.sublite.subscription.domain.PlanPrice;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface PlanPriceRepository extends JpaRepository<PlanPrice, UUID> {

    // plan is FetchType.LAZY - callers that need plan.getCode()/getName()
    // (the purchase flow does, to put them in the outbox payload) have to
    // ask for it eagerly or hit LazyInitializationException the moment
    // they touch it outside the transaction that loaded this PlanPrice.
    // Same gotcha sublite-core hit for real - see its
    // PlanPriceRepository.findByIdWithPlan().
    @Query("SELECT pp FROM PlanPrice pp JOIN FETCH pp.plan WHERE pp.id = :id")
    Optional<PlanPrice> findByIdWithPlan(@Param("id") UUID id);
}
