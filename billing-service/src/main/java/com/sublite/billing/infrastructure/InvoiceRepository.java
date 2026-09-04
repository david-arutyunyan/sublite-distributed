package com.sublite.billing.infrastructure;

import com.sublite.billing.domain.Invoice;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface InvoiceRepository extends JpaRepository<Invoice, UUID> {

    // No longer a redelivery guard as of step 5-6 - that's
    // ProcessedMessageRepository's job now. Kept for lookups (tests,
    // and eventually a "billing history for this subscription" read
    // path).
    List<Invoice> findBySubscriptionId(UUID subscriptionId);
}
