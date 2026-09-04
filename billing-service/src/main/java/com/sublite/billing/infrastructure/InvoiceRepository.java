package com.sublite.billing.infrastructure;

import com.sublite.billing.domain.Invoice;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface InvoiceRepository extends JpaRepository<Invoice, UUID> {

    // Cheap partial guard against the redelivery gap called out in
    // BillingEventListener: not a real dedup mechanism (that's step 5-6,
    // keyed by eventId) - just good enough to stop this step's one and
    // only event type (SubscriptionCreated) from creating two invoices
    // for the same subscription if it's redelivered before then. Would
    // stop being valid the moment recurring renewal events exist, since
    // multiple invoices per subscription becomes normal at that point.
    List<Invoice> findBySubscriptionId(UUID subscriptionId);
}
