package com.sublite.billing.application;

import com.sublite.billing.domain.ChargeResult;
import com.sublite.billing.domain.Invoice;
import com.sublite.billing.domain.Money;
import com.sublite.billing.domain.OutboxEvent;
import com.sublite.billing.domain.PaymentAttempt;
import com.sublite.billing.domain.PaymentAttemptStatus;
import com.sublite.billing.domain.PaymentGateway;
import com.sublite.billing.infrastructure.InvoiceRepository;
import com.sublite.billing.infrastructure.OutboxEventRepository;
import com.sublite.billing.infrastructure.PaymentAttemptRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Mirrors SubscriptionPurchaseService.purchase() on the other side of
 * the wire: everything - creating the Invoice, recording the
 * PaymentAttempt, and writing the OutboxEvent that reports the outcome -
 * happens in one local @Transactional method. Same reasoning as before:
 * the gateway call itself isn't part of that transaction (calling an
 * external system inside a DB transaction is its own bad idea - holds a
 * connection open for however long the "network call" takes), but
 * everything that follows the gateway's answer is atomic, so there's no
 * window where a charge outcome exists without a fact about it being
 * queued for publishing.
 */
@Service
public class SubscriptionChargeService {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionChargeService.class);
    private static final String INVOICE_AGGREGATE = "Invoice";

    private final InvoiceRepository invoices;
    private final PaymentAttemptRepository paymentAttempts;
    private final OutboxEventRepository outbox;
    private final PaymentGateway gateway;
    private final Clock clock;

    public SubscriptionChargeService(
            InvoiceRepository invoices,
            PaymentAttemptRepository paymentAttempts,
            OutboxEventRepository outbox,
            PaymentGateway gateway,
            Clock clock
    ) {
        this.invoices = invoices;
        this.paymentAttempts = paymentAttempts;
        this.outbox = outbox;
        this.gateway = gateway;
        this.clock = clock;
    }

    @Transactional
    public void chargeNewSubscription(UUID subscriptionId, BigDecimal amount, String currency, UUID correlationId) {
        // Partial, deliberately-incomplete redelivery guard - see
        // InvoiceRepository.findBySubscriptionId()'s own javadoc for why
        // this isn't the real fix (that's step 5-6, keyed by eventId).
        if (!invoices.findBySubscriptionId(subscriptionId).isEmpty()) {
            log.warn("Subscription {} already has an invoice - skipping, likely a redelivered SubscriptionCreated", subscriptionId);
            return;
        }

        Money money = new Money(amount, currency);
        Instant now = Instant.now(clock);
        Invoice invoice = invoices.save(new Invoice(UUID.randomUUID(), subscriptionId, money, now));

        ChargeResult result = gateway.charge(money);
        PaymentAttempt attempt = recordAttempt(invoice, result, now);

        if (attempt.succeeded()) {
            invoice.markPaid();
        } else {
            invoice.markFailed();
        }

        outbox.save(new OutboxEvent(
                UUID.randomUUID(),
                INVOICE_AGGREGATE,
                subscriptionId,
                attempt.succeeded() ? "PaymentSucceeded" : "PaymentFailed",
                paymentOutcomePayload(subscriptionId, invoice, attempt),
                correlationId,
                now
        ));

        log.info("Charge attempted: subscriptionId={}, invoiceId={}, outcome={}, correlationId={}",
                subscriptionId, invoice.getId(), attempt.getStatus(), correlationId);
    }

    private PaymentAttempt recordAttempt(Invoice invoice, ChargeResult result, Instant now) {
        PaymentAttemptStatus status = switch (result) {
            case ChargeResult.Success ignored -> PaymentAttemptStatus.SUCCEEDED;
            case ChargeResult.Declined ignored -> PaymentAttemptStatus.DECLINED;
        };
        String failureReason = result instanceof ChargeResult.Declined declined ? declined.reason() : null;
        return paymentAttempts.save(new PaymentAttempt(UUID.randomUUID(), invoice.getId(), status, failureReason, now));
    }

    private static Map<String, Object> paymentOutcomePayload(UUID subscriptionId, Invoice invoice, PaymentAttempt attempt) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("subscriptionId", subscriptionId.toString());
        payload.put("invoiceId", invoice.getId().toString());
        payload.put("amount", invoice.getAmount().amount());
        payload.put("currency", invoice.getAmount().currency());
        if (!attempt.succeeded()) {
            payload.put("reason", attempt.getFailureReason());
        }
        return payload;
    }
}
