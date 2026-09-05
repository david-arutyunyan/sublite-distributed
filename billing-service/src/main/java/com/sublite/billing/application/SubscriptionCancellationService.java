package com.sublite.billing.application;

import com.sublite.billing.domain.ChargeResult;
import com.sublite.billing.domain.Money;
import com.sublite.billing.domain.OutboxEvent;
import com.sublite.billing.domain.PaymentGateway;
import com.sublite.billing.domain.ProcessedMessage;
import com.sublite.billing.domain.Refund;
import com.sublite.billing.domain.RefundStatus;
import com.sublite.billing.infrastructure.OutboxEventRepository;
import com.sublite.billing.infrastructure.ProcessedMessageRepository;
import com.sublite.billing.infrastructure.RefundRepository;
import io.micrometer.core.instrument.MeterRegistry;
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
 * The cancellation saga's billing-service half - see subscription-service's
 * CancellationService for the full saga shape. This is the step whose
 * outcome decides whether the saga completes (RefundIssued) or triggers a
 * compensating transaction back on subscription-service's side
 * (RefundFailed reverting CANCEL_PENDING to ACTIVE).
 */
@Service
public class SubscriptionCancellationService {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionCancellationService.class);
    static final String REFUND_AGGREGATE = "Refund";

    private final RefundRepository refunds;
    private final OutboxEventRepository outbox;
    private final ProcessedMessageRepository processedMessages;
    private final PaymentGateway gateway;
    private final Clock clock;
    private final MeterRegistry meterRegistry;

    public SubscriptionCancellationService(
            RefundRepository refunds,
            OutboxEventRepository outbox,
            ProcessedMessageRepository processedMessages,
            PaymentGateway gateway,
            Clock clock,
            MeterRegistry meterRegistry
    ) {
        this.refunds = refunds;
        this.outbox = outbox;
        this.processedMessages = processedMessages;
        this.gateway = gateway;
        this.clock = clock;
        this.meterRegistry = meterRegistry;
    }

    @Transactional
    public void refundSubscription(UUID eventId, UUID subscriptionId, BigDecimal amount, String currency, UUID correlationId) {
        if (processedMessages.existsById(eventId)) {
            log.info("Skipping already-processed event: eventId={}, subscriptionId={}", eventId, subscriptionId);
            return;
        }

        Money money = new Money(amount, currency);
        Instant now = Instant.now(clock);

        ChargeResult result = gateway.refund(money);
        RefundStatus status = result instanceof ChargeResult.Success ? RefundStatus.ISSUED : RefundStatus.FAILED;
        String failureReason = result instanceof ChargeResult.Declined declined ? declined.reason() : null;
        Refund refund = refunds.save(new Refund(UUID.randomUUID(), subscriptionId, money, status, failureReason, now));

        outbox.save(new OutboxEvent(
                UUID.randomUUID(),
                REFUND_AGGREGATE,
                subscriptionId,
                status == RefundStatus.ISSUED ? "RefundIssued" : "RefundFailed",
                refundPayload(subscriptionId, refund),
                correlationId,
                now
        ));
        processedMessages.save(new ProcessedMessage(eventId, now));

        log.info("Refund attempted: subscriptionId={}, refundId={}, outcome={}, correlationId={}",
                subscriptionId, refund.getId(), status, correlationId);
        meterRegistry.counter("refunds", "outcome", status == RefundStatus.ISSUED ? "issued" : "failed").increment();
    }

    private static Map<String, Object> refundPayload(UUID subscriptionId, Refund refund) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("subscriptionId", subscriptionId.toString());
        payload.put("refundId", refund.getId().toString());
        payload.put("amount", refund.getAmount().amount());
        payload.put("currency", refund.getAmount().currency());
        if (refund.getStatus() == RefundStatus.FAILED) {
            payload.put("reason", refund.getFailureReason());
        }
        return payload;
    }
}
