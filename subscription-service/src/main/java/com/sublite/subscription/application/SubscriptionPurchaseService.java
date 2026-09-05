package com.sublite.subscription.application;

import com.sublite.subscription.domain.OutboxEvent;
import com.sublite.subscription.domain.PlanPrice;
import com.sublite.subscription.domain.PlanPriceNotFoundException;
import com.sublite.subscription.domain.Subscription;
import com.sublite.subscription.infrastructure.OutboxEventRepository;
import com.sublite.subscription.infrastructure.PlanPriceRepository;
import com.sublite.subscription.infrastructure.SubscriptionRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * purchase() is the one method in this whole service where the outbox
 * pattern actually earns its keep: saving the Subscription row and
 * saving the OutboxEvent row happen in the same @Transactional method,
 * so they commit or roll back together as one local Postgres
 * transaction - no distributed transaction across Postgres and Kafka
 * needed, because nothing here talks to Kafka directly at all. That's
 * OutboxPoller's job, running separately, on its own schedule.
 */
@Service
public class SubscriptionPurchaseService {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionPurchaseService.class);
    private static final String SUBSCRIPTION_AGGREGATE = "Subscription";
    private static final String CORRELATION_ID_MDC_KEY = "correlationId";

    private final SubscriptionRepository subscriptions;
    private final PlanPriceRepository planPrices;
    private final OutboxEventRepository outbox;
    private final Clock clock;
    private final MeterRegistry meterRegistry;

    public SubscriptionPurchaseService(
            SubscriptionRepository subscriptions,
            PlanPriceRepository planPrices,
            OutboxEventRepository outbox,
            Clock clock,
            MeterRegistry meterRegistry
    ) {
        this.subscriptions = subscriptions;
        this.planPrices = planPrices;
        this.outbox = outbox;
        this.clock = clock;
        this.meterRegistry = meterRegistry;
    }

    @Transactional
    public Subscription purchase(UUID customerId, UUID planPriceId) {
        PlanPrice planPrice = planPrices.findByIdWithPlan(planPriceId)
                .orElseThrow(() -> new PlanPriceNotFoundException(planPriceId));

        Instant now = Instant.now(clock);
        Subscription subscription = subscriptions.save(new Subscription(UUID.randomUUID(), customerId, planPrice, now));

        UUID correlationId = currentCorrelationId();
        outbox.save(new OutboxEvent(
                UUID.randomUUID(),
                SUBSCRIPTION_AGGREGATE,
                subscription.getId(),
                "SubscriptionCreated",
                subscriptionCreatedPayload(subscription),
                correlationId,
                now
        ));

        log.info("Subscription created: customerId={}, subscriptionId={}, planPriceId={}, correlationId={}",
                customerId, subscription.getId(), planPriceId, correlationId);
        meterRegistry.counter("subscriptions.purchased", "plan", planPrice.getPlan().getCode()).increment();
        return subscription;
    }

    private static Map<String, Object> subscriptionCreatedPayload(Subscription subscription) {
        PlanPrice planPrice = subscription.getPlanPrice();
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("subscriptionId", subscription.getId().toString());
        payload.put("customerId", subscription.getCustomerId().toString());
        payload.put("planCode", planPrice.getPlan().getCode());
        payload.put("planName", planPrice.getPlan().getName());
        payload.put("billingPeriod", planPrice.getBillingPeriod().name());
        payload.put("amount", planPrice.getPrice().amount());
        payload.put("currency", planPrice.getPrice().currency());
        payload.put("status", subscription.getStatus().name());
        payload.put("currentPeriodStart", subscription.getCurrentPeriodStart().toString());
        payload.put("currentPeriodEnd", subscription.getCurrentPeriodEnd().toString());
        return payload;
    }

    /**
     * Falls back to a fresh id rather than failing when nothing set one
     * (CorrelationIdFilter always does for a real HTTP request - this
     * only matters for a direct service-layer call, e.g. from a test).
     */
    private static UUID currentCorrelationId() {
        String existing = MDC.get(CORRELATION_ID_MDC_KEY);
        if (existing != null) {
            try {
                return UUID.fromString(existing);
            } catch (IllegalArgumentException notAUuid) {
                // falls through to a fresh id
            }
        }
        return UUID.randomUUID();
    }
}
