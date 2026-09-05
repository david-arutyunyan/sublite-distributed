package com.sublite.subscription.api;

import com.sublite.subscription.api.dto.PurchaseSubscriptionRequest;
import com.sublite.subscription.api.dto.SubscriptionResponse;
import com.sublite.subscription.application.CancellationService;
import com.sublite.subscription.application.SubscriptionPurchaseService;
import com.sublite.subscription.domain.Subscription;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/subscriptions")
public class SubscriptionController {

    private final SubscriptionPurchaseService purchaseService;
    private final CancellationService cancellationService;

    public SubscriptionController(SubscriptionPurchaseService purchaseService, CancellationService cancellationService) {
        this.purchaseService = purchaseService;
        this.cancellationService = cancellationService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public SubscriptionResponse purchase(@Valid @RequestBody PurchaseSubscriptionRequest request) {
        Subscription subscription = purchaseService.purchase(request.customerId(), request.planPriceId());
        return SubscriptionResponse.from(subscription);
    }

    /**
     * 202, not 200/204: this only starts the cancellation saga
     * (ACTIVE -> CANCEL_PENDING) - whether it actually ends in CANCELLED
     * or gets compensated back to ACTIVE depends on billing-service's
     * refund attempt, which hasn't happened yet when this responds.
     */
    @PostMapping("/{id}/cancel")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public SubscriptionResponse cancel(@PathVariable UUID id) {
        Subscription subscription = cancellationService.requestCancellation(id);
        return SubscriptionResponse.from(subscription);
    }
}
