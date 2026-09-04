package com.sublite.subscription.api;

import com.sublite.subscription.api.dto.PurchaseSubscriptionRequest;
import com.sublite.subscription.api.dto.SubscriptionResponse;
import com.sublite.subscription.application.SubscriptionPurchaseService;
import com.sublite.subscription.domain.Subscription;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/subscriptions")
public class SubscriptionController {

    private final SubscriptionPurchaseService service;

    public SubscriptionController(SubscriptionPurchaseService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public SubscriptionResponse purchase(@Valid @RequestBody PurchaseSubscriptionRequest request) {
        Subscription subscription = service.purchase(request.customerId(), request.planPriceId());
        return SubscriptionResponse.from(subscription);
    }
}
