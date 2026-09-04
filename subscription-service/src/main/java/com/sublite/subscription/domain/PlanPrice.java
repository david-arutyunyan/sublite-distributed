package com.sublite.subscription.domain;

import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import java.util.UUID;

@Entity
@Table(name = "plan_prices")
public class PlanPrice {

    @Id
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "plan_id", nullable = false)
    private Plan plan;

    @Enumerated(EnumType.STRING)
    private BillingPeriod billingPeriod;

    @Embedded
    private Money price;

    protected PlanPrice() {
        // JPA
    }

    public PlanPrice(UUID id, Plan plan, BillingPeriod billingPeriod, Money price) {
        this.id = id;
        this.plan = plan;
        this.billingPeriod = billingPeriod;
        this.price = price;
    }

    public UUID getId() {
        return id;
    }

    public Plan getPlan() {
        return plan;
    }

    public BillingPeriod getBillingPeriod() {
        return billingPeriod;
    }

    public Money getPrice() {
        return price;
    }
}
