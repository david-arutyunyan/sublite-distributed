package com.sublite.subscription.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.UUID;

/**
 * Deliberately no admin CRUD in this rebuild - plans are seeded once via
 * Flyway (see V2__seed_plans.sql). sublite-core's plan/price versioning
 * (valid_period, closeCurrentPrice) isn't ported here: this project's
 * point is Kafka/Saga/K8s, not re-litigating catalog management the
 * monolith already covers. If that gap matters later, it's a single
 * admin controller to add, not a redesign.
 */
@Entity
@Table(name = "plans")
public class Plan {

    @Id
    private UUID id;

    @Column(nullable = false, unique = true, length = 50)
    private String code;

    @Column(nullable = false)
    private String name;

    protected Plan() {
        // JPA
    }

    public Plan(UUID id, String code, String name) {
        this.id = id;
        this.code = code;
        this.name = name;
    }

    public UUID getId() {
        return id;
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
