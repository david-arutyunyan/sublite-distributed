package com.sublite.subscription;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

// EnableScheduling is what actually activates OutboxPoller's @Scheduled
// method - without it, the annotation is silently inert and no polling
// ever happens.
@SpringBootApplication
@EnableScheduling
public class SubscriptionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(SubscriptionServiceApplication.class, args);
    }
}
