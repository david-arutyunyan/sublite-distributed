package com.sublite.notification;

import com.sublite.notification.domain.Notification;
import com.sublite.notification.infrastructure.NotificationRepository;
import com.sublite.notification.infrastructure.SubscriptionProjectionRepository;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives this the same way the other two services' own consumer ITs do:
 * real envelopes published onto the real topics, so the @KafkaListener
 * wiring and JSON parsing are genuinely exercised, not just the service
 * layer underneath them.
 */
@SpringBootTest
@Testcontainers
class NotificationConsumptionIT {

    @Container
    @ServiceConnection
    static final MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private NotificationRepository notifications;
    @Autowired
    private SubscriptionProjectionRepository projections;

    @Test
    void subscriptionCreatedThenPaymentSucceededProduceTwoLinkedNotifications() throws Exception {
        UUID subscriptionId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();

        publish("subscription.events", subscriptionId, """
                {
                  "eventId": "%s",
                  "eventType": "SubscriptionCreated",
                  "aggregateId": "%s",
                  "occurredAt": "%s",
                  "correlationId": "%s",
                  "payload": {
                    "subscriptionId": "%s",
                    "customerId": "%s",
                    "planName": "Sublite Plus"
                  }
                }
                """.formatted(UUID.randomUUID(), subscriptionId, Instant.now(), UUID.randomUUID(), subscriptionId, customerId));

        awaitProjectionFor(subscriptionId);
        assertThat(projections.findById(subscriptionId.toString()))
                .hasValueSatisfying(p -> assertThat(p.getCustomerId()).isEqualTo(customerId.toString()));

        publish("billing.events", subscriptionId, """
                {
                  "eventId": "%s",
                  "eventType": "PaymentSucceeded",
                  "aggregateId": "%s",
                  "occurredAt": "%s",
                  "correlationId": "%s",
                  "payload": {
                    "subscriptionId": "%s",
                    "invoiceId": "%s",
                    "amount": 9.99,
                    "currency": "USD"
                  }
                }
                """.formatted(UUID.randomUUID(), subscriptionId, Instant.now(), UUID.randomUUID(), subscriptionId, UUID.randomUUID()));

        awaitNotificationCount(customerId.toString(), 2);

        List<Notification> history = notifications.findByCustomerIdOrderByCreatedAtDesc(customerId.toString());
        assertThat(history).extracting(Notification::getType)
                .containsExactlyInAnyOrder("SUBSCRIPTION_CREATED", "PAYMENT_SUCCEEDED");
        assertThat(history).allSatisfy(n -> assertThat(n.getSubscriptionId()).isEqualTo(subscriptionId.toString()));
    }

    @Test
    void redeliveringTheSameEventIdDoesNotDuplicateTheNotification() throws Exception {
        UUID subscriptionId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String envelope = """
                {
                  "eventId": "%s",
                  "eventType": "SubscriptionCreated",
                  "aggregateId": "%s",
                  "occurredAt": "%s",
                  "correlationId": "%s",
                  "payload": {
                    "subscriptionId": "%s",
                    "customerId": "%s",
                    "planName": "Sublite Plus"
                  }
                }
                """.formatted(eventId, subscriptionId, Instant.now(), UUID.randomUUID(), subscriptionId, customerId);

        publish("subscription.events", subscriptionId, envelope);
        awaitNotificationCount(customerId.toString(), 1);

        publish("subscription.events", subscriptionId, envelope);
        Thread.sleep(2000);

        assertThat(notifications.findByCustomerIdOrderByCreatedAtDesc(customerId.toString())).hasSize(1);
    }

    private void publish(String topic, UUID key, String value) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, key.toString(), value)).get(10, TimeUnit.SECONDS);
        }
    }

    private void awaitProjectionFor(UUID subscriptionId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(15).toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (projections.findById(subscriptionId.toString()).isPresent()) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("No subscription projection appeared for " + subscriptionId + " within 15s");
    }

    private void awaitNotificationCount(String customerId, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(15).toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (notifications.findByCustomerIdOrderByCreatedAtDesc(customerId).size() >= expected) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Notification count for " + customerId + " never reached " + expected + " within 15s");
    }
}
