package com.sublite.notification;

import com.sublite.notification.domain.Notification;
import com.sublite.notification.infrastructure.NotificationRepository;
import com.sublite.notification.infrastructure.SubscriptionProjectionRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
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
@TestPropertySource(properties = {
        "sublite.kafka.retry.max-attempts=2",
        "sublite.kafka.retry.initial-interval-ms=50",
        "sublite.kafka.retry.multiplier=1.0",
        "sublite.kafka.retry.max-interval-ms=50"
})
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

    private KafkaConsumer<String, String> consumer;

    @AfterEach
    void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void malformedMessageGoesStraightToDlqWithoutRetrying() throws Exception {
        UUID key = UUID.randomUUID();
        publish("subscription.events", key, "this is not JSON at all");

        ConsumerRecord<String, String> dlqRecord = consumeRecordWithKey("subscription.events.DLQ", key.toString());
        assertThat(dlqRecord.value()).isEqualTo("this is not JSON at all");
    }

    private ConsumerRecord<String, String> consumeRecordWithKey(String topic, String expectedKey) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));

        long deadline = System.currentTimeMillis() + Duration.ofSeconds(10).toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                if (expectedKey.equals(record.key())) {
                    return record;
                }
            }
        }
        throw new AssertionError("No record with key " + expectedKey + " received on topic " + topic + " within 10s");
    }

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
