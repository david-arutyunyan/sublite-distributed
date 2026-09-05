package com.sublite.subscription;

import com.sublite.subscription.api.dto.PurchaseSubscriptionRequest;
import com.sublite.subscription.application.OutboxPoller;
import com.sublite.subscription.domain.SubscriptionStatus;
import com.sublite.subscription.infrastructure.OutboxEventRepository;
import com.sublite.subscription.infrastructure.ProcessedMessageRepository;
import com.sublite.subscription.infrastructure.SubscriptionRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The closing half of the loop OutboxPublishingIT exercises from the other
 * side: this drives a subscription all the way from PENDING_PAYMENT to
 * ACTIVE/GRACE_PERIOD by publishing a real PaymentSucceeded/PaymentFailed
 * envelope onto billing.events, so BillingEventListener's @KafkaListener,
 * its JSON parsing, and PaymentOutcomeService are all genuinely exercised -
 * not just Subscription.activate()/enterGracePeriod() directly.
 */
@TestPropertySource(properties = {
        "sublite.outbox.poll-interval-ms=3600000",
        "sublite.kafka.retry.max-attempts=2",
        "sublite.kafka.retry.initial-interval-ms=50",
        "sublite.kafka.retry.multiplier=1.0",
        "sublite.kafka.retry.max-interval-ms=50"
})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
@Testcontainers
class BillingEventConsumptionIT {

    private static final UUID SEEDED_PLAN_PRICE_ID = UUID.fromString("33333333-3333-3333-3333-333333333333");

    @Container
    @ServiceConnection
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17");

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private SubscriptionRepository subscriptions;
    @Autowired
    private ProcessedMessageRepository processedMessages;
    @Autowired
    private OutboxEventRepository outboxEvents;
    @Autowired
    private OutboxPoller outboxPoller;

    private KafkaConsumer<String, String> consumer;

    @AfterEach
    void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void malformedMessageGoesStraightToDlqWithoutRetrying() throws Exception {
        String key = UUID.randomUUID().toString();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("billing.events", key, "this is not JSON at all"))
                    .get(10, java.util.concurrent.TimeUnit.SECONDS);
        }

        ConsumerRecord<String, String> dlqRecord = consumeRecordWithKey("billing.events.DLQ", key);
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
    void aPaymentSucceededEventActivatesTheSubscription() throws Exception {
        UUID subscriptionId = purchaseSubscription();

        publishBillingEvent(subscriptionId, "PaymentSucceeded", """
                {
                  "subscriptionId": "%s",
                  "invoiceId": "%s",
                  "amount": 9.99,
                  "currency": "USD"
                }
                """.formatted(subscriptionId, UUID.randomUUID()));

        awaitStatus(subscriptionId, SubscriptionStatus.ACTIVE);
    }

    @Test
    void aPaymentFailedEventPutsTheSubscriptionInGracePeriod() throws Exception {
        UUID subscriptionId = purchaseSubscription();

        publishBillingEvent(subscriptionId, "PaymentFailed", """
                {
                  "subscriptionId": "%s",
                  "invoiceId": "%s",
                  "amount": 9.99,
                  "currency": "USD",
                  "reason": "INSUFFICIENT_FUNDS"
                }
                """.formatted(subscriptionId, UUID.randomUUID()));

        awaitStatus(subscriptionId, SubscriptionStatus.GRACE_PERIOD);
    }

    @Test
    void redeliveringTheSamePaymentSucceededEventIsANoOp() throws Exception {
        UUID subscriptionId = purchaseSubscription();
        UUID eventId = UUID.randomUUID();
        String payload = """
                {
                  "subscriptionId": "%s",
                  "invoiceId": "%s",
                  "amount": 9.99,
                  "currency": "USD"
                }
                """.formatted(subscriptionId, UUID.randomUUID());

        publishBillingEvent(eventId, subscriptionId, "PaymentSucceeded", payload);
        awaitStatus(subscriptionId, SubscriptionStatus.ACTIVE);

        // Redelivery of the SAME eventId - if dedup weren't in place this
        // would still just be a no-op today (activate() only fires from
        // PENDING_PAYMENT), so the real proof this test needs is the
        // processed_messages row, not just the status staying ACTIVE.
        publishBillingEvent(eventId, subscriptionId, "PaymentSucceeded", payload);
        Thread.sleep(2000);

        assertThat(subscriptions.findById(subscriptionId).orElseThrow().getStatus())
                .isEqualTo(SubscriptionStatus.ACTIVE);
        assertThat(processedMessages.findById(eventId)).isPresent();
    }

    @Test
    void cancellingAnActiveSubscriptionPublishesCancellationRequested() throws Exception {
        UUID subscriptionId = purchaseAndActivate();

        mockMvc.perform(post("/subscriptions/" + subscriptionId + "/cancel"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("CANCEL_PENDING"));

        assertThat(outboxEvents.findAll())
                .anySatisfy(event -> {
                    assertThat(event.getAggregateId()).isEqualTo(subscriptionId);
                    assertThat(event.getEventType()).isEqualTo("SubscriptionCancellationRequested");
                });

        outboxPoller.publishPending();
        ConsumerRecord<String, String> record = consumeRecordWithType("subscription.events", subscriptionId.toString(), "SubscriptionCancellationRequested");
        JsonNode payload = objectMapper.readTree(record.value()).get("payload");
        assertThat(payload.get("amount").decimalValue()).isEqualByComparingTo("9.99");
        assertThat(payload.get("currency").asText()).isEqualTo("USD");
    }

    @Test
    void cancellingANonActiveSubscriptionIsRejected() throws Exception {
        // Never activated - still PENDING_PAYMENT.
        UUID subscriptionId = purchaseSubscription();

        mockMvc.perform(post("/subscriptions/" + subscriptionId + "/cancel"))
                .andExpect(status().isConflict());
    }

    @Test
    void aRefundIssuedEventConfirmsTheCancellation() throws Exception {
        UUID subscriptionId = purchaseAndActivate();
        mockMvc.perform(post("/subscriptions/" + subscriptionId + "/cancel")).andExpect(status().isAccepted());
        awaitStatus(subscriptionId, SubscriptionStatus.CANCEL_PENDING);

        publishBillingEvent(subscriptionId, "RefundIssued", """
                {
                  "subscriptionId": "%s",
                  "refundId": "%s",
                  "amount": 9.99,
                  "currency": "USD"
                }
                """.formatted(subscriptionId, UUID.randomUUID()));

        awaitStatus(subscriptionId, SubscriptionStatus.CANCELLED);
    }

    @Test
    void aRefundFailedEventCompensatesByRevertingToActive() throws Exception {
        UUID subscriptionId = purchaseAndActivate();
        mockMvc.perform(post("/subscriptions/" + subscriptionId + "/cancel")).andExpect(status().isAccepted());
        awaitStatus(subscriptionId, SubscriptionStatus.CANCEL_PENDING);

        // The compensating transaction: the refund the saga was counting
        // on didn't happen, so the optimistic ACTIVE -> CANCEL_PENDING
        // transition from the cancel request gets undone. Without this,
        // the subscription would be stuck in CANCEL_PENDING forever -
        // cancelled from the customer's perspective but never actually
        // refunded.
        publishBillingEvent(subscriptionId, "RefundFailed", """
                {
                  "subscriptionId": "%s",
                  "refundId": "%s",
                  "amount": 9.99,
                  "currency": "USD",
                  "reason": "REFUND_PROVIDER_ERROR"
                }
                """.formatted(subscriptionId, UUID.randomUUID()));

        awaitStatus(subscriptionId, SubscriptionStatus.ACTIVE);
    }

    @Test
    void aSecondConflictingRefundFailedDoesNotRepublishTheCompensation() throws Exception {
        UUID subscriptionId = purchaseAndActivate();
        mockMvc.perform(post("/subscriptions/" + subscriptionId + "/cancel")).andExpect(status().isAccepted());
        awaitStatus(subscriptionId, SubscriptionStatus.CANCEL_PENDING);

        publishBillingEvent(subscriptionId, "RefundFailed", """
                {"subscriptionId": "%s", "refundId": "%s", "amount": 9.99, "currency": "USD", "reason": "FIRST_FAILURE"}
                """.formatted(subscriptionId, UUID.randomUUID()));
        awaitStatus(subscriptionId, SubscriptionStatus.ACTIVE);

        // A second, DIFFERENT RefundFailed for the same subscription -
        // its own distinct eventId, so eventId-dedup correctly treats it
        // as a new event, not a redelivery. Reproduces a real bug caught
        // live: two racing RefundFailed responses (a replayed one and
        // billing-service's own real one) each got processed, and each
        // published its OWN SubscriptionCancellationFailed event, even
        // though the second one's abortCancellation() was a pure no-op
        // (already ACTIVE) - because the service layer published the
        // event and logged success unconditionally, without checking
        // whether the entity-level transition actually happened.
        publishBillingEvent(subscriptionId, "RefundFailed", """
                {"subscriptionId": "%s", "refundId": "%s", "amount": 9.99, "currency": "USD", "reason": "SECOND_FAILURE"}
                """.formatted(subscriptionId, UUID.randomUUID()));
        Thread.sleep(2000);

        assertThat(subscriptions.findById(subscriptionId).orElseThrow().getStatus()).isEqualTo(SubscriptionStatus.ACTIVE);
        long cancellationFailedCount = outboxEvents.findAll().stream()
                .filter(event -> event.getAggregateId().equals(subscriptionId)
                        && "SubscriptionCancellationFailed".equals(event.getEventType()))
                .count();
        assertThat(cancellationFailedCount).isEqualTo(1);
    }

    private UUID purchaseAndActivate() throws Exception {
        UUID subscriptionId = purchaseSubscription();
        publishBillingEvent(subscriptionId, "PaymentSucceeded", """
                {
                  "subscriptionId": "%s",
                  "invoiceId": "%s",
                  "amount": 9.99,
                  "currency": "USD"
                }
                """.formatted(subscriptionId, UUID.randomUUID()));
        awaitStatus(subscriptionId, SubscriptionStatus.ACTIVE);
        return subscriptionId;
    }

    /**
     * Like consumeRecordWithKey, but also filters by eventType - needed
     * once a subscriptionId has more than one event type on the same
     * topic (SubscriptionCreated AND SubscriptionCancellationRequested
     * share both the topic and the key).
     */
    private ConsumerRecord<String, String> consumeRecordWithType(String topic, String expectedKey, String expectedEventType) {
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
                if (!expectedKey.equals(record.key())) {
                    continue;
                }
                try {
                    if (expectedEventType.equals(objectMapper.readTree(record.value()).get("eventType").asText())) {
                        return record;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new AssertionError("No " + expectedEventType + " record with key " + expectedKey + " received on topic " + topic + " within 10s");
    }

    private UUID purchaseSubscription() throws Exception {
        UUID customerId = UUID.randomUUID();
        String body = mockMvc.perform(post("/subscriptions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new PurchaseSubscriptionRequest(customerId, SEEDED_PLAN_PRICE_ID))))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return UUID.fromString(objectMapper.readTree(body).get("id").asText());
    }

    private void publishBillingEvent(UUID subscriptionId, String eventType, String payloadJson) throws Exception {
        publishBillingEvent(UUID.randomUUID(), subscriptionId, eventType, payloadJson);
    }

    private void publishBillingEvent(UUID eventId, UUID subscriptionId, String eventType, String payloadJson) throws Exception {
        String envelope = """
                {
                  "eventId": "%s",
                  "eventType": "%s",
                  "aggregateId": "%s",
                  "occurredAt": "%s",
                  "correlationId": "%s",
                  "payload": %s
                }
                """.formatted(eventId, eventType, subscriptionId, Instant.now(), UUID.randomUUID(), payloadJson);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("billing.events", subscriptionId.toString(), envelope))
                    .get(10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private void awaitStatus(UUID subscriptionId, SubscriptionStatus expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(15).toMillis();
        while (System.currentTimeMillis() < deadline) {
            SubscriptionStatus current = subscriptions.findById(subscriptionId)
                    .orElseThrow()
                    .getStatus();
            if (current == expected) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Subscription " + subscriptionId + " never reached " + expected + " within 15s");
    }
}
