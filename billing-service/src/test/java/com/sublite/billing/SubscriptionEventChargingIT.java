package com.sublite.billing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sublite.billing.application.OutboxPoller;
import com.sublite.billing.domain.ChargeResult;
import com.sublite.billing.domain.Money;
import com.sublite.billing.domain.PaymentGateway;
import com.sublite.billing.infrastructure.InvoiceRepository;
import com.sublite.billing.infrastructure.ProcessedMessageRepository;
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
import org.springframework.test.context.bean.override.mockito.MockitoBean;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Drives this the same way a real subscription-service would: publishes
 * an actual SubscriptionCreated envelope onto subscription.events (not a
 * direct method call into SubscriptionChargeService) so
 * SubscriptionEventListener's @KafkaListener, its JSON parsing, and the
 * consumer group wiring in KafkaConsumerConfig are all genuinely
 * exercised - not just the business logic underneath them.
 */
@TestPropertySource(properties = "sublite.outbox.poll-interval-ms=3600000")
@SpringBootTest
@Testcontainers
class SubscriptionEventChargingIT {

    private static final UUID SUBSCRIPTION_ID = UUID.randomUUID();
    private static final UUID CUSTOMER_ID = UUID.randomUUID();

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
    private ObjectMapper objectMapper;
    @Autowired
    private OutboxPoller outboxPoller;
    @Autowired
    private InvoiceRepository invoices;
    @Autowired
    private ProcessedMessageRepository processedMessages;

    @MockitoBean
    private PaymentGateway paymentGateway;

    private KafkaConsumer<String, String> consumer;

    @AfterEach
    void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void aSuccessfulChargePublishesPaymentSucceededKeyedBySubscriptionId() throws Exception {
        when(paymentGateway.charge(any())).thenReturn(new ChargeResult.Success("ref-1"));

        UUID correlationId = UUID.randomUUID();
        publishSubscriptionCreated(SUBSCRIPTION_ID, CUSTOMER_ID, "9.99", "USD", correlationId);

        // Waits for the LISTENER (a background container thread) to have
        // actually processed the message and committed the Invoice -
        // unlike subscription-service's own IT, there's a real
        // asynchronous hop here between "message published" and "side
        // effect visible", so this can't just check immediately.
        awaitInvoiceFor(SUBSCRIPTION_ID);

        assertThat(invoices.findBySubscriptionId(SUBSCRIPTION_ID))
                .singleElement()
                .satisfies(invoice -> {
                    assertThat(invoice.getStatus().name()).isEqualTo("PAID");
                    assertThat(invoice.getAmount()).isEqualTo(new Money(new java.math.BigDecimal("9.99"), "USD"));
                });

        outboxPoller.publishPending();

        ConsumerRecord<String, String> record = consumeRecordWithKey("billing.events", SUBSCRIPTION_ID.toString());
        JsonNode envelope = objectMapper.readTree(record.value());
        assertThat(envelope.get("eventType").asText()).isEqualTo("PaymentSucceeded");
        assertThat(envelope.get("correlationId").asText()).isEqualTo(correlationId.toString());
        assertThat(envelope.get("payload").get("subscriptionId").asText()).isEqualTo(SUBSCRIPTION_ID.toString());
    }

    @Test
    void aDeclinedChargePublishesPaymentFailedWithTheReason() throws Exception {
        when(paymentGateway.charge(any())).thenReturn(new ChargeResult.Declined("INSUFFICIENT_FUNDS"));
        UUID subscriptionId = UUID.randomUUID();

        publishSubscriptionCreated(subscriptionId, CUSTOMER_ID, "9.99", "USD", UUID.randomUUID());
        awaitInvoiceFor(subscriptionId);
        outboxPoller.publishPending();

        ConsumerRecord<String, String> record = consumeRecordWithKey("billing.events", subscriptionId.toString());
        JsonNode envelope = objectMapper.readTree(record.value());
        assertThat(envelope.get("eventType").asText()).isEqualTo("PaymentFailed");
        assertThat(envelope.get("payload").get("reason").asText()).isEqualTo("INSUFFICIENT_FUNDS");
    }

    @Test
    void redeliveringTheSameEventIdDoesNotChargeTwice() throws Exception {
        when(paymentGateway.charge(any())).thenReturn(new ChargeResult.Success("ref-redelivery"));
        UUID subscriptionId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        // Same eventId, published twice - simulates the message being
        // redelivered (a rebalance or a consumer restart before the
        // offset committed), not two different SubscriptionCreated
        // events for the same subscription.
        publishSubscriptionCreated(eventId, subscriptionId, CUSTOMER_ID, "9.99", "USD", UUID.randomUUID());
        awaitInvoiceFor(subscriptionId);
        publishSubscriptionCreated(eventId, subscriptionId, CUSTOMER_ID, "9.99", "USD", UUID.randomUUID());

        // Give the second delivery a moment to actually reach the
        // listener before asserting its absence - otherwise this could
        // pass for the wrong reason (too fast to have been processed
        // yet, not because it was deduped).
        Thread.sleep(2000);

        assertThat(invoices.findBySubscriptionId(subscriptionId)).hasSize(1);
        assertThat(processedMessages.findById(eventId)).isPresent();
    }

    private void publishSubscriptionCreated(UUID subscriptionId, UUID customerId, String amount, String currency, UUID correlationId)
            throws Exception {
        publishSubscriptionCreated(UUID.randomUUID(), subscriptionId, customerId, amount, currency, correlationId);
    }

    private void publishSubscriptionCreated(UUID eventId, UUID subscriptionId, UUID customerId, String amount, String currency, UUID correlationId)
            throws Exception {
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
                    "amount": %s,
                    "currency": "%s"
                  }
                }
                """.formatted(
                eventId, subscriptionId, Instant.now(), correlationId,
                subscriptionId, customerId, amount, currency
        );

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("subscription.events", subscriptionId.toString(), envelope)).get(10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private void awaitInvoiceFor(UUID subscriptionId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(15).toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (!invoices.findBySubscriptionId(subscriptionId).isEmpty()) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("No invoice appeared for subscription " + subscriptionId + " within 15s");
    }

    /**
     * Filters by key rather than just returning the first record: this
     * class's two tests share the same billing.events topic (no topic-
     * per-test isolation), and a fresh from-earliest consumer group sees
     * BOTH tests' messages regardless of which test method is currently
     * running - JUnit doesn't guarantee method execution order, so
     * "just take whatever's first" flaked against whichever test
     * actually ran first. Each test uses its own subscriptionId already;
     * this just makes the assertion robust to that instead of relying on
     * ordering.
     */
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
}
