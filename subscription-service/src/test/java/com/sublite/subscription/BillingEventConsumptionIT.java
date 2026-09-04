package com.sublite.subscription;

import com.sublite.subscription.api.dto.PurchaseSubscriptionRequest;
import com.sublite.subscription.domain.SubscriptionStatus;
import com.sublite.subscription.infrastructure.SubscriptionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The closing half of the loop OutboxPublishingIT exercises from the other
 * side: this drives a subscription all the way from PENDING_PAYMENT to
 * ACTIVE/GRACE_PERIOD by publishing a real PaymentSucceeded/PaymentFailed
 * envelope onto billing.events, so BillingEventListener's @KafkaListener,
 * its JSON parsing, and PaymentOutcomeService are all genuinely exercised -
 * not just Subscription.activate()/enterGracePeriod() directly.
 */
@TestPropertySource(properties = "sublite.outbox.poll-interval-ms=3600000")
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
        String envelope = """
                {
                  "eventId": "%s",
                  "eventType": "%s",
                  "aggregateId": "%s",
                  "occurredAt": "%s",
                  "correlationId": "%s",
                  "payload": %s
                }
                """.formatted(UUID.randomUUID(), eventType, subscriptionId, Instant.now(), UUID.randomUUID(), payloadJson);

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
