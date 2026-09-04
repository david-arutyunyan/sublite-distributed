package com.sublite.subscription;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sublite.subscription.api.dto.PurchaseSubscriptionRequest;
import com.sublite.subscription.application.OutboxPoller;
import com.sublite.subscription.infrastructure.OutboxEventRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The point of this test: prove the outbox pattern actually holds up
 * against a REAL Postgres and a REAL Kafka broker, not mocks - a mocked
 * KafkaTemplate would happily let a test pass even if the envelope shape
 * or the partition key were wrong, since a mock can't tell you what a
 * real consumer would actually see on the wire.
 *
 * Same seeded plan price as the README's curl example
 * (33333333-...-333333333333, see V4__seed_plans.sql) - one less thing
 * to keep in sync between docs and tests.
 */
// The scheduled poller isn't disabled outright (unlike sublite-core's
// BillingScheduler, which has an on/off property) - it stays a normal
// bean here so this test can call publishPending() directly, just with
// its own timer pushed out an hour so it never fires again mid-test and
// races the containers during teardown (real symptom hit here: a wall of
// HikariPool/Kafka reconnect errors as it kept trying against already-
// stopped containers - harmless, but noisy enough to look like a failure).
@TestPropertySource(properties = "sublite.outbox.poll-interval-ms=3600000")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
@Testcontainers
class OutboxPublishingIT {

    private static final UUID SEEDED_PLAN_PRICE_ID = UUID.fromString("33333333-3333-3333-3333-333333333333");

    @Container
    @ServiceConnection
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17");

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    @org.springframework.test.context.DynamicPropertySource
    static void kafkaProperties(org.springframework.test.context.DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private OutboxPoller outboxPoller;
    @Autowired
    private OutboxEventRepository outboxEvents;

    private KafkaConsumer<String, String> consumer;

    @AfterEach
    void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void purchasingASubscriptionEventuallyPublishesSubscriptionCreatedWithTheSubscriptionIdAsKey() throws Exception {
        UUID customerId = UUID.randomUUID();

        String body = mockMvc.perform(post("/subscriptions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new PurchaseSubscriptionRequest(customerId, SEEDED_PLAN_PRICE_ID))))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        UUID subscriptionId = UUID.fromString(objectMapper.readTree(body).get("id").asText());

        // The outbox row exists the instant purchase() returns (same
        // local transaction) - nothing to wait for here, unlike the
        // Kafka side below.
        assertThat(outboxEvents.findAll())
                .anySatisfy(event -> assertThat(event.getAggregateId()).isEqualTo(subscriptionId));

        // Calling the poller directly rather than waiting on its
        // @Scheduled timer: what's actually under test is "does a
        // pending outbox row make it onto the topic with the right key/
        // payload", not "does Spring's scheduling infrastructure fire on
        // time" - the latter is well-trodden framework behavior, not
        // this project's own logic.
        outboxPoller.publishPending();

        ConsumerRecord<String, String> record = consumeOneRecordFrom("subscription.events");

        assertThat(record.key()).isEqualTo(subscriptionId.toString());
        JsonNode envelope = objectMapper.readTree(record.value());
        assertThat(envelope.get("eventType").asText()).isEqualTo("SubscriptionCreated");
        assertThat(envelope.get("aggregateId").asText()).isEqualTo(subscriptionId.toString());
        assertThat(envelope.get("payload").get("customerId").asText()).isEqualTo(customerId.toString());
        assertThat(envelope.get("payload").get("planCode").asText()).isEqualTo("sublite-plus");
        assertThat(envelope.get("payload").get("status").asText()).isEqualTo("PENDING_PAYMENT");
        // Regression check: Jackson's default is to write an Instant as
        // a numeric epoch-seconds timestamp, not the ISO-8601 string
        // docs/architecture.md's envelope promises - isTextual() catches
        // that regressing back to a NUMBER node.
        assertThat(envelope.get("occurredAt").isTextual())
                .as("occurredAt should serialize as an ISO-8601 string, not a numeric timestamp")
                .isTrue();

        assertThat(outboxEvents.findById(UUID.fromString(envelope.get("eventId").asText())))
                .hasValueSatisfying(published -> assertThat(published.getPublishedAt()).isNotNull());
    }

    private ConsumerRecord<String, String> consumeOneRecordFrom(String topic) {
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
            if (!records.isEmpty()) {
                return records.iterator().next();
            }
        }
        throw new AssertionError("No record received on topic " + topic + " within 10s");
    }
}
