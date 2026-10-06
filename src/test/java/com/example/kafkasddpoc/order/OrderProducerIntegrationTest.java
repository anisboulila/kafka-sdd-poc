package com.example.kafkasddpoc.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrderProducerIntegrationTest {

    private static final String TOPIC = "order-events";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void declaresTopicAndPublishesOrderIdKeyedJsonEvent() throws Exception {
        TopicDescription topic = describeTopic();
        assertEquals(1, topic.partitions().size());
        assertEquals(1, topic.partitions().get(0).replicas().size());

        try (KafkaConsumer<String, String> consumer = createConsumer()) {
            TopicPartition partition = new TopicPartition(TOPIC, 0);
            consumer.assign(List.of(partition));
            consumer.seekToEnd(List.of(partition));
            long expectedOffset = consumer.position(partition);

            ResponseEntity<OrderCreated> response = restTemplate.postForEntity(
                    "/orders",
                    new CreateOrderRequest("customer-integration-test", new BigDecimal("24.50")),
                    OrderCreated.class);

            assertEquals(HttpStatus.CREATED, response.getStatusCode());
            OrderCreated responseBody = response.getBody();
            assertNotNull(responseBody);
            assertTrue(responseBody.orderId().matches("[0-9a-fA-F-]{36}"));
            assertEquals("customer-integration-test", responseBody.customerId());
            assertEquals(new BigDecimal("24.50"), responseBody.amount());
            assertEquals(Instant.parse(responseBody.createdAt()).toString(), responseBody.createdAt());

            ConsumerRecord<String, String> record = pollForRecord(consumer);
            assertEquals(expectedOffset, record.offset());
            assertEquals(responseBody.orderId(), record.key());

            JsonNode payload = objectMapper.readTree(record.value());
            assertTrue(payload.isObject());
            assertEquals(responseBody.orderId(), payload.path("orderId").asText());
            assertEquals(responseBody.customerId(), payload.path("customerId").asText());
            assertEquals(0, responseBody.amount().compareTo(payload.path("amount").decimalValue()));
            assertEquals(responseBody.createdAt(), payload.path("createdAt").asText());
        }
    }

    private TopicDescription describeTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS))) {
            return admin.describeTopics(List.of(TOPIC))
                    .values()
                    .get(TOPIC)
                    .get(10, TimeUnit.SECONDS);
        }
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "order-producer-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

    private ConsumerRecord<String, String> pollForRecord(KafkaConsumer<String, String> consumer) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));
            if (!records.isEmpty()) {
                return records.iterator().next();
            }
        }
        throw new AssertionError("No record was published to " + TOPIC);
    }
}
