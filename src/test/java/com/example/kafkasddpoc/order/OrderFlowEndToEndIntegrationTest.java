package com.example.kafkasddpoc.order;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.utils.ContainerTestUtils;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "spring.kafka.consumer.auto-offset-reset=earliest",
            // Same properties as PaymentRetryAndDltIntegrationTest so both share ONE Spring
            // context: a second context would start a second member in the fixed consumer groups
            // and split the single partition between them.
            "app.payment.fail-customer-id=dlt-test-customer"
        })
class OrderFlowEndToEndIntegrationTest {

    private static final String TOPIC = "order-events";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final TopicPartition ORDER_PARTITION = new TopicPartition(TOPIC, 0);

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    @Test
    void createsOrderAndBothConsumerGroupsProcessThePublishedEvent() throws Exception {
        ContainerTestUtils.waitForAssignment(
                listenerRegistry.getListenerContainer("paymentConsumerListener"), 1);
        ContainerTestUtils.waitForAssignment(
                listenerRegistry.getListenerContainer("notificationConsumerListener"), 1);

        KafkaListener paymentListener = PaymentConsumer.class
                .getMethod("consume", OrderCreated.class)
                .getAnnotation(KafkaListener.class);
        KafkaListener notificationListener = NotificationConsumer.class
                .getMethod("consume", OrderCreated.class)
                .getAnnotation(KafkaListener.class);
        assertArrayEquals(new String[]{TOPIC}, paymentListener.topics());
        assertArrayEquals(new String[]{TOPIC}, notificationListener.topics());
        assertEquals("payment-group", paymentListener.groupId());
        assertEquals("notification-group", notificationListener.groupId());
        assertNotEquals(paymentListener.groupId(), notificationListener.groupId());

        Logger paymentLogger = (Logger) LoggerFactory.getLogger(PaymentConsumer.class);
        Logger notificationLogger = (Logger) LoggerFactory.getLogger(NotificationConsumer.class);
        String customerId = "e2e-customer-" + UUID.randomUUID();
        ConsumerLogAppender appender = new ConsumerLogAppender(customerId);
        appender.start();
        paymentLogger.addAppender(appender);
        notificationLogger.addAppender(appender);
        try {
            PublishedOrder published = postOrderAndReadPublishedRecord(customerId);
            OrderCreated record = published.order();
            assertTrue(
                    appender.awaitBoth(record.orderId()),
                    "Both consumer groups should process the event created through the API");

            GroupProgress paymentProgress = awaitGroupProgress("payment-group", published.offset());
            GroupProgress notificationProgress =
                    awaitGroupProgress("notification-group", published.offset());
            assertEquals(0, paymentProgress.lag());
            assertEquals(0, notificationProgress.lag());
            assertTrue(paymentProgress.committedOffset() > published.offset());
            assertTrue(notificationProgress.committedOffset() > published.offset());
        } finally {
            paymentLogger.detachAppender(appender);
            notificationLogger.detachAppender(appender);
            appender.stop();
        }
    }

    private PublishedOrder postOrderAndReadPublishedRecord(String customerId) throws Exception {
        try (KafkaConsumer<String, String> consumer = createRecordConsumer();
                AdminClient admin = createAdminClient()) {
            consumer.assign(List.of(ORDER_PARTITION));
            consumer.seekToEnd(List.of(ORDER_PARTITION));
            long startingOffset = consumer.position(ORDER_PARTITION);

            ResponseEntity<OrderCreated> response = restTemplate.postForEntity(
                    "/orders",
                    new CreateOrderRequest(customerId, new BigDecimal("31.45")),
                    OrderCreated.class);

            assertEquals(HttpStatus.CREATED, response.getStatusCode());
            OrderCreated body = response.getBody();
            assertNotNull(body);
            assertTrue(body.orderId().matches("[0-9a-fA-F-]{36}"));
            assertEquals(customerId, body.customerId());
            assertEquals(new BigDecimal("31.45"), body.amount());
            assertEquals(Instant.parse(body.createdAt()).toString(), body.createdAt());

            ConsumerRecord<String, String> published = pollForRecord(consumer, startingOffset);
            assertEquals(body.orderId(), published.key());
            JsonNode payload = objectMapper.readTree(published.value());
            assertEquals(body.orderId(), payload.path("orderId").asText());
            assertEquals(body.customerId(), payload.path("customerId").asText());
            assertEquals(0, body.amount().compareTo(payload.path("amount").decimalValue()));
            assertEquals(body.createdAt(), payload.path("createdAt").asText());
            return new PublishedOrder(body, published.offset());
        }
    }

    private KafkaConsumer<String, String> createRecordConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "order-flow-e2e-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

    private ConsumerRecord<String, String> pollForRecord(
            KafkaConsumer<String, String> consumer,
            long startingOffset) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));
            for (ConsumerRecord<String, String> record : records) {
                if (record.offset() >= startingOffset) {
                    return record;
                }
            }
        }
        throw new AssertionError("No new record was published to " + TOPIC);
    }

    private GroupProgress awaitGroupProgress(String groupId, long eventOffset) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        try (AdminClient admin = createAdminClient()) {
            while (System.nanoTime() < deadline) {
                Map<TopicPartition, OffsetAndMetadata> offsets =
                        admin.listConsumerGroupOffsets(groupId)
                                .partitionsToOffsetAndMetadata()
                                .get(5, TimeUnit.SECONDS);
                OffsetAndMetadata committed = offsets.get(ORDER_PARTITION);
                if (committed != null && committed.offset() > eventOffset) {
                    long endOffset = admin.listOffsets(Map.of(ORDER_PARTITION, OffsetSpec.latest()))
                            .all()
                            .get(5, TimeUnit.SECONDS)
                            .get(ORDER_PARTITION)
                            .offset();
                    return new GroupProgress(committed.offset(), endOffset - committed.offset());
                }
                Thread.sleep(200);
            }
        }
        throw new AssertionError("Consumer group did not commit past the order event: " + groupId);
    }

    private AdminClient createAdminClient() {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS));
    }

    private record PublishedOrder(OrderCreated order, long offset) {}

    private record GroupProgress(long committedOffset, long lag) {}

    private static final class ConsumerLogAppender extends AppenderBase<ILoggingEvent> {

        private final String customerId;
        private final ConcurrentLinkedQueue<String> messages = new ConcurrentLinkedQueue<>();

        private ConsumerLogAppender(String customerId) {
            this.customerId = customerId;
        }

        @Override
        protected void append(ILoggingEvent event) {
            messages.add(event.getFormattedMessage());
        }

        private boolean awaitBoth(String orderId) throws InterruptedException {
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline) {
                boolean paymentLogged = messages.stream().anyMatch(message ->
                        message.contains("Simulating payment") && message.contains(orderId));
                boolean notificationLogged = messages.stream().anyMatch(message ->
                        message.contains("Simulating notification")
                                && message.contains(orderId)
                                && message.contains(customerId));
                if (paymentLogged && notificationLogged) {
                    return true;
                }
                Thread.sleep(50);
            }
            return false;
        }
    }
}
