package com.example.kafkasddpoc.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.utils.ContainerTestUtils;

// Tasks 2.3 and 2.4: Payment retry + Dead Letter Topic, and Notification independence.
// Runs against the real local broker through POST /orders. The Spring properties are identical
// to OrderFlowEndToEndIntegrationTest on purpose, so both tests share one context (see there).
// Test isolation: the DLT is read by a raw consumer in a UNIQUE group, so it never interferes
// with payment-group / notification-group offsets.
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "spring.kafka.consumer.auto-offset-reset=earliest",
            "app.payment.fail-customer-id=dlt-test-customer"
        })
class PaymentRetryAndDltIntegrationTest {

    private static final String FAILING_CUSTOMER = "dlt-test-customer";
    private static final String DLT = "order-events.DLT";
    private static final TopicPartition DLT_PARTITION = new TopicPartition(DLT, 0);

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    @Test
    void failingPaymentIsRetriedThenPublishedToDltWithSameKeyAndPayload() throws Exception {
        waitForBothGroups();
        try (LogCapture logs = LogCapture.begin();
                KafkaConsumer<String, String> dlt = createDltConsumer()) {
            OrderCreated order = postOrder(FAILING_CUSTOMER);

            ConsumerRecord<String, String> dead = awaitDltRecord(dlt, order.orderId());
            assertNotNull(dead, "The exhausted record must be published to " + DLT);

            // Same key and same payload as the original event.
            assertEquals(order.orderId(), dead.key());
            JsonNode payload = objectMapper.readTree(dead.value());
            assertEquals(order.orderId(), payload.path("orderId").asText());
            assertEquals(FAILING_CUSTOMER, payload.path("customerId").asText());
            assertEquals(0, order.amount().compareTo(payload.path("amount").decimalValue()));
            assertEquals(order.createdAt(), payload.path("createdAt").asText());

            // The recoverer adds headers describing where the record came from and why it failed.
            assertEquals("order-events", header(dead, "kafka_dlt-original-topic"));
            assertTrue(header(dead, "kafka_dlt-exception-message").contains("Simulated payment failure"));

            // 1 initial delivery + 3 retries = 4 attempts (FixedBackOff(1000, 3)).
            assertEquals(4, logs.count("Simulated payment failure", order.orderId()));
            // The payment was never simulated successfully for this order.
            assertEquals(0, logs.count("Simulating payment", order.orderId()));
        }
    }

    @Test
    void succeedingPaymentIsNotPublishedToDlt() throws Exception {
        waitForBothGroups();
        try (LogCapture logs = LogCapture.begin();
                KafkaConsumer<String, String> dlt = createDltConsumer()) {
            OrderCreated order = postOrder("normal-customer-" + UUID.randomUUID());

            assertTrue(logs.await("Simulating payment", order.orderId()), "Payment should succeed");
            // Give a wrongly routed record time to appear, then check the DLT stayed empty for it.
            assertNull(awaitDltRecord(dlt, order.orderId(), Duration.ofSeconds(3)));
            assertEquals(0, logs.count("Simulated payment failure", order.orderId()));
        }
    }

    // Task 2.4: separate consumer groups track their own offsets, so Payment's blocking retries
    // (about 3 s on the single partition) must not delay Notification at all.
    @Test
    void notificationKeepsProcessingWhilePaymentRetriesAndAfterDlt() throws Exception {
        waitForBothGroups();
        try (LogCapture logs = LogCapture.begin()) {
            OrderCreated failing = postOrder(FAILING_CUSTOMER);
            OrderCreated healthy = postOrder("healthy-customer-" + UUID.randomUUID());

            // Payment succeeds for the healthy order only after the failing one went to the DLT.
            assertTrue(logs.await("Simulating payment", healthy.orderId()), "Payment must resume");

            assertEquals(1, logs.count("Simulating notification", failing.orderId()));
            assertEquals(1, logs.count("Simulating notification", healthy.orderId()));
            assertEquals(4, logs.count("Simulated payment failure", failing.orderId()));

            // Notification handled BOTH orders before Payment made its 4th (last) failed attempt,
            // i.e. it was never blocked by Payment's retries.
            int fourthFailure = logs.indexOf("Simulated payment failure", failing.orderId(), 4);
            assertTrue(logs.indexOf("Simulating notification", failing.orderId(), 1) < fourthFailure);
            assertTrue(logs.indexOf("Simulating notification", healthy.orderId(), 1) < fourthFailure);
            assertTrue(fourthFailure < logs.indexOf("Simulating payment", healthy.orderId(), 1));
        }
    }

    private static void assertNull(Object value) {
        assertFalse(value != null, "Unexpected record: " + value);
    }

    private void waitForBothGroups() {
        ContainerTestUtils.waitForAssignment(
                listenerRegistry.getListenerContainer("paymentConsumerListener"), 1);
        ContainerTestUtils.waitForAssignment(
                listenerRegistry.getListenerContainer("notificationConsumerListener"), 1);
    }

    private OrderCreated postOrder(String customerId) {
        ResponseEntity<OrderCreated> response = restTemplate.postForEntity(
                "/orders",
                new CreateOrderRequest(customerId, new BigDecimal("12.34")),
                OrderCreated.class);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody());
        return response.getBody();
    }

    // Reads only records published after this point (seekToEnd) so old DLT content is ignored.
    private KafkaConsumer<String, String> createDltConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.assign(List.of(DLT_PARTITION));
        consumer.seekToEnd(List.of(DLT_PARTITION));
        consumer.position(DLT_PARTITION);
        return consumer;
    }

    private ConsumerRecord<String, String> awaitDltRecord(
            KafkaConsumer<String, String> consumer, String orderId) {
        // Retries take about 3 s; allow generous time for the DLT publication.
        return awaitDltRecord(consumer, orderId, Duration.ofSeconds(20));
    }

    private ConsumerRecord<String, String> awaitDltRecord(
            KafkaConsumer<String, String> consumer, String orderId, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));
            for (ConsumerRecord<String, String> record : records) {
                if (orderId.equals(record.key())) {
                    return record;
                }
            }
        }
        return null;
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        assertNotNull(header, "Missing DLT header " + name);
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    // Captures the consumers' log lines to observe attempts without touching consumer code.
    private static final class LogCapture extends AppenderBase<ILoggingEvent> implements AutoCloseable {

        private final ConcurrentLinkedQueue<String> messages = new ConcurrentLinkedQueue<>();
        private final Logger payment = (Logger) LoggerFactory.getLogger(PaymentConsumer.class);
        private final Logger notification = (Logger) LoggerFactory.getLogger(NotificationConsumer.class);

        static LogCapture begin() {
            LogCapture capture = new LogCapture();
            capture.start();
            capture.payment.addAppender(capture);
            capture.notification.addAppender(capture);
            return capture;
        }

        @Override
        protected void append(ILoggingEvent event) {
            messages.add(event.getFormattedMessage());
        }

        long count(String text, String orderId) {
            return messages.stream().filter(m -> m.contains(text) && m.contains(orderId)).count();
        }

        int indexOf(String text, String orderId, int occurrence) {
            int seen = 0;
            int index = 0;
            for (String message : messages) {
                if (message.contains(text) && message.contains(orderId) && ++seen == occurrence) {
                    return index;
                }
                index++;
            }
            return -1;
        }

        boolean await(String text, String orderId) throws InterruptedException {
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline) {
                if (count(text, orderId) > 0) {
                    return true;
                }
                Thread.sleep(50);
            }
            return false;
        }

        @Override
        public void close() {
            payment.detachAppender(this);
            notification.detachAppender(this);
            stop();
        }
    }
}
