package com.example.kafkasddpoc.order;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootTest(properties = "spring.kafka.consumer.auto-offset-reset=earliest")
class PaymentConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, OrderCreated> kafkaTemplate;

    @Test
    void consumesOrderCreatedAndTriggersSimulatedPayment() throws Exception {
        OrderCreated event = new OrderCreated(
                UUID.randomUUID().toString(),
                "payment-consumer-test",
                new BigDecimal("17.25"),
                Instant.now().toString());
        KafkaListener listener = PaymentConsumer.class
                .getMethod("consume", OrderCreated.class)
                .getAnnotation(KafkaListener.class);
        assertArrayEquals(new String[]{"order-events"}, listener.topics());
        assertEquals("payment-group", listener.groupId());

        Logger consumerLogger = (Logger) LoggerFactory.getLogger(PaymentConsumer.class);
        PaymentLogAppender appender = new PaymentLogAppender(event.orderId());
        appender.start();
        consumerLogger.addAppender(appender);
        try {
            kafkaTemplate.send("order-events", event.orderId(), event).get();
            assertTrue(appender.await(), "Payment Consumer should log the simulated action");
        } finally {
            consumerLogger.detachAppender(appender);
            appender.stop();
        }
    }

    private static final class PaymentLogAppender extends AppenderBase<ILoggingEvent> {

        private final String orderId;
        private final CountDownLatch paymentLogged = new CountDownLatch(1);

        private PaymentLogAppender(String orderId) {
            this.orderId = orderId;
        }

        @Override
        protected void append(ILoggingEvent event) {
            String message = event.getFormattedMessage();
            if (message.contains("Simulating payment") && message.contains(orderId)) {
                paymentLogged.countDown();
            }
        }

        private boolean await() throws InterruptedException {
            return paymentLogged.await(10, TimeUnit.SECONDS);
        }
    }
}
