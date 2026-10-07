package com.example.kafkasddpoc.order;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.ContainerTestUtils;

@SpringBootTest(properties = "spring.kafka.consumer.auto-offset-reset=earliest")
class NotificationConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, OrderCreated> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    @Test
    void notificationAndPaymentGroupsConsumeTheSameEventIndependently() throws Exception {
        ContainerTestUtils.waitForAssignment(
                listenerRegistry.getListenerContainer("notificationConsumerListener"),
                1);

        OrderCreated event = new OrderCreated(
                UUID.randomUUID().toString(),
                "notification-consumer-test",
                new BigDecimal("17.25"),
                Instant.now().toString());

        KafkaListener notificationListener = NotificationConsumer.class
                .getMethod("consume", OrderCreated.class)
                .getAnnotation(KafkaListener.class);
        KafkaListener paymentListener = PaymentConsumer.class
                .getMethod("consume", OrderCreated.class)
                .getAnnotation(KafkaListener.class);
        assertArrayEquals(new String[]{"order-events"}, notificationListener.topics());
        assertEquals("notification-group", notificationListener.groupId());
        assertEquals("payment-group", paymentListener.groupId());
        assertNotEquals(notificationListener.groupId(), paymentListener.groupId());

        Logger notificationLogger = (Logger) LoggerFactory.getLogger(NotificationConsumer.class);
        Logger paymentLogger = (Logger) LoggerFactory.getLogger(PaymentConsumer.class);
        ConsumerLogAppender appender = new ConsumerLogAppender(event.orderId(), event.customerId());
        appender.start();
        notificationLogger.addAppender(appender);
        paymentLogger.addAppender(appender);
        try {
            kafkaTemplate.send("order-events", event.orderId(), event).get();
            assertTrue(
                    appender.awaitBoth(),
                    "Both consumer groups should process the published event independently");
            assertTrue(appender.notificationLogged.get());
            assertTrue(appender.paymentLogged.get());
        } finally {
            notificationLogger.detachAppender(appender);
            paymentLogger.detachAppender(appender);
            appender.stop();
        }
    }

    private static final class ConsumerLogAppender extends AppenderBase<ILoggingEvent> {

        private final String orderId;
        private final String customerId;
        private final CountDownLatch bothConsumersLogged = new CountDownLatch(2);
        private final AtomicBoolean notificationLogged = new AtomicBoolean();
        private final AtomicBoolean paymentLogged = new AtomicBoolean();

        private ConsumerLogAppender(String orderId, String customerId) {
            this.orderId = orderId;
            this.customerId = customerId;
        }

        @Override
        protected void append(ILoggingEvent event) {
            String message = event.getFormattedMessage();
            if (message.contains(orderId)
                    && message.contains("Simulating notification")
                    && message.contains(customerId)
                    && notificationLogged.compareAndSet(false, true)) {
                bothConsumersLogged.countDown();
            } else if (message.contains(orderId)
                    && message.contains("Simulating payment")
                    && paymentLogged.compareAndSet(false, true)) {
                bothConsumersLogged.countDown();
            }
        }

        private boolean awaitBoth() throws InterruptedException {
            return bothConsumersLogged.await(10, TimeUnit.SECONDS);
        }
    }
}
