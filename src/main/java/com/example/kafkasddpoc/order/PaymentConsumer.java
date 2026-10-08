package com.example.kafkasddpoc.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    // SIMULATION SWITCH, not payment logic: when an event carries this customerId, the listener
    // fails on purpose so that consumer retries and the Dead Letter Topic can be exercised later.
    // Empty (default) means "never fail". The OrderCreated contract and POST /orders stay untouched.
    private final String failCustomerId;

    public PaymentConsumer(@Value("${app.payment.fail-customer-id:}") String failCustomerId) {
        this.failCustomerId = failCustomerId;
    }

    @KafkaListener(
            id = "paymentConsumerListener",
            containerFactory = "paymentKafkaListenerContainerFactory",
            topics = "order-events",
            groupId = "payment-group")
    public void consume(OrderCreated event) {
        // The failure is deterministic: it is thrown on EVERY delivery of a matching event.
        // An exception escaping the listener is what makes Spring Kafka's error handler react
        // (retry, then recover); the offset is not committed past a failing record until then.
        if (!failCustomerId.isBlank() && failCustomerId.equals(event.customerId())) {
            // Logged on every attempt so the retries are visible in the logs (and countable in tests).
            log.warn("Simulated payment failure for orderId={}", event.orderId());
            throw new IllegalStateException(
                    "Simulated payment failure for orderId=" + event.orderId());
        }
        log.info("Simulating payment for orderId={} amount={}", event.orderId(), event.amount());
    }
}
