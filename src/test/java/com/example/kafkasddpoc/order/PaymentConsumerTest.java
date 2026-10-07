package com.example.kafkasddpoc.order;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

// Unit tests of the simulated failure trigger (no Kafka needed): the listener method is a
// plain Java method, so the trigger can be verified without a broker.
class PaymentConsumerTest {

    private static OrderCreated orderOf(String customerId) {
        return new OrderCreated(
                "order-1", customerId, new BigDecimal("10.00"), "2026-10-07T10:00:00Z");
    }

    @Test
    void failsWhenCustomerIdMatchesTheConfiguredTrigger() {
        PaymentConsumer consumer = new PaymentConsumer("customer-fail");

        assertThrows(IllegalStateException.class, () -> consumer.consume(orderOf("customer-fail")));
    }

    @Test
    void succeedsWhenCustomerIdDoesNotMatch() {
        PaymentConsumer consumer = new PaymentConsumer("customer-fail");

        assertDoesNotThrow(() -> consumer.consume(orderOf("customer-ok")));
    }

    @Test
    void neverFailsWhenTriggerIsEmptyByDefault() {
        // An empty trigger must not match anything, not even an empty customerId.
        PaymentConsumer consumer = new PaymentConsumer("");

        assertDoesNotThrow(() -> consumer.consume(orderOf("customer-fail")));
        assertDoesNotThrow(() -> consumer.consume(orderOf("")));
    }
}
