package com.example.kafkasddpoc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.mockito.Mockito.mock;

import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;

// Task 1.1 (evolve-kafka-poc-reliability-and-scaling): read-only inspection.
// It documents what the Kafka client and Spring Kafka ACTUALLY use today, so that
// design.md records verified facts instead of assumptions. It changes no configuration.
// Listeners are not started: the effective settings are readable without a broker.
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
class KafkaEffectiveDefaultsTest {

    @Autowired
    private ProducerFactory<?, ?> producerFactory;

    @Autowired
    private ConsumerFactory<?, ?> consumerFactory;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    @Test
    void producerUsesKafkaClientDefaultsBecauseNothingIsConfigured() {
        // What Spring Boot / our application.properties hands to the Kafka client.
        // Interview point: a Spring property is only an INPUT; if a key is absent here,
        // the Kafka client falls back to its own built-in default.
        Map<String, Object> springLevel = producerFactory.getConfigurationProperties();
        assertFalse(springLevel.containsKey(ProducerConfig.ACKS_CONFIG));
        assertFalse(springLevel.containsKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertFalse(springLevel.containsKey(ProducerConfig.RETRIES_CONFIG));
        assertFalse(springLevel.containsKey(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
        assertFalse(springLevel.containsKey(ProducerConfig.MAX_BLOCK_MS_CONFIG));

        // KafkaProducer parses its settings with ProducerConfig, so building the same
        // object here gives the effective values (defaults applied, idempotence resolved).
        ProducerConfig effective = new ProducerConfig(springLevel);
        System.out.println("EFFECTIVE PRODUCER acks=" + effective.getString(ProducerConfig.ACKS_CONFIG)
                + " enable.idempotence=" + effective.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)
                + " retries=" + effective.getInt(ProducerConfig.RETRIES_CONFIG)
                + " delivery.timeout.ms=" + effective.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)
                + " max.block.ms=" + effective.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG)
                + " request.timeout.ms=" + effective.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)
                + " linger.ms=" + effective.getLong(ProducerConfig.LINGER_MS_CONFIG));
        // The client stores acks=all internally as "-1" (all in-sync replicas).
        assertEquals("-1", effective.getString(ProducerConfig.ACKS_CONFIG));
        assertTrue(effective.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals(Integer.MAX_VALUE, effective.getInt(ProducerConfig.RETRIES_CONFIG));
        assertEquals(120_000, effective.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
        assertEquals(60_000L, effective.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG));
    }

    @Test
    void consumerCommitsAfterTheListenerAndUsesSpringDefaultErrorHandling() {
        // Spring Kafka disables the Kafka client's auto-commit unless it is set explicitly:
        // offsets are committed by the listener container, not on a timer.
        Map<String, Object> springLevel = consumerFactory.getConfigurationProperties();
        assertFalse(springLevel.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertFalse(springLevel.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        // Trap worth knowing: the factory-level value is the raw client default (true).
        // At runtime the listener container overrides enable.auto.commit=false on the real
        // consumers (visible in the "ConsumerConfig values" log of each listener).
        assertTrue(consumerFactory.isAutoCommit());

        AbstractMessageListenerContainer<?, ?> payment = container("paymentConsumerListener");
        AbstractMessageListenerContainer<?, ?> notification = container("notificationConsumerListener");

        // BATCH = offsets of a poll() are committed once every record of that poll was
        // handled by the listener without exception => at-least-once delivery.
        assertEquals(ContainerProperties.AckMode.BATCH, payment.getContainerProperties().getAckMode());
        assertEquals(ContainerProperties.AckMode.BATCH, notification.getContainerProperties().getAckMode());

        // No error handler is set on the container by our application; Spring Kafka
        // then uses a DefaultErrorHandler created at container start.
        assertNull(payment.getCommonErrorHandler());
        assertNull(notification.getCommonErrorHandler());
    }

    @Test
    void defaultErrorHandlerRetriesTenDeliveriesWithoutDelayThenSkipsTheRecord() {
        // Spring's default handler is "new DefaultErrorHandler()". We replay failures of
        // one record against it to count how many deliveries it allows before giving up.
        DefaultErrorHandler handler = new DefaultErrorHandler();
        ConsumerRecord<String, String> record = new ConsumerRecord<>("order-events", 0, 0L, "key", "value");
        Exception failure = new ListenerExecutionFailedException("listener failed", new IllegalStateException("boom"));

        int deliveries = 1;
        // handleOne returns false while the record must be retried (seek back), true once
        // the recoverer (default: only logs the record) has handled it and it is skipped.
        while (!handler.handleOne(failure, record, mock(Consumer.class), mock(MessageListenerContainer.class))) {
            deliveries++;
        }
        System.out.println("DEFAULT ERROR HANDLER deliveries before skip=" + deliveries);
        // Interview point: this is blocking retry, without DLT. After the last attempt
        // the record is only logged and the offset moves on => the message is lost for this group.
        assertEquals(10, deliveries);
    }
    private AbstractMessageListenerContainer<?, ?> container(String listenerId) {
        return (AbstractMessageListenerContainer<?, ?>) listenerRegistry.getListenerContainer(listenerId);
    }
}
