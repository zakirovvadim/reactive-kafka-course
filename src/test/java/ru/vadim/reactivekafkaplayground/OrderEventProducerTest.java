package ru.vadim.reactivekafkaplayground;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.test.StepVerifier;
import ru.vadim.reactivekafkaplayground.sec17.producer.OrderEvent;

import java.time.Duration;

@TestPropertySource(properties = "app=producer")
public class OrderEventProducerTest extends AbstractIntegrationTest {

    public static final Logger log = LoggerFactory.getLogger(OrderEventProducerTest.class);

    @Test
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.AFTER_METHOD)  // если запускаем несколько тестов,нужно подчистить контекст
    public void producerTest() {
        KafkaReceiver<String, OrderEvent> receiver = createReceiver("order-events");
        var orderEvents = receiver.receive()
                .take(10)
                .doOnNext(r -> log.info("key: " + r.key() + ", value: " + r.value()));
        StepVerifier.create(orderEvents)
                .consumeNextWith(r -> Assertions.assertNotNull(r.value().orderId()))
                .expectNextCount(9)
                .expectComplete()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.AFTER_METHOD)
    public void producerTest2() {
        KafkaReceiver<String, OrderEvent> receiver = createReceiver("order-events");
        var orderEvents = receiver.receive()
                .take(10)
                .doOnNext(r -> log.info("key: " + r.key() + ", value: " + r.value()));
        StepVerifier.create(orderEvents)
                .consumeNextWith(r -> Assertions.assertNotNull(r.value().orderId()))
                .expectNextCount(9)
                .expectComplete()
                .verify(Duration.ofSeconds(10));
    }
}
