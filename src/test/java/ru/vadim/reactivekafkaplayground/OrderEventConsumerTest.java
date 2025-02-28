package ru.vadim.reactivekafkaplayground;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.test.StepVerifier;
import ru.vadim.reactivekafkaplayground.sec16.DummyOrder;
import ru.vadim.reactivekafkaplayground.sec17.producer.OrderEvent;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

@ExtendWith(OutputCaptureExtension.class)
@TestPropertySource(properties = "app=consumer")
public class OrderEventConsumerTest extends AbstractIntegrationTest {

    @Test
    public void consumerTest(CapturedOutput capturedOutput) {
        KafkaSender<String, OrderEvent> sender = createSender();
        var uuid = UUID.randomUUID();
        var orderEvent = new OrderEvent(uuid, 1, LocalDateTime.now());
        var dummyOrder = new DummyOrder(uuid.toString(), "1");
        var sr = toSenderRecord("order-events", "1", orderEvent);

        var mono = sender.send(Mono.just(sr))
                .then(Mono.delay(Duration.ofMillis(500)))
                .then();

        StepVerifier.create(mono)
                .verifyComplete();

        Assertions.assertTrue(capturedOutput.getOut().contains(dummyOrder.toString()));
    }
}
