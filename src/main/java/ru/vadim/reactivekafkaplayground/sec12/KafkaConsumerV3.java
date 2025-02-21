package ru.vadim.reactivekafkaplayground.sec12;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/*
94. Lesson

 */
public class KafkaConsumerV3 {

    public static final Logger log = LoggerFactory.getLogger(KafkaConsumerV3.class);

    public static void main(String[] args) {
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );
        ReceiverOptions<Object, Object> options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events"));

        KafkaReceiver.create(options)
                .receive()
                .concatMap(KafkaConsumerV3::process)
                .subscribe();
    }

    // делаем отдельный пайплайн

    private static Mono<Void> process(ReceiverRecord<Object, Object> receiverRecord) {
        // распростроняется сигнал ретрая и ошибки тьолько для этого паблишера  Mono.just(receiverRecord)
        return Mono.just(receiverRecord)
                .doOnNext(r -> {
                    if (r.key().toString().equals("5"))
                        throw new RuntimeException("DB is down"); // елси ошибка не индексБаунд - мы не попадем в ретрай, а просто залогируем ошибку и в onErrorResume
                    var index = ThreadLocalRandom.current().nextInt(1, 20); // если падает индексБаунд - мы перейдем в retrySpec, и если в нем мы не сможем повторно обработать, кидаем сигнал фейла до кафки и останавливаем паблишер
                    log.info("key: {}, index: {},  value: {}", r.key(), index, r.value().toString().toCharArray()[index]);
                    r.receiverOffset().acknowledge(); // если без ошибок мы подтверждаем
                })
                .retryWhen(retrySpec()) //В случае ошибки типа `IndexOutOfBoundsException`, применяется стратегия ретрая, которая описана в методе `retrySpec()`.
                .doOnError(ex -> log.info(ex.getMessage()))
                .onErrorResume(IndexOutOfBoundsException.class, e -> Mono.fromRunnable(() -> receiverRecord.receiverOffset().acknowledge())) // - Если выбрасывается ошибка `IndexOutOfBoundsException`, то вместо завершения цепочки, вызывается метод `acknowledge` для подтверждения смещения Kafka (фиксируется, что сообщение обработано).- Это позволяет обработать ошибочную ситуацию без остановки потока сообщений.
                .then();
    }

    private static Retry retrySpec() {
        return Retry.fixedDelay(3, Duration.ofMillis(1))
                .filter(IndexOutOfBoundsException.class::isInstance)
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }
}
