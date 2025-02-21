package ru.vadim.reactivekafkaplayground.sec13;

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

 */
public class KafkaConsumer {

    public static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

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
                .concatMap(KafkaConsumer::process)
                .subscribe();
    }
    // делаем отдельный пайплайн
    private static Mono<Void> process(ReceiverRecord<Object, Object> receiverRecord) {
        // распростроняется сигнал ретрая и ошибки тьолько для этого паблишера  Mono.just(receiverRecord)
        return Mono.just(receiverRecord)
                .doOnNext(r -> {
                    var index = ThreadLocalRandom.current().nextInt(1, 100);
                    log.info("key: {}, index: {},  value: {}", r.key(), index, r.value().toString().toCharArray()[index]);
                })
                .retryWhen(Retry.fixedDelay(3, Duration.ofMillis(100)).onRetryExhaustedThrow((spec, signal) -> signal.failure()))
                .doFinally(s -> receiverRecord.receiverOffset().acknowledge())
                .onErrorComplete()
                .then();
    }
}
