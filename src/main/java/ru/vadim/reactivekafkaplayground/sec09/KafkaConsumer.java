package ru.vadim.reactivekafkaplayground.sec09;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;
/*
Флакс считывает сообщения по max.poll.records, по умолчанию 500, мы установили 3, каждый батч идет во флакс, и после опустошения переходит к следующей партии
с новым флакс. Поэтому надо использовать concatMap в опшинах потребителя
### Вывод:
1. **`max.poll.records`** влияет на количество сообщений, считываемых за одну итерацию. Установка меньших значений может помочь лучше контролировать нагрузку.
2. Использование **`receiveAutoAcknowledge()`** автоматизирует отправку подтверждений после обработки `Flux`.
3. Уменьшение **commit interval** позволяет потребителю быстрее передавать состояние Kafka Broker, что минимизирует обработку дубликатов при перезапуске Consumer.
4. Эти параметры важны для оптимизации поведения потребителей и обеспечения минимальной обработки повторяющихся сообщений.
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
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3 // устанавливаем max.poll.records - батчи с максимальным количеством
        );
        ReceiverOptions<Object, Object> options = ReceiverOptions.create(consumerConfig)
                .commitInterval(Duration.ofSeconds(1)) // устанавливаем интервал отправки подтверждений в одну секунду как и батчПроцесс для синхронизации, так как он может не совпадать с реальной обработкой
                .subscription(List.of("order-events"));

        KafkaReceiver.create(options)
                .receiveAutoAck()
                .log()
                .concatMap(KafkaConsumer::batchProcess) // используем конкат мап и кладем туда батчи
                .subscribe();
    }

    private static Mono<Void> batchProcess(Flux<ConsumerRecord<Object, Object>> flux) {
        return flux
                .doFirst(() -> log.info("-------"))
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .then(Mono.delay(Duration.ofSeconds(1)))
                .then();

    }

}
