package ru.vadim.reactivekafkaplayground.sec12;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
/*
88. Lesson
п. 1 При появлении ошибки потребитель перестает потреблять сообщения
п.2 Таким образом, когда в реактивном конвейере возникает исключение, мы будем выдавать команду отмены для восходящего потока иошибка даже для нисходящего потока.
Итак, когда вы отправляете сигнал отмены для восходящего потока, получатель Kafka думает, что все, вы закончили.
п.3 для обработки ошибки без отсоединения, нужно делать обработку в отдельном конвейре, не в том, который получает сообщения из кафки, и уже там
осуществлять все ретраи и так далее назвисимо от пайплайно идущего непосредственно из кафки
 */
public class KafkaConsumerV1 {

    public static final Logger log = LoggerFactory.getLogger(KafkaConsumerV1.class);

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
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value().toString().toCharArray()[15])) // моделируем ошибку
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .doOnError(e -> log.error("Error: {}", e.getMessage()))
                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1))) // в таком случае мы будем отсоединяться от кафки и подсоединяться снова (если надо запустить в демо варианте, тогда надо вместо сабскрайба запускать блокЛаст) см. пункт 2
                .subscribe();
    }

}
