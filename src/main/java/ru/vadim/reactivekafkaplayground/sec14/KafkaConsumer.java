package ru.vadim.reactivekafkaplayground.sec14;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

public class KafkaConsumer {

    public static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String[] args) {
        var consumerConfig = Map.<String, Object> of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092 ",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
//                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerSerializer.class, убрали стандартный сериализатор
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );
        var options = ReceiverOptions.<String, Integer>create(consumerConfig)
                .withValueDeserializer(errorHandlingDeserializer()) // засетили свой инициализатор
                .subscription(List.of("order-events"));

        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();
    }

    // написали свой собственный сериализатор, который вслучае ошибки, например получения стринг вместо интеджер, залогирует и выдаст -10000
    private static ErrorHandlingDeserializer<Integer> errorHandlingDeserializer() {
        var deserializer = new ErrorHandlingDeserializer<>(new IntegerDeserializer());
        deserializer.setFailedDeserializationFunction(info -> {
            log.error("led record {}", new String(info.getData()));
            return -10_000;
        });
        return deserializer;
    }

}
