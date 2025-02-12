package ru.vadim.reactivekafkaplayground.sec07;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

/*
Поиск нужного офссета
 */
public class KafkaConsumer {

    public static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

//    public static void main(String[] args) {
//        Map<String, Object> consumerConfig = Map.of(
//                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
//                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
//                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
//                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
//                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
//                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
//        );
//        ReceiverOptions<Object, Object> options = ReceiverOptions.create(consumerConfig)
//                .addAssignListener(c -> {
//                    c.forEach(p -> log.info("assigned: {}", p.position()));
////                    c.forEach(r -> r.seek(r.position() - 2)); // можно получать сообщения не с саомго первого,а с двигом оффсета на 2, число не должно быть отрицательным
//                    c.stream()
//                            .filter(r -> r.topicPartition().partition() == 2)//1. ищет партицию с конкретным номером `2`.
//                            .findFirst()
//                            .ifPresent(r -> r.seek(r.position() - 2)); // 1. ищет партицию с конкретным номером `2`. Помимо числоа можно указывать время
//                })
//                .subscription(List.of("order-events"));
//
//        KafkaReceiver.create(options)
//                .receive()
//                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
//                .doOnNext(r -> r.receiverOffset().acknowledge())
//                .subscribe();
//
//    }

    public static void main(String[] args) {
        System.out.println(mergeAlternately("abc", "pqr"));
    }
    /**
     * Merges two strings alternately, character by character. If one string is longer than the other,
     **/
    public static String mergeAlternately(String word1, String word2) {
        StringBuilder builder = new StringBuilder();
        if (word1.length() > word2.length()) {
            for (int i = 0; i < word2.length(); i++) {
                if (i == word2.length() - 1) {
                    builder.append(word1.substring(i));
                } else {
                    builder.append(word1.charAt(i));
                    builder.append(word2.charAt(i));
                }
            }
        }
        return builder.toString();
    }

}
