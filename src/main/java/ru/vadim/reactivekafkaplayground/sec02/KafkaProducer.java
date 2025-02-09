package ru.vadim.reactivekafkaplayground.sec02;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

public class KafkaProducer {

    public static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    public static void main(String[] args) {
        Map<String, Object> producerConfig = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        var options = SenderOptions.<String, String>create(producerConfig);
       var flux = Flux.interval(Duration.ofMillis(100))
                .take(100)
                .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                .map(producerRecord -> SenderRecord.create(producerRecord, producerRecord.key()));
        var sender = KafkaSender.create(options);
        sender.send(flux)
                .doOnNext(result -> log.info("correlation id: {}", result.correlationMetadata())) //Этот корреляционный ID — это метаданные, связанные с отправленным сообщением (в данном случае это ключ сообщения, переданный в `SenderRecord.create`).
                .doOnComplete(sender::close)
                .subscribe();
    }
}
