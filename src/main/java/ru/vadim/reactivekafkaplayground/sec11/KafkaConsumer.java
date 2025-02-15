package ru.vadim.reactivekafkaplayground.sec11;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.List;
import java.util.Map;
/*

Нужно быть осторожным при использовании флатмап. Так как обработка каждого флакса не последовательная, а в перемешку, изза параллельности
Решением проблемы может стать группировка создоваемых флаксов. Однако не стоит делать группировку исходя, например, из номера бизнес сущности
так как из может быть очень много и это создаст много паблишеров. Вместо этого, мы моржем сделать по аналогими с кавкой, когда она
определяет в какую партьицию отправить по мурмур2 хешу.

Вот что будет в логах
15:24:15.114 [reactive-kafka-demo-group-1] INFO  o.a.k.c.c.internals.ConsumerUtils - Setting offset for partition order-events-0 to the committed offset FetchPosition{offset=900, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:9092 (id: 1 rack: null)], epoch=0}}
15:24:15.167 [reactive-kafka-demo-group-1] INFO  r.v.r.sec11.KafkaConsumer - ----------mod: 1
15:24:15.170 [boundedElastic-1] INFO  r.v.r.sec11.KafkaConsumer - key: 1, value: order-1
15:24:15.171 [reactive-kafka-demo-group-1] INFO  r.v.r.sec11.KafkaConsumer - ----------mod: 2
15:24:15.171 [boundedElastic-2] INFO  r.v.r.sec11.KafkaConsumer - key: 2, value: order-2
15:24:15.171 [reactive-kafka-demo-group-1] INFO  r.v.r.sec11.KafkaConsumer - ----------mod: 3
15:24:15.172 [reactive-kafka-demo-group-1] INFO  r.v.r.sec11.KafkaConsumer - ----------mod: 4
15:24:15.173 [boundedElastic-3] INFO  r.v.r.sec11.KafkaConsumer - key: 3, value: order-3
15:24:15.173 [reactive-kafka-demo-group-1] INFO  r.v.r.sec11.KafkaConsumer - ----------mod: 0

Итак, согласно нашей логике, согласно нашей логике, вы берете ключ и делите его на пять.
Если это значение равно нулю, перейдите к этому потоку.
Нулевой поток.
Если значение равно единице, перейдите к единице.
Если значение равно двум, перейдите к двум.
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
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1 // устанавливаем max.poll.records - батчи с максимальным количеством
        );
        var options = ReceiverOptions.<String, String>create(consumerConfig)
                .commitInterval(Duration.ofSeconds(1))
                .subscription(List.of("order-events"));

        KafkaReceiver.create(options)
                .receive()// убираем автоматическое подтверждение
                .groupBy(r -> Integer.parseInt(r.key()) % 5) // используем не флатмап а групбай и считываем ключ (тут просто вариант для демонстрации)
                // также можно использовать группировку на основе партиций r.partition()
                // r.key().hashCode() % 5 - число , например пять, устанавливается в заивисимости от количества потоков или ядер, затем это омжно запустить на тред пуле в параллели
                .flatMap(KafkaConsumer::batchProcess)
                .subscribe();
    }

    private static Mono<Void> batchProcess(GroupedFlux<Integer, ReceiverRecord<String, String>> flux) {
        return flux
                .publishOn(Schedulers.boundedElastic())
                .doFirst(() -> log.info("----------mod: {}", flux.key() ))
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .then(Mono.delay(Duration.ofSeconds(1)))
                .then();

    }

}