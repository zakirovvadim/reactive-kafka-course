package ru.vadim.reactivekafkaplayground.sec15;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

public class TransferEventProcessor {

    public static final Logger log = LoggerFactory.getLogger(TransferEventConsumer.class);
    private final KafkaSender<String, String> sender;

    public TransferEventProcessor(KafkaSender<String, String> sender) {
        this.sender = sender;
    }

    public void process(Flux<TransferEvent> flux) {

    }

    // key 3 does not have money to transfer
    private Mono<TransferEvent> validate(TransferEvent transferEvent) {
        return Mono.just(transferEvent)
                .filter(e -> e.key().equals("5"))
                .switchIfEmpty(
                        Mono.<TransferEvent>fromRunnable(transferEvent.acknowledge())
                                .doFirst(() -> log.info("fails validation: {}", transferEvent.key()))
                );
    }

    private Flux<SenderRecord<String, String, String>> toSenderRecords(TransferEvent event) {
        var pr1 = new ProducerRecord<>("transaction-events", event.key(), "%s+%s".formatted(event.to(), event.amount()));
        var pr2 = new ProducerRecord<>("transaction-events", event.key(), "%s-%s".formatted(event.from(), event.amount()));
        var sr1 = SenderRecord.create(pr1, pr1.key());
        var sr2 = SenderRecord.create(pr2, pr2.key());
        return Flux.just(sr1, sr2);
    }
}
