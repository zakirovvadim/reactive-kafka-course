package ru.vadim.reactivekafkaplayground.sec15;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Duration;
import java.util.function.Predicate;
/*
**ProducerFencedException**.
1. **Когда возникает ошибка:**
    - Если Kafka-брокер или координатор транзакции обнаруживает два продюсера с одним и тем же **transactionalId** (например, при горизонтальном масштабировании с autoscaler, когда создаётся новая копия продюсера с тем же `transactionalId`).
    - Если происходит таймаут транзакции, например, транзакция инициирована, но не была завершена (`commit` или `abort`) в течение времени, установленного в параметре **transaction timeout** (по умолчанию 60 секунд).

2. **Что означает ошибка:**
    - Координатор помечает текущего продюсера как недействительного, запрещая ему производить сообщения. В результате необходимо создать нового продюсера.

3. **Решение:**
    - Использовать уникальный **transactionalId** для каждого экземпляра продюсера (например, добавление UUID к имени приложения).
    - Следить за тем, чтобы транзакции завершались (commit/abort) в пределах таймаута или увеличить значение таймаута параметром **transaction.timeout.ms**.

4. **Ключевые рекомендации:**
    - Уникальный transactionalId для каждого приложения.
    - Контролировать время выполнения транзакций, чтобы избежать таймаутов.

 */
public class TransferEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(TransferEventProcessor.class);
    private final KafkaSender<String, String> sender;

    public TransferEventProcessor(KafkaSender<String, String> sender) {
        this.sender = sender;
    }

    public Flux<SenderResult<String>> process(Flux<TransferEvent> flux){
        return flux.concatMap(this::validate)
                .concatMap(this::sendTransaction);
    }

    private Mono<SenderResult<String>> sendTransaction(TransferEvent event){
        var senderRecords = this.toSenderRecords(event);
        var manager = this.sender.transactionManager();

//        this.sender.sendTransactionally(Flux<Flux<SenderRecord>>)   если все объекты флакса успешно произведены, он зафиксирует транзакцию, в противном случае он откатится, что-то вроде

        return manager.begin() // ручной контроль транзакций
                .then(this.sender.send(senderRecords)
                        // delaying for demo
                        .concatWith(Mono.delay(Duration.ofSeconds(1)).then(Mono.fromRunnable(event.acknowledge())))
                        .concatWith(manager.commit())
                        .last())
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(ex -> manager.abort());
    }

    // 5 does not have money to transfer
    private Mono<TransferEvent> validate(TransferEvent event){
        return Mono.just(event)
                .filter(Predicate.not(e -> e.key().equals("5")))
                .switchIfEmpty(
                        Mono.<TransferEvent>fromRunnable(event.acknowledge())
                                .doFirst(() -> log.info("fails validation: {}", event.key()))
                );
    }

    private Flux<SenderRecord<String, String, String>> toSenderRecords(TransferEvent event){
        var pr1 = new ProducerRecord<>("transaction-events", event.key(), "%s+%s".formatted(event.to(), event.amount()));
        var pr2 = new ProducerRecord<>("transaction-events", event.key(), "%s-%s".formatted(event.from(), event.amount()));
        var sr1 = SenderRecord.create(pr1, pr1.key());
        var sr2 = SenderRecord.create(pr2, pr2.key());
        return Flux.just(sr1, sr2);
    }
}
