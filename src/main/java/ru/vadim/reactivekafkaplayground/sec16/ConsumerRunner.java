package ru.vadim.reactivekafkaplayground.sec16;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;

@Service
public class ConsumerRunner implements CommandLineRunner {

    public static final Logger log = org.slf4j.LoggerFactory.getLogger(ConsumerRunner.class);

    @Autowired
    private ReactiveKafkaConsumerTemplate<String, DummyOrder> template;

    @Override
    public void run(String... args) throws Exception {
        this.template.receive()
//                .doOnNext(r -> r.headers().forEach(h -> log.info("header key: {} = {}", h.key(), new String(h.value()))))
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value().customerId()))
                .subscribe();
    }
}
