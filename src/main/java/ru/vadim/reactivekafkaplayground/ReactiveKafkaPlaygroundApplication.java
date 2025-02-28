package ru.vadim.reactivekafkaplayground;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "ru.vadim.reactivekafkaplayground.sec17.${app}")
public class ReactiveKafkaPlaygroundApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReactiveKafkaPlaygroundApplication.class, args);
    }

}
