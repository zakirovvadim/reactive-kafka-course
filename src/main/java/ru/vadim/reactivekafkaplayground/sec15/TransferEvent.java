package ru.vadim.reactivekafkaplayground.sec15;

public record TransferEvent(
        String key,
        String from,
        String to,
        String amount,
        Runnable acknowledge
) {
}
