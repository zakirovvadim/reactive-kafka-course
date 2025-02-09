package ru.vadim.reactivekafkaplayground.sec05;
// при добавлении или удалении консьюмеров и создании 3 партиций, можно увидеть как партиции переназначаются работающим консьюмерам
public class KafkaConsumerGroup {

    private static class Consumer1 {
        public static void main(String[] args) {
            KafkaConsumer.start("1");
        }
    }
    private static class Consumer2 {
        public static void main(String[] args) {
            KafkaConsumer.start("2");
        }
    }
    private static class Consumer3 {
        public static void main(String[] args) {
            KafkaConsumer.start("3");
        }
    }


}
