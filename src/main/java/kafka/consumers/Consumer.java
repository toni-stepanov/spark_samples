package kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface Consumer {
    void run(KafkaConsumer<String, String> kafkaConsumer);

    ConsumerRecords<String, String> records(KafkaConsumer<String, String> kafkaConsumer);

    void print(ConsumerRecords<String, String> records);

    KafkaConsumer<String, String> get();
}
