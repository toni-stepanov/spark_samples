package kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;

public interface KafkProducer {
    void run(KafkaProducer<String, String> producer) throws InterruptedException;

    KafkaProducer<String, String> initialize();
}
