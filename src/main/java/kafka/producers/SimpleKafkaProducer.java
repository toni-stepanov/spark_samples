package kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleKafkaProducer implements KafkProducer {

    @Override
    public void run(KafkaProducer<String, String> kafkaProducer) {
        for (int num = 0; num < 100; num++) {
            kafkaProducer.send(new ProducerRecord<>("messages", Integer.toString(num),
                    Integer.toString(num)));
        }
        kafkaProducer.close();
    }

    @Override
    public KafkaProducer<String, String> initialize() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("batch.size", 65536);
        props.put("buffer.memory", 10000000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

}
