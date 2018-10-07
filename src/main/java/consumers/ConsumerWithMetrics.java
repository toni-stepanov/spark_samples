package consumers;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Properties;

public class ConsumerWithMetrics {

    private final static Logger logger = Logger.getLogger(ConsumerWithMetrics.class);


    public static void main(String[] args) {
        ConsumerWithMetrics consumerWithMetrics = new ConsumerWithMetrics();
        KafkaConsumer<String, String> consumer = consumerWithMetrics.get();
        consumerWithMetrics.run(consumer);
    }

    private void run(KafkaConsumer<String, String> consumer) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (!records.isEmpty()) {
                consumer.metrics().forEach((k, v) -> logger.info(k + " " + v.value()));
            }
            records.forEach(e ->
                    logger.info("Offset " + e.offset() + " key= " + e.key() + " value= " + e.value()));
        }
    }

    private KafkaConsumer<String, String> get() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("session.timeout", 10000);
        props.put("group.id", "test");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("messages"));
        return consumer;
    }

}
