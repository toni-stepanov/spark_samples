package consumers;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Properties;

public class Consumer {

    private final static Logger logger = Logger.getLogger(Consumer.class);

    public static void main(String[] args) {
        Consumer consumer1 = new Consumer();
        KafkaConsumer<String, String> consumer = consumer1.get();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
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
