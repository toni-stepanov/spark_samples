package kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;

public class MultithreadingProducer implements KafkProducer {

    private SimpleKafkaProducer origin;

    public MultithreadingProducer(SimpleKafkaProducer originl) {
        this.origin = originl;
    }

    @Override
    public void run(KafkaProducer<String, String> producer) throws InterruptedException {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(20);
        ScheduledFuture scheduledTask = service.scheduleAtFixedRate(lambda(initialize(),
                new AtomicInteger(0)), 0, 1, TimeUnit.MILLISECONDS);
        service.schedule((Runnable) () -> scheduledTask
                .cancel(true), 6000, SECONDS);
        while (!scheduledTask.isCancelled()) {
            Thread.sleep(10);
        }
        try {
            service.awaitTermination(1, SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        service.shutdown();
    }

    @Override
    public KafkaProducer<String, String> initialize() {
        return origin.initialize();
    }

    private static Runnable lambda(Producer<String, String> producer, AtomicInteger counter) {
        return () -> {
            String result = String.valueOf(counter.getAndIncrement());
            try {
                producer.send(new ProducerRecord<>(
                        "messages",
                        String.valueOf(ThreadLocalRandom.current().nextInt(10)),
                        result));

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        };
    }

}
