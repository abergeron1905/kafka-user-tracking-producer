package com.kafka.producer;

import com.kafka.producer.model.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Properties;

import static java.lang.Thread.sleep;

@Slf4j
public class Main {
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String LOCALHOST_9093_9094 = "localhost:9093, localhost:9094";

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    private static final String USER_TRACKING = "user-tracking";

    public static void main(String[] args) throws InterruptedException {
        EventGenerator eventGenerator = new EventGenerator();

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS, LOCALHOST_9093_9094);
        props.put(KEY_SERIALIZER, STRING_SERIALIZER);
        props.put(VALUE_SERIALIZER, LOCALHOST_9093_9094);

        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 1; i <= 10; i++) {
            log.info("Generate event #" + i);

            Event event = eventGenerator.generateEvent();

            String key = extractKey(event);
            String value = extractValue(event);

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(USER_TRACKING, key, value);

            log.info("Producing to Kafka the record: " + key + ":" + value);

            producer.send(producerRecord);

            sleep(1000);

        }

        producer.close();
    }


    private static String extractKey (Event event) {
        return event.getUser().getUserId().toString();
    }

    private static String extractValue(Event event) {
        return String.format("%s, %s, %s", event.getProduct().getType(), event.getProduct().getColor(), event.getProduct().getDesignType());
    }
}
