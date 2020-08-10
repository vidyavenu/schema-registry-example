package com.schemaregistryexamples.producer;


import com.schemaregistryexamples.ParkingAddress;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.Properties;

public class ProducerExample {
    private static final String TOPIC = "parking-events";

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) {

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        try (KafkaProducer<String, ParkingAddress> producer = new KafkaProducer<>(props)) {

            for (long i = 0; i < 20; i++) {
                final String parkingID = "parkingid-" + Long.toString(i);

                final ParkingAddress parkingAddress = ParkingAddress.newBuilder().setHumanAddress("Albert st, Melbourne").setStatus("Available").build();
                final ProducerRecord<String, ParkingAddress> record = new ProducerRecord<>(TOPIC, parkingID, parkingAddress);
                producer.send(record);
                Thread.sleep(1000L);
            }

            producer.flush();
            System.out.printf("Successfully produced 3 messages to a topic called %s%n", TOPIC);

        } catch (final SerializationException e) {
            e.printStackTrace();
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }

}


