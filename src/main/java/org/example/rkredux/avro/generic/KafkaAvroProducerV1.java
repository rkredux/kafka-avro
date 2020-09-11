package org.example.rkredux.avro.generic;

import com.example.Customer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class KafkaAvroProducerV1 {

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String schemaRegistryUrl = "http://127.0.0.1:8081";
        String topic = "customer-avro";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "KafkaAvroProducer");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl );

        KafkaProducer<String, Customer> kafkaProducer = new  KafkaProducer<>(properties);

        String key = "aa86773";
        Customer.Builder builder = Customer.newBuilder()
                .setFirstName("Chekker")
                .setLastName("Blake")
                .setAge(34)
                .setHeight(178f)
                .setWeight(87f)
                .setAutomatedEmail(true);
        Customer customer = builder.build();

        ProducerRecord<String, Customer> record = new ProducerRecord<>(topic,key,customer);

        kafkaProducer.send(record, (recordMetadata, e) -> {
            if (e == null) {
                System.out.println(recordMetadata.offset());
            } else e.printStackTrace();
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }

}
