package com.example.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
public class KafkaProducerApplication {
    private static Logger log = LoggerFactory.getLogger(KafkaProducerApplication.class.getSimpleName());

    public static void main(String[] args) {
        //create producer properties
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");//leverage idempotent producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");//don't push duplicates

        //properties.setProperty("batch.size", "40");

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        int i=0;
        while (true){
            System.out.println("Producing batch i="+i);
            try {
                producer.send(produceRecord("Ania"));
                Thread.sleep(100);
                producer.send(produceRecord("Kasia"));
                Thread.sleep(100);
                producer.send(produceRecord("Szymon"));
                Thread.sleep(100);
                i+=1;
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();
    }

    public static ProducerRecord<String, String> produceRecord(String key) {
        String userData="";
        try {
            userData= produceData(key);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        ProducerRecord <String, String> producerRecord = new ProducerRecord<>("bank-transactions3", key, userData);
        return producerRecord;
    }

    private static String produceData(String key) throws JsonProcessingException {
        Date data = new Date();
        Integer amount = ThreadLocalRandom.current().nextInt(0, 100);
        String time = String.valueOf(data.getTime());
        UserData userData = UserData.builder().name(key).amount(amount).time(time).build();
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(userData);
    }

}
