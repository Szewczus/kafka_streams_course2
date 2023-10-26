package com.example.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaProducerApplicationTests {

    @Test
    void produceRecordTest() {
        ProducerRecord<String, String> producerRecord = KafkaProducerApplication.produceRecord("Ania");
        String key = producerRecord.key();
        String value = producerRecord.value();
        assertEquals(key, "Ania");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = null;
        try {
            node = mapper.readTree(value);
            assertEquals(node.get("name").asText(), "Ania");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }

}
