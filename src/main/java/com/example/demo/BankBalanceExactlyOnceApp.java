package com.example.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import javax.management.ObjectName;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;

public class BankBalanceExactlyOnceApp {
    public static void main(String[] args) {
        //create producer properties
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC, "0");


        final Serializer<JsonNode> jsonSerializer = new JsonSerializer<>();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer<>(JsonNode.class);
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        //config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,jsonSerde.getClass());
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> bankTransactions = builder.stream("bank-transactions3", Consumed.with(Serdes.String(), jsonSerde));

        //initial json object
        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey()
                .aggregate(
                        ()->initialBalance,
                        (k, v, b)->newBalance(v, b),
                        Materialized.with(Serdes.String(), jsonSerde)
                );
        bankBalance.toStream().to("bank-balance-exactly-once5", Produced.with(Serdes.String(), jsonSerde));
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams (topology, config);
        streams.cleanUp();
        streams.start();
        System.out.println("Streams: "+streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static JsonNode newBalance(JsonNode transaction, JsonNode balance){
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt()+1);
        newBalance.put("balance", balance.get("balance").asInt()+1);

        //Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        //Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        //Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
//        Date data = new Date();
//        newBalance.put("time", String.valueOf(data.getTime()).toString());
        return newBalance;
    }
}
