package com.khtm.kafka;

import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class FunctionalTest {


    @Test
    public void ProducerTest() throws ExecutionException, InterruptedException {
        String serverIPAddress = "10.12.47.125:9092";
        String topic = "user_registry";
        Producer producer = new Producer(serverIPAddress);
        producer.put(topic, "user-01", "alireza.khtm@gmail.com");
        producer.put(topic, "user-02", "morteza.mosavi@gmail.com");
        producer.close();
    }


}
