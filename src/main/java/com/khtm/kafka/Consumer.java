package com.khtm.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {

    private static final String TAG = "Consumer";
    private final Logger mLogger = LoggerFactory.getLogger(TAG);
    private final KafkaConsumer<String, String> mConsumer;
    private final String mBootstrapServer;
    private final String mTopic;

    public Consumer(String server, String topic, String groupId){
        mLogger.info("Creating consumer.");
        this.mBootstrapServer = server;
        this.mTopic = topic;
        this.mConsumer = new KafkaConsumer<String, String>(this.getProperties(mBootstrapServer, groupId));
    }

    private Properties getProperties(String bootstrapServer, String groupId){
        mLogger.info("Properties for making connection between client and broker is created.");
        String deserializer = StringDeserializer.class.getName();
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    private class ConsumerRunable implements Runnable {

        private CountDownLatch mLatch;
        private KafkaConsumer<String, String> mConsumer;

        public ConsumerRunable(String bootstrapServer, String groupId, String topic, CountDownLatch latch){

        }

        public void run() {
            do{

            }while (true);
        }
    }

}
