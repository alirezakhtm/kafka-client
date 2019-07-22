package com.khtm.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    private final String mBootstrapServer;
    private final KafkaProducer<String, String> mProducer;

    private static final String TAG = "Producer";
    private final Logger mLogger = LoggerFactory.getLogger(TAG);

    public Producer(String bootstrapServer){
        mLogger.info("Producer is creating.");
        this.mBootstrapServer = bootstrapServer;
        this.mProducer = new KafkaProducer<String, String>(this.getProperties(bootstrapServer));
    }

    private Properties getProperties(String bootstrapServer){
        String serializer = StringSerializer.class.getName();
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
        return properties;
    }

    public void put(String topic, String key, String value) throws ExecutionException, InterruptedException {
        mLogger.info("topic: " + topic + ", key: " + key + ", value: " + value);
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
        this.mProducer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e != null){
                    mLogger.error("Putting data into kafka has been gotten error while processing.");
                    return;
                }
                mLogger.info(String.format("topic: %s\nHas Offset: %s\nTimestamp: %s\nOffset: %s\nPartition: %s\nKey Size: %s\n" +
                                "Value Size: %s\nTimestamp: %s",
                        recordMetadata.topic(), recordMetadata.hasOffset(),
                        recordMetadata.hasTimestamp(), recordMetadata.offset(), recordMetadata.partition(),
                        recordMetadata.serializedKeySize(), recordMetadata.serializedValueSize(),recordMetadata.timestamp()));
            }
        }).get();
    }

    public void close(){
        mLogger.info("Closing producer connection.");
        mProducer.close();
    }
}
