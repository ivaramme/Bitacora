package com.grayscaleconsulting.streaming.kafka;

import com.grayscaleconsulting.streaming.data.metadata.KeyValue;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by ivaramme on 6/27/15.
 */
public class KafkaProducerImpl implements Producer {
    private final static Logger logger = LoggerFactory.getLogger(KafkaProducerImpl.class);
    
    private final String brokerList;
    private final String topic;
    private final kafka.javaapi.producer.Producer<String, String> producer;
    
    public KafkaProducerImpl(String brokerList, String topic) {
        this.brokerList = brokerList;
        this.topic = topic;

        producer = new kafka.javaapi.producer.Producer<String, String>(configure(brokerList));
    }
    
    @Override
    public void publish(KeyValue value) {
        if(null != producer) {
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, value.getKey(), value.serialize());
            producer.send(data);
        }
    }

    @Override
    public void shutdown() {
        if(null != producer) {
            producer.close();
        }
    }
    
    public ProducerConfig configure(String brokerList) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");
        
        return new ProducerConfig(props);
    }
    
}
