package com.grayscaleconsulting.bitacora.kafka;

import com.grayscaleconsulting.bitacora.data.metadata.KeyValue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
    private final KafkaProducer<String, String> producer;
    
    public KafkaProducerImpl(String brokerList, String topic) {
        this.brokerList = brokerList;
        this.topic = topic;

        producer = new KafkaProducer<String, String>(configure(brokerList));
    }
    
    @Override
    public void publish(KeyValue value) {
        if(null != producer) {
            ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, value.getKey(), value.serialize());
            producer.send(data);
        }
    }

    @Override
    public void shutdown() {
        if(null != producer) {
            producer.close();
        }
    }
    
    @Override
    public void publish(String key, Object value) {
        if(null != producer) {
            ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, key, (String) value);
            producer.send(data);
        }
    }
    
    public Properties configure(String brokerList) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.required.acks", "1");
        
        return props;
    }
    
}
