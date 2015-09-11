package com.grayscaleconsulting.bitacora.kafka;

import com.grayscaleconsulting.bitacora.model.KeyValue;
import com.grayscaleconsulting.bitacora.metrics.Metrics;
import com.grayscaleconsulting.bitacora.util.Utils;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
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
    
    private Timer publishingLatency = Metrics.getDefault().newTimer(KafkaProducerImpl.class, "publishing-latency");
    
    private final String brokerList;
    private final String topic;
    private KafkaProducer<String, byte[]> producer;
    
    public KafkaProducerImpl(String brokerList, String topic) {
        this.brokerList = brokerList;
        this.topic = topic;
    }
    
    @Override
    public void start() {
        producer = new KafkaProducer<String, byte[]>(configure(brokerList));
    }

    @Override
    public void publish(KeyValue value) {
        if(null != producer) {

            byte[] data = Utils.convertToAvro(value);
            if(null != data) {
                final TimerContext context = publishingLatency.time();
                ProducerRecord<String, byte[]> message = new ProducerRecord<String, byte[]>(topic, value.getKey(), data);
                producer.send(message);
                context.stop();
            } else {
                logger.error("Unable to send this message: {}", value);
            }
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
            byte[] payload = null;
            if(null != value && value instanceof KeyValue) {
                payload = Utils.convertToAvro((KeyValue) value);
            }

            if(null != payload) {
                final TimerContext context = publishingLatency.time();
                ProducerRecord<String, byte[]> message = new ProducerRecord<String, byte[]>(topic, key, payload);
                producer.send(message);
                context.stop();
            } else {
                logger.error("Unable to send this message: {}", value);
            }
        }
    }

    public Properties configure(String brokerList) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("request.required.acks", "1");

        return props;
    }
    
}
