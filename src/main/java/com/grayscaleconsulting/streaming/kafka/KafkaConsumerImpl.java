package com.grayscaleconsulting.streaming.kafka;

import com.grayscaleconsulting.streaming.data.DataManager;
import com.grayscaleconsulting.streaming.data.metadata.KeyValue;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by ivaramme on 6/27/15.
 */
public class KafkaConsumerImpl implements com.grayscaleconsulting.streaming.kafka.Consumer {
    private final static Logger logger = LoggerFactory.getLogger(KafkaConsumerImpl.class);
    
    private ConsumerConnector consumer;
    private ExecutorService executor;
    private final String topic;
    private final String clientId;
    private final String zookeeperHosts;
    private DataManager dataManager;

    public KafkaConsumerImpl(String topic, String clientId, String zookeeperHosts) {
        this.topic = topic;
        this.zookeeperHosts = zookeeperHosts;
        this.clientId = clientId;
        
        
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(configure(clientId, zookeeperHosts));
    }
    
    public void start(int partitions) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(partitions));
        
        // This returns a map of KafkaStream with size equal to the number of
        // partitions desired for a given topic
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap); 
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        executor = Executors.newFixedThreadPool(partitions);

        // This callback is invoked by the ConsumerTask when it receives a new message
        java.util.function.Consumer<String> callback = new java.util.function.Consumer<String>() {
            @Override
            public void accept(String s) {
                if(KeyValue.isValidKeyValue(s)) {
                    logger.info("New message received from log: " + s );
                    String[] vals = s.split("\\"+String.valueOf(KeyValue.SEPARATOR));
                    KeyValue value = KeyValue.createKeyValueFromLog(vals[0], vals[1], Long.valueOf(vals[2]), Integer.valueOf(vals[3]));
                    // sets value in local datastore
                    if(null != dataManager) {
                        dataManager.setFromLog(value);
                    }
                } else {
                    logger.info("Invalid message received from log: " + s);
                }
            }
        };

        // Create a task for each stream created based on the number of partitions
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new KafkaConsumerTask(consumer, stream, threadNumber, callback));
            threadNumber++;
        }
        
    }
    
    public void setDataManager(DataManager dataManager) {
        this.dataManager = dataManager;
    }
    
    @Override
    public void shutdown() {
        if(null != consumer) consumer.shutdown();
        if(null != executor) executor.shutdown();
    }

    private static ConsumerConfig configure(String clientId, String zookeeperHosts) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeperHosts);
        props.put("group.id", clientId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.commit.enable", "false");
        return new ConsumerConfig(props);
    }
}
