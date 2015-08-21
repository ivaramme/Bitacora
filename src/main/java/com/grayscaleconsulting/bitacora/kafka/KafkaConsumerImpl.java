package com.grayscaleconsulting.bitacora.kafka;

import com.grayscaleconsulting.bitacora.data.DataManager;
import com.grayscaleconsulting.bitacora.data.metadata.KeyValue;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by ivaramme on 6/27/15.
 */
public class KafkaConsumerImpl implements com.grayscaleconsulting.bitacora.kafka.Consumer {
    private final static Logger logger = LoggerFactory.getLogger(KafkaConsumerImpl.class);
    
    private ConsumerConnector consumer;
    private ExecutorService executor;
    private final String topic;
    private final String clientId;
    private final String zookeeperHosts;
    private DataManager dataManager;
    private int partitions;

    public KafkaConsumerImpl(String topic, String clientId, String zookeeperHosts, int partitions) {
        this.topic = topic;
        this.zookeeperHosts = zookeeperHosts;
        this.clientId = clientId;
        this.partitions = partitions;
        
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(configure(clientId, zookeeperHosts));
    }

    public void start() {
        start(partitions);
    }
    
    protected void start(int partitions) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(partitions));
        
        // This returns a map of KafkaStream with size equal to the number of
        // partitions desired for a given topic
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap); 
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        executor = Executors.newFixedThreadPool(partitions);

        // This callback is invoked by the ConsumerTask when it receives a new message
        java.util.function.BiConsumer<String, String> callback = new java.util.function.BiConsumer<String, String>() {
            @Override
            public void accept(String key, String message) {
                logger.info("New message received from log: " + message );
                if(KeyValue.isValidKeyValue(message)) {
                    String[] vals = message.split("\\"+String.valueOf(KeyValue.SEPARATOR));
                    KeyValue value = KeyValue.createKeyValueFromLog(vals[0], vals[1], Long.valueOf(vals[2]), Integer.valueOf(vals[4]), vals[3]);
                    // sets value in local datastore
                    if(null != dataManager) {
                        dataManager.setFromLog(value);
                    }
                } else {
                    dataManager.setFromLog(key, null);
                    logger.info("Invalid message received from log: " + message);
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

/**
 * Created by ivaramme on 6/27/15.
 */
class KafkaConsumerTask implements Runnable{
    private KafkaStream stream;
    private int threadNumber;
    private ConsumerConnector consumer;
    private java.util.function.BiConsumer<String, String> callback;

    public KafkaConsumerTask(ConsumerConnector consumer, KafkaStream stream, int threadNumber, java.util.function.BiConsumer<String, String> callback) {
        this.consumer = consumer;
        this.stream = stream;
        this.threadNumber = threadNumber;
        this.callback = callback;
    }

    public void run(){
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            try {
                MessageAndMetadata<byte[], byte[]> message = it.next();
                String key = IOUtils.toString(new ByteArrayInputStream(message.key()));
                if(message.message() != null) {
                    InputStream is = new ByteArrayInputStream(message.message());
                    callback.accept(key, IOUtils.toString(is));
                } else {
                    callback.accept(key, null);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.commitOffsets(true);
            }
        }

        System.out.println("Shutting down Thread: " + threadNumber);
    }
}
