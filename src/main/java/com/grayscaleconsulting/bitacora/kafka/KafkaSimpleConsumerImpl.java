package com.grayscaleconsulting.bitacora.kafka;

import com.grayscaleconsulting.bitacora.data.DataManager;
import com.grayscaleconsulting.bitacora.data.metadata.KeyValue;
import com.grayscaleconsulting.bitacora.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of a Kafka Simple Consumer Based off: https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example
 * 
 * <p>This consumer keeps track of its own read offset using zookeeper.</p>
 * 
 * Created by ivaramme on 8/24/15.
 */
public class KafkaSimpleConsumerImpl implements Consumer, Runnable {
    public static final int MAX_ERRORS = 5;
    public static final int PAUSE = 500;

    private static Logger logger = LoggerFactory.getLogger(KafkaSimpleConsumerImpl.class);
    private static final String ZK_PARENT_NODE = "/custom-consumers-offset";
    private static final int ZK_ACTION_OK = 0;
    private static final int ZK_ACTION_RETRY = 1;

    private String topic;
    private int partition;
    private String leadBroker;
    private String clientName;
    private boolean read = true;
    private List<String> brokers;
    private int port;
    private String groupId;
    private kafka.javaapi.consumer.SimpleConsumer consumer;
    private long currentOffset;
    private boolean ready = false;
    
    private DataManager dataManager;

    private ZooKeeper zookeeper;
    private String zkConsumerNodeName;
    private String zookeperHosts;

    private final Counter totalMessagesProcessed = 
            Metrics.getDefault().newCounter(KafkaSimpleConsumerImpl.class, "kafka-total-messages-processed");
    private final Counter totalValidMessagesProcessed =
            Metrics.getDefault().newCounter(KafkaSimpleConsumerImpl.class, "kafka-total-valid-messages-processed");
    private final Counter countOffsetUpdates =
            Metrics.getDefault().newCounter(KafkaSimpleConsumerImpl.class, "kafka-count-offset-updates");
    private final Counter countErrors =
            Metrics.getDefault().newCounter(KafkaSimpleConsumerImpl.class, "kafka-count-errors");

    public KafkaSimpleConsumerImpl(String groupId, List<String> brokers, int port, String topic, int partition, String zookeperHosts) {
        checkArgument(!brokers.isEmpty(), "Kafka brokers cannot be empty.");
        
        this.groupId = groupId;
        this.brokers = brokers;
        this.port = port;
        this.topic = topic;
        this.partition = partition;
        this.zookeperHosts = zookeperHosts;
    }

    @Override
    public void setDataManager(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    @Override
    public void start() {
        PartitionMetadata metadata = fetchTopicMetadata(brokers, port, topic, partition);
        if (metadata == null) {
            logger.error("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        if (metadata.leader() == null) {
            logger.error("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        
        leadBroker = metadata.leader().host();
        clientName = groupId;
        zkConsumerNodeName = ZK_PARENT_NODE + "/" + clientName;

        try {
            initializeZookeeper(zookeperHosts);
        } catch (IOException e) {
            logger.error("Unable to start zookeeper");
            e.printStackTrace();
            throw new RuntimeException("Unable to start zookeeper");
        }

        new Thread(this).start();
    }

    private int zkRetries = 0;
    private void initializeZookeeper(String zookeeperHosts) throws IOException {
        zookeeper = new ZooKeeper(zookeeperHosts, 2500, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // Takes care of the global events for this session
                logger.info("Zookeeper event: " + event);
                if (event.getState().equals(Event.KeeperState.Disconnected)) {
                    logger.error("Disconnected from Zookeeper cluster");
                    if(zkRetries < MAX_ERRORS) {
                        try {
                            initializeZookeeper(zookeeperHosts);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        zkRetries++;
                    } else {
                        logger.error("Unable to reconnect to zookeeper");
                        throw new RuntimeException("Unable to reconnect to zookeeper");
                    }
                } else if (event.getState().equals(Event.KeeperState.Expired)) {
                    logger.error("Session expired by Zookeeper");
                    if(zkRetries < MAX_ERRORS) {
                        try {
                            initializeZookeeper(zookeeperHosts); // retry
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        zkRetries++;
                    } else {
                        logger.error("Unable to create a new session in zookeeper, going down");
                        throw new RuntimeException("Unable to create a new session in zookeeper, going down");
                    }
                } else if (event.getState().equals(Event.KeeperState.SyncConnected)) {
                    logger.info("Created a new Zookeeper session successfully.");
                    // Create nodes to record this consumer's offset
                    createConsumerOffsetNodes();
                    zkRetries = 0; // reset counter
                    ready = true;
                }
            }
        });
    }

    public void run() {
        consumer = new kafka.javaapi.consumer.SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
        long readOffset = getConsumerOffset();//getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);

        logger.info("Starting topic read from: " + readOffset);

        int processedMessages = 0;
        int numErrors = 0;
        while (read) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
            }

            kafka.api.FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(topic, partition, readOffset, 100000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);

            // Error found while fetching messages, try to see if there's a new leader
            if (fetchResponse.hasError()) {
                countErrors.inc();
                numErrors++;
                short code = fetchResponse.errorCode(topic, partition);
                logger.error("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);

                if (numErrors > MAX_ERRORS) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    // Reload offset
                    readOffset = getConsumerOffset();//getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }

                consumer.close();
                consumer = null;
                try {
                    leadBroker = findNewLeader(leadBroker, topic, partition, port);
                    continue;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            numErrors = 0;

            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    logger.error("Mismatch while finding offsets. Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();

                // Read message
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);

                try {
                    String message = new String(bytes, "UTF-8");
                    logger.info(String.valueOf(messageAndOffset.offset()) + ": " + message);
                    currentOffset = readOffset;
                    
                    if(KeyValue.isValidKeyValue(message)) {
                        String[] vals = message.split("\\"+String.valueOf(KeyValue.SEPARATOR));
                        KeyValue value = KeyValue.createKeyValueFromLog(vals[0], vals[1], Long.valueOf(vals[2]), Integer.valueOf(vals[4]), vals[3]);
                        // sets value in local datastore
                        if(null != dataManager) {
                            dataManager.setFromLog(value);
                        }

                        totalValidMessagesProcessed.inc();
                    }
                    
                    // Persist to Zookeeper only every 10 messages
                    if ((processedMessages % 10) == 0) {
                        persistOffset(currentOffset, consumer);
                        countOffsetUpdates.inc();
                    }
                } catch (UnsupportedEncodingException e) {
                    logger.error("Error while parsing message: " + e);
                    e.printStackTrace();
                }
                numRead++;
                processedMessages++;

                totalMessagesProcessed.inc();
            }

            // No more messages to read, rest
            if (numRead == 0) {
                try {
                    Thread.sleep(PAUSE);
                } catch (InterruptedException ie) {
                }
            }
        }

        if (consumer != null) consumer.close();
    }
    
    public void persistOffset(long readOffset, SimpleConsumer consumer) {
        logger.info("Update offset to: {}", readOffset);
        int retries = 0;

        while (retries < MAX_ERRORS) {
            if (updateZKNode(zkConsumerNodeName, ByteBuffer.allocate(Long.BYTES).putLong(new Long(readOffset)).array())
                    == ZK_ACTION_OK)
                break;
            retries++;
        }
    }

    public boolean isReady() {
        return ready;
    }

    @Override
    public void shutdown() {
        read = false;
        try {
            if(null != zookeeper)
                zookeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        if(null != consumer)
            consumer.close();
    }

    /**
     * Reads the read offset for this topic
     *
     * @param simpleConsumer
     * @param topic
     * @param partition
     * @param whichTime
     * @param clientName
     * @return
     */
    public long getLastOffset(kafka.javaapi.consumer.SimpleConsumer simpleConsumer, String topic, int partition,
                              long whichTime, String clientName) {
        if(null == simpleConsumer)
            simpleConsumer = consumer;
        
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));

        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = simpleConsumer.getOffsetsBefore(request);

        if (response.hasError()) {
            logger.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }

        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    /**
     * Returns metadata information about a topic and partition partition
     *
     * @param seedBrokers
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    private PartitionMetadata fetchTopicMetadata(List<String> seedBrokers, int port, String topic, int partition) {
        PartitionMetadata returnMetaData = null;
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);

        loop:
        for (String seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");

                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
                logger.info("Looking for topic/partition information from broker " + seed);

                List<TopicMetadata> metaData = resp.topicsMetadata();

                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
                        + ", " + partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            logger.info("Updating list of replicas for topic " + topic + " and partition " + partition);
            brokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                brokers.add(replica.host());
            }
        }
        return returnMetaData;
    }

    /**
     * Returns the name of the broker that works as the lead for a given topic and replica based off the partition metadata found
     *
     * @param a_oldLeader
     * @param a_topic
     * @param a_partition
     * @param a_port
     * @return
     * @throws Exception
     */
    private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = fetchTopicMetadata(brokers, a_port, a_topic, a_partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                String leader = metadata.leader().host();
                logger.info("Found leader for topic: " + a_topic + " and partition: " + a_partition + ": " + leader);
                return leader;
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        logger.error("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    /**
     * Helper method to create ZK nodes to persist the offset for this consumer
     */
    private void createConsumerOffsetNodes() {
        createZKNode(ZK_PARENT_NODE, new byte[0]);
        createZKNode(zkConsumerNodeName, new byte[0]);
    }
    
    /**
     * Provides async node creation
     */
    private void createZKNode(String path, byte[] data) {
        checkNotNull(zookeeper, "Zookeeper has not been instantiated");

        zookeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        switch (KeeperException.Code.get(rc)) {
                            case CONNECTIONLOSS:
                                createZKNode(path, (byte[]) ctx); // retry in cse of connection error
                                break;
                            case NODEEXISTS:
                                logger.info("Nodes already created");
                                break;
                            case OK:
                                logger.info("Node created");
                                break;
                            default:
                                logger.error("Something went wrong", KeeperException.create(KeeperException.Code.get(rc), path));
                        }
                    }
                }, data);
    }


    /**
     * Updates the data of an specific node in ZK
     */
    private int updateZKNode(String path, byte[] data) {
        checkNotNull(zookeeper, "Zookeeper has not been instantiated");

        try {
            zookeeper.setData(path, data, -1);
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error updating ZK node {} with exception {}", path, e);
            if(e instanceof KeeperException && 
                    ((KeeperException) e).code() == KeeperException.Code.NONODE) {
                createZKNode(zkConsumerNodeName, new byte[0]);
                return ZK_ACTION_RETRY;
            }
            e.printStackTrace();
        }

        return ZK_ACTION_OK;
    }

    /**
     * Fetches the current offset for this consumer from ZK and if not found returns the earliest offset
     * available in Kafka
     *
     * @return offset id
     */
    public long getConsumerOffset() {
        checkNotNull(zookeeper, "Zookeeper has not been instantiated");

        try {
            byte[] data = zookeeper.getData(zkConsumerNodeName, false, new Stat());
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.put(data);
            buffer.flip();
            return buffer.getLong();
        } catch (KeeperException | InterruptedException | BufferUnderflowException e) {
            logger.error("Error reading from ZK, node {} with exception {}", zkConsumerNodeName, e);
            if(e instanceof KeeperException && 
                    ((KeeperException) e).code() == KeeperException.Code.NONODE) {
                logger.info("No offset registered for this node, it doesn't exist");
            }
            e.printStackTrace();
        }

        return getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
    }

}