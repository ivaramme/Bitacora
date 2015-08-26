package com.grayscaleconsulting.bitacora.kafka;

import com.grayscaleconsulting.bitacora.data.DataManager;
import com.grayscaleconsulting.bitacora.data.DataManagerImpl;
import kafka.common.TopicAndPartition;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import scala.Int;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;
import scala.collection.JavaConverters.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class KafkaSimpleConsumerImplTest {
    public static final int SLEEP_TIME = 600;
    
    private Consumer consumer;
    private Producer producer;
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;
    private KafkaServer kafkaServer;
    private Buffer<KafkaServer> servers;
    private String test_topic = "test_topic";
    private DataManager dataManager;
    private int kafkaPort;
    private List<String> brokers;
    private List<KafkaServer> kafkaServers;

    @Before
    public void setup() throws Exception {
        zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
        zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        kafkaPort = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(0, kafkaPort, false);

        KafkaConfig config = new KafkaConfig(props);
        MockTime mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        kafkaServers = new ArrayList<KafkaServer>();
        kafkaServers.add(kafkaServer);
        servers = JavaConversions.asScalaBuffer(kafkaServers);

        TestUtils.createTopic(zkClient, test_topic, 1, 1, servers, new Properties());
        TestUtils.waitUntilMetadataIsPropagated(servers, test_topic, 0, 5000);

        producer = new KafkaProducerImpl("localhost:"+ kafkaPort, test_topic);
        dataManager = new DataManagerImpl();
        dataManager.setProducer(producer);

        brokers = new ArrayList<String>();
        brokers.add("localhost");
        //KafkaConsumerImpl(test_topic, consumer_name, zkServer.connectString(), 1);
        consumer = new KafkaSimpleConsumerImpl("localhost-node", brokers, kafkaPort, test_topic, 0, zkServer.connectString());
        consumer.setDataManager(dataManager);
        
        Thread.sleep(SLEEP_TIME);
    }

    @After
    public void tearDown() throws IOException {

        consumer.shutdown();
        producer.shutdown();

        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }
    
    @Test
    public void testKafkaServerDown() throws Exception {
        kafkaServer.shutdown();
        
        consumer.start();
        Thread.sleep(SLEEP_TIME);
        assertFalse(consumer.isReady());
    }

    @Test
    public void testInvalidTopic() throws Exception {
        consumer = new KafkaSimpleConsumerImpl("localhost-node", brokers, kafkaPort, "invalid_test", 0, zkServer.connectString());
        consumer.start();
        Thread.sleep(SLEEP_TIME);
        assertFalse(consumer.isReady());
    }

    @Test
    public void testPublishOffsetEvery10Messages() throws Exception {
        consumer.start();
        
        assertEquals(0, ((KafkaSimpleConsumerImpl)consumer).getConsumerOffset());
        
        for(int i = 0; i < 18; i++) {
            dataManager.set("key", "value");
        }
        
        Thread.sleep(SLEEP_TIME);

        // Zookeeper's consumer
        assertEquals(11, ((KafkaSimpleConsumerImpl)consumer).getConsumerOffset());
        // Source of truth - Kafka
        assertEquals(18, ((KafkaSimpleConsumerImpl) consumer).getLastOffset(null, test_topic, 0, kafka.api.OffsetRequest.LatestTime(), "localhost-node"));
    }
    
    @Ignore
    public void testExistsWhenZookeperGoesDown() throws Exception {
        consumer.start();
        ConcurrentHashMap.KeySetView sessions = (ConcurrentHashMap.KeySetView) zkServer.zookeeper().getZKDatabase().getSessions();
        long sessionID = (Long) sessions.getMap().keySet().iterator().next();
        zkServer.zookeeper().closeSession(sessionID);
    }

    @Ignore
    public void testElectsNewLeader() throws Exception {
        // Create a new Kafka server for the same topic/partition and switch the leader
        Properties props = TestUtils.createBrokerConfig(1, TestUtils.choosePort(), false);
        KafkaConfig config = new KafkaConfig(props);
        MockTime mock = new MockTime();
        KafkaServer kafkaServer2 = TestUtils.createServer(config, mock);
        
        kafkaServers.add(kafkaServer2);
        servers = JavaConversions.asScalaBuffer(kafkaServers);

        TestUtils.createTopic(zkClient, "topic2", 1, 2, servers, new Properties());
        TestUtils.waitUntilMetadataIsPropagated(servers, "topic2", 0, 5000);
        
        consumer.start();

        dataManager.set("key", "value");
        Thread.sleep(SLEEP_TIME);
        assertNotNull(dataManager.get("key")); // consumer is working fine


        Map _p = new HashMap<>();
        _p.put(0,0);
        _p.put(1,1);
        scala.collection.immutable.Map partitions = (scala.collection.immutable.Map) JavaConversions.asScalaMap(_p);

        TopicAndPartition topicAndPartition = new TopicAndPartition("topic2", 0);
        TestUtils.makeLeaderForPartition(zkClient, "topic2", partitions, 1);

        dataManager.set("key2", "value");
        Thread.sleep(SLEEP_TIME);
        assertNotNull(dataManager.get("key2")); // consumer is still working
        
        kafkaServer2.shutdown();
    }
}