package com.grayscaleconsulting.bitacora.kafka;

import com.grayscaleconsulting.bitacora.data.DataManager;
import com.grayscaleconsulting.bitacora.data.DataManagerImpl;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class KafkaSimpleConsumerImplTest {
    public static final int SLEEP_TIME = 600;
    
    private Consumer consumer;
    private Producer producer;
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;
    private KafkaServer kafkaServer;
    private KafkaServer kafkaServer2;
    private Buffer<KafkaServer> servers;
    private String test_topic = "test_topic";
    private DataManager dataManager;
    private int kafkaPort;
    private int kafkaPort2;
    private List<String> brokers;
    private List<KafkaServer> kafkaServers;
    private KafkaConfig config;

    @Before
    public void setup() throws Exception {
        zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
        zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        kafkaPort = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(0, kafkaPort, false);
        config = new KafkaConfig(props);
        MockTime mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        kafkaPort2 = TestUtils.choosePort();
        props = TestUtils.createBrokerConfig(1, kafkaPort2, false);
        KafkaConfig config2 = new KafkaConfig(props);
        mock = new MockTime();
        kafkaServer2 = TestUtils.createServer(config2, mock);

        kafkaServers = new ArrayList<KafkaServer>();
        kafkaServers.add(kafkaServer);
        kafkaServers.add(kafkaServer2);
        servers = JavaConversions.asScalaBuffer(kafkaServers);

        TestUtils.createTopic(zkClient, test_topic, 1, 2, servers, new Properties());
        TestUtils.waitUntilMetadataIsPropagated(servers, test_topic, 0, 5000);

        producer = new KafkaProducerImpl("localhost:"+ kafkaPort+",localhost:"+kafkaPort2, test_topic);
        producer.start();
        
        dataManager = new DataManagerImpl();
        dataManager.setProducer(producer);

        brokers = new ArrayList<String>();
        brokers.add("localhost:"+kafkaPort);
        brokers.add("localhost:"+kafkaPort2);
        
        consumer = new KafkaSimpleConsumerImpl("localhost-node", brokers, test_topic, 0, zkServer.connectString());
        consumer.setDataManager(dataManager);
        
        Thread.sleep(SLEEP_TIME);
    }

    @After
    public void tearDown() throws IOException {

        consumer.shutdown();
        producer.shutdown();

        kafkaServer.shutdown();
        kafkaServer2.shutdown();
        
        zkClient.close();
        zkServer.shutdown();
    }
    
    @Test (expected = RuntimeException.class)
    public void testKafkaServerDown() throws Exception {
        kafkaServer.shutdown();
        kafkaServer2.shutdown();
        
        consumer.start();
        Thread.sleep(SLEEP_TIME);
    }

    @Test
    public void testKafkaOneServerDownOnly() throws Exception {
        kafkaServer.shutdown();

        TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, test_topic, 0, 15000, Option.empty(), Option.apply(1));
        consumer.start();
        Thread.sleep(SLEEP_TIME*3);
        assertTrue(consumer.isReady());
    }

    @Test (expected = RuntimeException.class)
    public void testInvalidTopic() throws Exception {
        consumer = new KafkaSimpleConsumerImpl("localhost-node", brokers, "invalid_test", 0, zkServer.connectString());
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
    
    @Test(expected = RuntimeException.class)
    public void testExistsWhenUnableToReachKafkaGoesDown() throws Exception {
        kafkaServer.shutdown();
        kafkaServer2.shutdown();
        consumer.start();
    }
    
    @Test
    public void testExistsWhenUnableToReachZK() throws Exception {
        zkServer.shutdown();
        consumer.start();
        assertFalse(consumer.isReady());
    }

    @Test
    public void testElectsNewLeader() throws Exception {
        consumer.start();
        
        dataManager.set("key", "value");
        Thread.sleep(SLEEP_TIME);
        assertNotNull(dataManager.get("key")); // consumer is working fine

        Option leader = ZkUtils.getLeaderForPartition(zkClient, test_topic, 0);
        KafkaServer leadServer;
        KafkaServer slaveServer;
        if(leader.isDefined() && ((Integer)leader.get() == 0)) {
            leadServer = kafkaServer;
            slaveServer = kafkaServer2;
        } else {
            leadServer = kafkaServer2;
            slaveServer = kafkaServer;
        }
        assertEquals("localhost:"+leadServer.config().port(), ((KafkaSimpleConsumerImpl) consumer).getLeadBroker());
        
        // shutdown first kafka server
        leadServer.shutdown();
        leadServer.awaitShutdown();

        TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, test_topic, 0, 5000, Option.apply((Integer)leader.get()), Option.empty());
        TestUtils.waitUntilMetadataIsPropagated(servers, test_topic, 0, 5000);
        Thread.sleep(5000); // Wait for consumer to catch up
        
        assertTrue(brokers.size() == 2);
        assertNotEquals(leader, ZkUtils.getLeaderForPartition(zkClient, test_topic, 0));
        assertEquals("localhost:"+slaveServer.config().port(), ((KafkaSimpleConsumerImpl) consumer).getLeadBroker());
    }

    @Test
    public void testRecoversWhenKafkaGoesDownAndUp() throws Exception {
        consumer.start();

        dataManager.set("key", "value");
        Thread.sleep(SLEEP_TIME);
        assertNotNull(dataManager.get("key")); // consumer is working fine

        // shutdown first kafka server
        kafkaServer.shutdown();
        kafkaServer2.shutdown();
        kafkaServer.awaitShutdown();
        kafkaServer2.shutdown();
        
        TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, test_topic, 0, 15000, Option.empty(), Option.apply(1));
        
        kafkaServer.startup();

        TestUtils.waitUntilMetadataIsPropagated(servers, test_topic, 0, 2000);
        Thread.sleep(SLEEP_TIME);
        dataManager.set("key2", "value");
        Thread.sleep(SLEEP_TIME);
        Thread.sleep(SLEEP_TIME);
        assertNotNull(dataManager.get("key2")); // consumer is still working
    }
    
    
}