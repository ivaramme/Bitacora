package com.grayscaleconsulting.bitacora.data;

import com.grayscaleconsulting.bitacora.data.external.ExternalRequest;
import com.grayscaleconsulting.bitacora.data.metadata.KeyValue;
import com.grayscaleconsulting.bitacora.kafka.*;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;

import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DataManagerImplTest {
    public static final int SLEEP_TIME = 600;
    private DataManager dataManager;
    private Producer producer;
    private Consumer consumer;
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;
    private KafkaServer kafkaServer;
    private Buffer<KafkaServer> servers;
    private String test_topic = "test_topic";
    
    @Before
    public void setup() throws Exception {
        zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
        zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        int kafkaPort = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(0, kafkaPort, false);

        KafkaConfig config = new KafkaConfig(props);
        MockTime mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        List<KafkaServer> kafkaServers = new ArrayList<>();
        kafkaServers.add(kafkaServer);
        servers = JavaConversions.asScalaBuffer(kafkaServers);

        TestUtils.createTopic(zkClient, test_topic, 1, 1, servers, new Properties());
        TestUtils.waitUntilMetadataIsPropagated(servers, test_topic, 0, 5000);
        
        producer = new KafkaProducerImpl("localhost:"+kafkaPort, test_topic);
        dataManager = new DataManagerImpl();
        dataManager.setProducer(producer);

        List<String> brokers = new ArrayList<>();
        brokers.add("localhost");
        //KafkaConsumerImpl(test_topic, consumer_name, zkServer.connectString(), 1);
        consumer = new KafkaSimpleConsumerImpl("localhost-node", brokers, kafkaPort, test_topic, 0, zkServer.connectString());
        consumer.setDataManager(dataManager);
        consumer.start();
        
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
    public void testGetRawData() throws Exception {
        final String key = "key";

        internalSet(key, "value");
        Thread.sleep(SLEEP_TIME);
        assertNotNull(dataManager.get(key));

        KeyValue value = dataManager.getRaw(key);
        assertEquals(dataManager.get(key),value.getValue());
        assertEquals(key,value.getKey());
        assertEquals(KeyValue.SOURCE_LOG, value.getSource());
        assertEquals(KeyValue.TTL_FOREVER, value.getTtl());
        assertNotNull(value.getUuid());
    }
    
    @Test
    public void testReturnNullIfNotFound() throws Exception {
        String key = "a_random_key";
        assertNull(dataManager.get(key));
    }

    @Test
    public void testDelete() throws Exception {
        final String key = "key";
        
        internalSet(key, "value");
        Thread.sleep(SLEEP_TIME);
        assertNotNull(dataManager.get(key));
        
        dataManager.delete(key);
        Thread.sleep(SLEEP_TIME);
        System.out.println(dataManager.get(key));
        assertNull(dataManager.get(key));
    }

    @Test
    public void testQueryingOtherNodes() throws Exception {
        KeyValue val = KeyValue.createNewKeyValue("key", "a_value", 22000000, KeyValue.TTL_FOREVER);
        dataManager.setExternal(setupExternalManager(200, val.getKey(), val.getValue(), val.getTtl()));

        assertNull(dataManager.get(val.getKey(), false)); // Not forwarding

        KeyValue fetchedValue = dataManager.getRaw(val.getKey(), true);
        assertNotNull(fetchedValue); // forwarding request
        assertEquals(val.getKey(), fetchedValue.getKey());
        assertEquals(val.getValue(), fetchedValue.getValue());
    }

    @Test
    public void testQueryingOtherNodesFail() throws Exception {
        dataManager.setExternal(setupExternalManager(404, "key", null, KeyValue.TTL_FOREVER));
        assertNull(dataManager.get("key", true));
    }
    
    @Test
    public void testIgnoreExpiredKey() throws Exception {
        dataManager.setExternal(setupExternalManager(200, "key", "a_value", KeyValue.TTL_EXPIRED));
        assertNull(dataManager.get("key", true));
    }

    @Test
    public void testCheckJustExpiredKey() throws Exception {
        // Set a key that will expire in 100 seconds
        dataManager.setExternal(setupExternalManager(200, "key", "a_value", System.currentTimeMillis() + 100000));
        KeyValue value = dataManager.getRaw("key", true);
        assertNotNull(value);
        assertEquals("uuid", value.getUuid());

        // Set an expired key
        dataManager.setExternal(setupExternalManager(200, "another_key", "another_value", System.currentTimeMillis() - 100));
        assertNull(dataManager.get("another_key", true));
    }
    
    private void internalSet(String key, String value) {
        dataManager.set(key, value);
    }
    
    private DataManagerExternal setupExternalManager(int responseCode, String key, String value, long ttl) {
        ExternalRequest mockedRequest = mock(ExternalRequest.class);
        when(mockedRequest.getKeyValue()).thenReturn(KeyValue.createKeyValueFromClusterValue(key, value, 100000, ttl, "uuid"));
        
        if(404 == responseCode) {
            mockedRequest = null;
        }
        
        DataManagerExternal manager = mock(DataManagerExternalImpl.class);
        when(manager.isStillValidRequest(key, null)).thenReturn(true);
        when(manager.initiateExternalRequest(key)).thenReturn(mockedRequest);
        
        return manager;
    }

}