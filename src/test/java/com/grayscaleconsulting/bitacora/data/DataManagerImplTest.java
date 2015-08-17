package com.grayscaleconsulting.bitacora.data;

import com.grayscaleconsulting.bitacora.data.external.ExternalRequest;
import com.grayscaleconsulting.bitacora.data.metadata.KeyValue;
import com.grayscaleconsulting.bitacora.kafka.KafkaConsumerImpl;
import com.grayscaleconsulting.bitacora.kafka.KafkaProducerImpl;
import com.grayscaleconsulting.bitacora.kafka.Producer;

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
import java.util.UUID;

import static org.junit.Assert.*;

public class DataManagerImplTest {
    private DataManager dataManager;
    private Producer producer;
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;
    private KafkaServer kafkaServer;
    private Buffer<KafkaServer> servers;
    private String test_topic = "test_topic";
    private String consumer_name = "test_consumer";
    
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

        KafkaConsumerImpl consumer = new KafkaConsumerImpl(test_topic, consumer_name, zkServer.connectString());
        consumer.setDataManager(dataManager);
        consumer.start(1);
        
        Thread.sleep(100);
    }
    
    @After
    public void tearDown() throws IOException {
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }
    
    @Test
    public void testGetRawData() throws Exception {
        final String key = "key";

        internalSet(key, "value");
        Thread.sleep(100);
        assertNotNull(dataManager.get(key));

        KeyValue value = dataManager.getRaw(key);
        assertEquals(dataManager.get(key),value.getValue());
        assertEquals(key,value.getKey());
        assertEquals(KeyValue.SOURCE_LOG, value.getSource());
        assertEquals(KeyValue.TTL_FOREVER, value.getTtl());

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
        Thread.sleep(100);
        assertNotNull(dataManager.get(key));
        
        dataManager.delete(key);
        Thread.sleep(100);
        System.out.println(dataManager.get(key));
        assertNull(dataManager.get(key));
    }

    @Test
    public void testQueringOtherNodes() throws Exception {
        KeyValue val = KeyValue.createKeyValueFromClusterValue("key", "a_value", 22000000, KeyValue.TTL_FOREVER);
        dataManager.setExternal(setupExternalManager(200, val.getValue()));

        assertNull(dataManager.get(val.getKey(), false)); // Not forwarding

        KeyValue fetchedValue = dataManager.getRaw(val.getKey(), true);
        assertNotNull(fetchedValue); // forwarding request
        assertEquals(val.getKey(), fetchedValue.getKey());
        assertEquals(val.getValue(), fetchedValue.getValue());
    }

    @Test
    public void testQueringOtherNodesFail() throws Exception {
        dataManager.setExternal(setupExternalManager(404, null));

        assertNull(dataManager.get("key", true));
    }
    
    private void internalSet(String key, String value) {
        dataManager.set(key, value);
    }
    
    private DataManagerExternal setupExternalManager(int responseCode, String value) {
        return new DataManagerExternal() {
            @Override
            public ExternalRequest initiateExternalRequest(String key) {
                return new MockExternalRequest(key, responseCode, value);
            }

            @Override
            public void setQuorum(long percentageNodesNeededToProceed) {

            }

            @Override
            public void invalidateExternalRequest(String key) {

            }

            @Override
            public boolean isStillValidRequest(String key, UUID token) {
                return true;
            }
        };
    }
    
    private class MockExternalRequest extends ExternalRequest {
        private String value;
        private int code;
        
        public MockExternalRequest(String key, int totalRequests) {
            super(key, totalRequests);
        }

        public MockExternalRequest(String key, int responseCode, String value) {
            super(key, 1);
            this.code = responseCode;
            this.value = value;
        }
        
        public KeyValue getKeyValue() {
            if(200 == code)
                return KeyValue.createKeyValueFromClusterValue(this.getKey(), this.value, 100000, KeyValue.TTL_FOREVER);
            else 
                return null;
        }
    }
}