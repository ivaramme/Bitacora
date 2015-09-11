package com.grayscaleconsulting.bitacora.data;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.grayscaleconsulting.bitacora.cluster.ClusterMembership;
import com.grayscaleconsulting.bitacora.cluster.ClusterMembershipImpl;
import com.grayscaleconsulting.bitacora.data.external.ExternalRequest;
import com.grayscaleconsulting.bitacora.model.KeyValue;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.zk.EmbeddedZookeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.*;

public class DataManagerExternalImplTest {
    public static final int SLEEP_TIME = 500;
    
    private EmbeddedZookeeper zkServer;
    private ClusterMembership membership;
    private DataManagerExternal externalHandler;

    @Rule
    public WireMockRule service1 = new WireMockRule(TestUtils.choosePort());
    @Rule
    public WireMockRule service2 = new WireMockRule(TestUtils.choosePort());
    @Rule
    public WireMockRule service3 = new WireMockRule(TestUtils.choosePort());
    
    @Before
    public void setup() {
        // Support multiple nodes
        zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
        int portApi = TestUtils.choosePort();
        membership = new ClusterMembershipImpl(zkServer.connectString(), "127.0.0.1", portApi);
        membership.initialize();
        
        externalHandler = new DataManagerExternalImpl(membership);
        externalHandler.setQuorum(0.5);
    }
    
    @After
    public void tearDown() throws Exception{
        membership.shutdown();
        zkServer.shutdown();
    }
    
    // Prepare a request, return it immediately but delay the response. Check if the response is still valid or not
    @Test
    public void testInitiateExternalRequest() throws Exception {
        ClusterMembershipImpl membership1 = new ClusterMembershipImpl(zkServer.connectString(), "localhost", service1.port());
        membership1.initialize();

        Thread.sleep(SLEEP_TIME);
        
        String response = "{\"key\":\"a_key\",\"value\":\"a_value\",\"timestamp\":1440127917442,\"source\":1,\"ttl\":0,\"uuid\":\"uuid\"}";

        service1.stubFor(get(urlEqualTo("/rpc/cluster?key=a_key")).
                willReturn(aResponse().withStatus(200).withBody(response)));
        
        externalHandler.setQuorum(0.30);
        ExternalRequest request = externalHandler.initiateExternalRequest("a_key");
        assertNotNull(request.getKeyValue());
        assertTrue(externalHandler.isStillValidRequest("a_key", request.getToken()));

        membership1.shutdown();
        service1.stop();
    }

    @Test
    public void testSkipExternalRequestWithinRequestWindow() throws Exception {
        ClusterMembershipImpl membership1 = new ClusterMembershipImpl(zkServer.connectString(), "localhost", service1.port());
        membership1.initialize();

        Thread.sleep(SLEEP_TIME);

        String response = "{\"key\":\"a_key\",\"value\":\"a_value\",\"timestamp\":1440127917442,\"source\":1,\"ttl\":0,\"uuid\":\"uuid\"}";

        service1.stubFor(get(urlEqualTo("/rpc/cluster?key=a_key")).
                willReturn(aResponse().withStatus(200).withBody(response)));
        service1.start();
        
        externalHandler.setQuorum(0.30);

        // Initiate another request immediately
        ExternalRequest request = externalHandler.initiateExternalRequest("a_key");
        assertTrue(externalHandler.isStillValidRequest("a_key", request.getToken()));

        // New request will not create a new request as it is still within the response_window
        externalHandler.initiateExternalRequest("a_key");
        assertTrue(externalHandler.isStillValidRequest("a_key", request.getToken()));

        membership1.shutdown();
        service1.stop();
    }

    @Test
    public void testRequestsAreInvalidated() throws Exception {
        ClusterMembershipImpl membership1 = new ClusterMembershipImpl(zkServer.connectString(), "localhost", service1.port());
        membership1.initialize();

        Thread.sleep(SLEEP_TIME);

        String response = "{\"key\":\"a_key\",\"value\":\"a_value\",\"timestamp\":1440127917442,\"source\":1,\"ttl\":0,\"uuid\":\"uuid\"}";

        service1.stubFor(get(urlEqualTo("/rpc/cluster?key=a_key")).
                willReturn(aResponse().withStatus(200).withBody(response)));
        service1.start();
        
        externalHandler.setQuorum(0.30);

        // Initiate another request immediately
        ExternalRequest request = externalHandler.initiateExternalRequest("a_key");
        assertTrue(externalHandler.isStillValidRequest("a_key", request.getToken()));

        // New request should create a new token
        Thread.sleep(SLEEP_TIME);
        externalHandler.initiateExternalRequest("a_key");
        assertFalse(externalHandler.isStillValidRequest("a_key", request.getToken()));

        membership1.shutdown();
        service1.stop();
    }
    
    @Test
    public void testNoMembersInCluster() throws Exception {
        assertNull(externalHandler.initiateExternalRequest("a_key"));
    }
    
    // Create 3 different nodes that return the same data
    @Test
    public void testSameResponseFromMembers() throws Exception {
        ClusterMembershipImpl membership1 = new ClusterMembershipImpl(zkServer.connectString(), "localhost", service1.port());
        membership1.initialize();

        ClusterMembershipImpl membership2 = new ClusterMembershipImpl(zkServer.connectString(), "localhost", service2.port());
        membership2.initialize();

        ClusterMembershipImpl membership3 = new ClusterMembershipImpl(zkServer.connectString(), "localhost", service3.port());
        membership3.initialize();

        String response = "{\"key\":\"a_key\",\"value\":\"a_value\",\"timestamp\":1440127917442,\"source\":1,\"ttl\":0,\"uuid\":\"uuid\"}";
        
        service1.stubFor(get(urlEqualTo("/rpc/cluster?key=a_key")).
                willReturn(aResponse().withStatus(200).withBody(response)));
        service2.stubFor(get(urlEqualTo("/rpc/cluster?key=a_key")).
                willReturn(aResponse().withStatus(200).withBody(response)));
        service3.stubFor(get(urlEqualTo("/rpc/cluster?key=a_key")).
                willReturn(aResponse().withStatus(200).withBody(response)));
        
        service1.start();
        service2.start();
        service3.start();
        
        Thread.sleep(SLEEP_TIME);
        
        ExternalRequest request = externalHandler.initiateExternalRequest("a_key");
        assertEquals(3, request.getSuccessfulRequests());
        assertEquals(0, request.getFailedRequests());
        
        KeyValue value = request.getKeyValue();
        assertEquals("a_key", value.getKey());
        assertEquals("a_value", value.getValue());
        assertEquals("uuid", value.getUuid());
        
        service1.stop();
        service2.stop();
        service3.stop();

        membership1.shutdown();
        membership2.shutdown();
        membership3.shutdown();
    }

    // Create 3 different nodes, only 1 returns valid data, tests that the quorum parameter is respected
    @Test
    public void testQuorumRespected() throws Exception {
        ClusterMembershipImpl membership1 = new ClusterMembershipImpl(zkServer.connectString(), "localhost", service1.port());
        membership1.initialize();

        ClusterMembershipImpl membership2 = new ClusterMembershipImpl(zkServer.connectString(), "localhost", service2.port());
        membership2.initialize();

        ClusterMembershipImpl membership3 = new ClusterMembershipImpl(zkServer.connectString(), "localhost", service3.port());
        membership3.initialize();

        String response = "{\"key\":\"a_key\",\"value\":\"a_value\",\"timestamp\":1440127917442,\"source\":1,\"ttl\":0,\"uuid\":\"uuid\"}";

        service1.stubFor(get(urlEqualTo("/rpc/cluster?key=a_key")).
                willReturn(aResponse().withStatus(200).withBody(response)));
        service2.stubFor(get(urlEqualTo("/rpc/cluster?key=a_key")).
                willReturn(aResponse().withStatus(404).withBody("")));
        service3.stubFor(get(urlEqualTo("/rpc/cluster?key=a_key")).
                willReturn(aResponse().withStatus(404).withBody("")));

        service1.start();
        service2.start();
        service3.start();

        Thread.sleep(SLEEP_TIME);
        
        // No quorum available (2 servers out of 3 down)
        ExternalRequest request = externalHandler.initiateExternalRequest("a_key");
        Thread.sleep(SLEEP_TIME);
        
        assertNull(request.getKeyValue());
        assertEquals(0, request.getSuccessfulRequests());
        assertTrue(request.getFailedRequests() >= 3);
        
        // Re-enable one of the servers in bad state, quorum reached
        service2.stop();
        service2.resetMappings();
        service2.stubFor(get(urlEqualTo("/rpc/cluster?key=a_key")).
                willReturn(aResponse().withStatus(200).withBody(response)));
        service2.start();

        Thread.sleep(SLEEP_TIME);
        
        request = externalHandler.initiateExternalRequest("a_key");
        Thread.sleep(SLEEP_TIME);

        assertEquals(2, request.getSuccessfulRequests());
        assertEquals(1, request.getFailedRequests());
        
        KeyValue value = request.getKeyValue();
        assertEquals("a_key", value.getKey());
        assertEquals("a_value", value.getValue());
        assertEquals("uuid", value.getUuid());

        membership1.shutdown();
        membership2.shutdown();
        membership3.shutdown();
        
        service1.stop();
        service2.stop();
        service3.stop();
    }

    // Out of three nodes all of them have the different response, latest write wins
    @Test
    public void testDifferentResponseFromMembersLastWriteWins() throws Exception {
        ClusterMembershipImpl membership1 = new ClusterMembershipImpl(zkServer.connectString(), "localhost", service1.port());
        membership1.initialize();

        ClusterMembershipImpl membership2 = new ClusterMembershipImpl(zkServer.connectString(), "localhost", service2.port());
        membership2.initialize();

        ClusterMembershipImpl membership3 = new ClusterMembershipImpl(zkServer.connectString(), "localhost", service3.port());
        membership3.initialize();

        String response1 = "{\"key\":\"a_key\",\"value\":\"a_value\",\"timestamp\":1440127917443,\"source\":1,\"ttl\":0,\"uuid\":\"uuid\"}";
        String response2 = "{\"key\":\"a_key\",\"value\":\"an_older_value\",\"timestamp\":1440127917442,\"source\":1,\"ttl\":0,\"uuid\":\"uuid\"}";
        String response3 = "{\"key\":\"a_key\",\"value\":\"an_even_older_value\",\"timestamp\":1440127917441,\"source\":1,\"ttl\":0,\"uuid\":\"uuid\"}";

        service1.stubFor(get(urlEqualTo("/rpc/cluster?key=a_key")).
                willReturn(aResponse().withStatus(200).withBody(response1)));
        service2.stubFor(get(urlEqualTo("/rpc/cluster?key=a_key")).
                willReturn(aResponse().withStatus(200).withBody(response2)));
        service3.stubFor(get(urlEqualTo("/rpc/cluster?key=a_key")).
                willReturn(aResponse().withStatus(200).withBody(response3)));

        service1.start();
        service2.start();
        service3.start();

        Thread.sleep(SLEEP_TIME);

        ExternalRequest request = externalHandler.initiateExternalRequest("a_key");
        Thread.sleep(SLEEP_TIME);
        assertEquals(3, request.getSuccessfulRequests());
        
        KeyValue value = request.getKeyValue();
        assertEquals("a_key", value.getKey());
        assertEquals("a_value", value.getValue());
        assertEquals("uuid", value.getUuid());

        membership1.shutdown();
        membership2.shutdown();
        membership3.shutdown();
        
        service1.stop();
        service2.stop();
        service3.stop();
    }
    
    
}