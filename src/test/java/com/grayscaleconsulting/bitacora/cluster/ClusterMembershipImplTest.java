package com.grayscaleconsulting.bitacora.cluster;

import kafka.utils.TestUtils;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ClusterMembershipImplTest {
    private TestingServer zkServer;
    private ClusterMembership membership;
    private int portZK;
    
    @Before
    public void setup() throws Exception {
        portZK = TestUtils.choosePort();
        int portApi = TestUtils.choosePort();
        zkServer = new TestingServer(portZK, true);
        membership = new ClusterMembershipImpl("127.0.0.1:" + portZK, "node1", portApi);
        membership.initialize();

        Thread.sleep(100);
    }

    @After
    public void tearDown() throws IOException {
        zkServer.stop();
        zkServer.close();
    }
    @Test(expected = java.lang.IllegalStateException.class )
    public void testRegisterNodeOnceOnly() throws Exception {
        assertTrue(membership.isInitialized());
        Thread.sleep(100);
        // Registering the second time will fail
        membership.registerNode();
    }

    /**
     * Makes sure that node receives updated list of nodes when a new member registers to the cluster
     * @throws Exception
     */
    @Test
    public void testWatchForNewNodes() throws Exception {
        assertTrue(membership.isInitialized());
        assertEquals(0, membership.getAvailableNodes().size());

        ClusterMembership newMember = new ClusterMembershipImpl("127.0.0.1:"+portZK, "node2", 5004);
        newMember.initialize();
        Thread.sleep(100);
        
        assertEquals(1, membership.getAvailableNodes().size());
    }

    @Test
    public void testWatchForRemovedNodes() throws Exception {
        assertTrue(membership.isInitialized());
        assertEquals(0, membership.getAvailableNodes().size());

        ClusterMembership newMember = new ClusterMembershipImpl("127.0.0.1:"+portZK, "node2", 5004);
        newMember.initialize();
        Thread.sleep(100);

        assertEquals(1, membership.getAvailableNodes().size());
        newMember.shutdown();
        Thread.sleep(100);

        assertEquals(0, membership.getAvailableNodes().size());
    }
    
    @Test
    public void testOnDisconnectNotRegistered() throws Exception {
        assertTrue(membership.isRegistered());
        
        zkServer.close();
        Thread.sleep(100);
        
        assertFalse(membership.isRegistered());
    }
    
    @Test(expected = java.lang.IllegalStateException.class )
    public void testOnDisconnectThrowExceptionFetchingNodes() throws Exception {
        assertTrue(membership.isRegistered());

        zkServer.close();
        Thread.sleep(200);

        membership.getAvailableNodes();
    }

}