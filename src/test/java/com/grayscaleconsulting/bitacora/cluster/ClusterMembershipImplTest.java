package com.grayscaleconsulting.bitacora.cluster;

import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.zk.EmbeddedZookeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ClusterMembershipImplTest {
    private EmbeddedZookeeper zkServer;
    private ClusterMembership membership;
    private int portZK;
    
    @Before
    public void setup() throws Exception {
        int portApi = TestUtils.choosePort();
        zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
        membership = new ClusterMembershipImpl(zkServer.connectString(), "node1", portApi);
        membership.initialize();

        Thread.sleep(100);
    }

    @After
    public void tearDown() throws IOException {
        zkServer.shutdown();
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

        ClusterMembership newMember = new ClusterMembershipImpl(zkServer.connectString(), "node2", 5004);
        newMember.initialize();
        Thread.sleep(200);
        
        assertEquals(1, membership.getAvailableNodes().size());
    }

    @Test
    public void testWatchForRemovedNodes() throws Exception {
        assertTrue(membership.isInitialized());
        assertEquals(0, membership.getAvailableNodes().size());

        ClusterMembership newMember = new ClusterMembershipImpl(zkServer.connectString(), "node2", 5004);
        newMember.initialize();
        Thread.sleep(200);

        assertEquals(1, membership.getAvailableNodes().size());
        newMember.shutdown();
        Thread.sleep(200);

        assertEquals(0, membership.getAvailableNodes().size());
    }
    
    @Test
    public void testOnDisconnectNotRegistered() throws Exception {
        assertTrue(membership.isRegistered());
        
        zkServer.shutdown();
        Thread.sleep(200);
        
        assertFalse(membership.isRegistered());
    }
    
    @Test(expected = java.lang.IllegalStateException.class )
    public void testOnDisconnectThrowExceptionFetchingNodes() throws Exception {
        assertTrue(membership.isRegistered());

        zkServer.shutdown();
        Thread.sleep(200);

        membership.getAvailableNodes();
    }

}