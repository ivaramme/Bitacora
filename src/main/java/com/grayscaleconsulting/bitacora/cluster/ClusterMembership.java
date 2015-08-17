package com.grayscaleconsulting.bitacora.cluster;

import com.grayscaleconsulting.bitacora.cluster.nodes.Node;

import java.util.List;

/**
 * Implements all the cluster membership logic needed by a node
 *
 * Created by ivaramme on 6/29/15.
 */
public interface ClusterMembership {
    /**
     * Initializes membership system
     */
    void initialize();

    /**
     * Registers node to main registry
     * @return
     * @throws Exception
     */
    boolean registerNode() throws Exception;

    /**
     * Watches for new nodes that join the cluster
     */
    void watchForNodes();

    /**
     * List all available nodes in the cluster 
     * @return list with node names
     */
    List<Node> getAvailableNodes() throws IllegalStateException;

    /**
     * Stops the cluster membership process
     * @throws InterruptedException
     */
    void shutdown() throws InterruptedException;
    
    boolean isInitialized();
    boolean isRegistered();
}
