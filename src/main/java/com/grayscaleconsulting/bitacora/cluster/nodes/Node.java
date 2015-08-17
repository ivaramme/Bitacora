package com.grayscaleconsulting.bitacora.cluster.nodes;

/**
 * Created by ivaramme on 6/29/15.
 */
public class Node {
    private String name;
    private String endpoint;
    private NodeStats stats;

    public Node(String name, String endpoint) {
        this.name = name;
        this.endpoint = endpoint;
        this.stats = new NodeStats();
    }

    public String getName() {
        return name;
    }

    public String getEndpoint() {
        return endpoint;
    }
    
    public NodeStats getStats() {
        return stats;
    }

    public void setStats(NodeStats stats) {
        this.stats = stats;
    }
    
    @Override
    public String toString() {
        return "Node: " + getName() + " Endpoint: " + getEndpoint();
    }
}

