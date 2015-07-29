package com.grayscaleconsulting.streaming.data;

import com.grayscaleconsulting.streaming.cluster.ClusterMembership;
import com.grayscaleconsulting.streaming.cluster.nodes.Node;
import com.grayscaleconsulting.streaming.data.external.ExternalRequestTask;
import com.grayscaleconsulting.streaming.data.external.ExternalRequest;
import com.grayscaleconsulting.streaming.data.metadata.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Created by ivaramme on 6/30/15.
 */
public class DataManagerExternalImpl implements DataManagerExternal {
    private static Logger logger = LoggerFactory.getLogger(DataManagerExternalImpl.class);
    
    private Map<String, ExternalRequestTask> pendingRequests;
    private Map<String, Long> lastRequestTimestamp;
    private ClusterMembership clusterMembership;
    
    private double quorumRequired = 0.5;
    // mills needed to pass before another request can be done for the same window
    private int    requestWindow  = 200;
    
    private ExecutorService executor;

    public DataManagerExternalImpl(ClusterMembership clusterMembership) {
        this.clusterMembership = clusterMembership;
        
        pendingRequests = new ConcurrentHashMap<>();
        lastRequestTimestamp = new ConcurrentHashMap<>();
        executor = Executors.newCachedThreadPool();
    }

    @Override
    public ExternalRequest initiateExternalRequest(String key) {
        KeyValue missingValue;

        if(pendingRequests.containsKey(pendingRequests)
                && System.currentTimeMillis() > (lastRequestTimestamp.getOrDefault(key, 0L) + requestWindow)) {
            logger.info("Cancelling existing request");
            cancelRequests(key);
        }

        List<Node> nodes = clusterMembership.getAvailableNodes();
        List<String> endpoints = nodes.stream().map(Node::getEndpoint).collect(Collectors.toList());

        ExternalRequestTask request =  new ExternalRequestTask(endpoints, key, quorumRequired);

        pendingRequests.putIfAbsent(key, request);
        lastRequestTimestamp.putIfAbsent(key, System.currentTimeMillis());

        ExternalRequest resp;
        try {
            resp = request.call();
        }catch (IllegalArgumentException iae) { 
            return null;
        }
        
        return resp;
    }

    @Override
    public void setQuorum(long percentageNodesNeededToProceed) {
        this.quorumRequired = percentageNodesNeededToProceed;
    }

    public void invalidateExternalRequest(String key) {
        if(pendingRequests.containsKey(key)) {
            cancelRequests(key);
        }
    }

    public boolean isStillValidRequest(String key, UUID token) {
        if(pendingRequests.containsKey(key) && pendingRequests.get(key).getRequest().getToken().equals(token)){
            logger.info("Valid external request completed with value: " + pendingRequests.get(key).getRequest());
            return true;
        } else {
            logger.info("Dismissed completed request");
            return false;
        }
    }

    /**
     * Attempts to cancel a task that is already in progress if another request with the same key
     * has been made.
     * 
     * @param key
     */
    private void cancelRequests(String key) {
        ExternalRequestTask task = pendingRequests.get(key);
        if(task != null) {
            task.cancel();
        }
        pendingRequests.remove(key);
    }
}
