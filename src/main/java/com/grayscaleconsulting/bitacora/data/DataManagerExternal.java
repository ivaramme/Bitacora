package com.grayscaleconsulting.bitacora.data;

import com.grayscaleconsulting.bitacora.data.external.ExternalRequest;

import java.util.UUID;

/**
 * Represents a Manager that interacts with other nodes in the cluster for missing data
 *
 * Created by ivaramme on 6/30/15.
 */
public interface DataManagerExternal {

    /**
     * Create a new KeyValue object as null and missing and send request
     * to nodes in cluster.
     * 
     * <p>Keep a reference of the request in a hashmap, so it can be stopped in case
     * the </p>
     * @param key
     */
    ExternalRequest initiateExternalRequest(String key);

    /**
     * Specifies minimum number of nodes needed to proceed setting a value returned from a cluster
     * in percentage, i.e. 0.50 requires half the cluster to respond, etc.
     *
     * @param percentageNodesNeededToProceed
     */
    void setQuorum(double percentageNodesNeededToProceed);

    /**
     * Attempts to cancel a task that is already in progress if another request with the same key
     * has been made.
     *
     * @param key
     */
    void invalidateExternalRequest(String key);

    /**
     * Returns true if the response received is still valid, that means the key has not been set by 
     * the source of truth nor has it have been requested again some time later
     * @param key
     * @param token
     * @return
     */
    boolean isStillValidRequest(String key, UUID token);
}
