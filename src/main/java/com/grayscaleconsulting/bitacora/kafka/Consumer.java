package com.grayscaleconsulting.bitacora.kafka;

import com.grayscaleconsulting.bitacora.data.DataManager;

/**
 * Represents an Instance of a Log consumer  
 * Created by ivaramme on 7/28/15.
 */
public interface Consumer {
    /**
     * Sets DataManager instance where to push the data to. 
     * @param dataManager
     */
    void setDataManager(DataManager dataManager);

    /**
     * Starts the consumer.
     */
    void start();

    /**
     * Stops the consumer.
     */
    void shutdown();
}
