package com.grayscaleconsulting.bitacora.kafka;

import com.grayscaleconsulting.bitacora.data.metadata.KeyValue;

/**
 * Broadcasts values set to the main datastore.
 *
 * Created by ivaramme on 7/28/15.
 */
public interface Producer {
    void start();
    
    /**
     * Publishes a key value object 
     * @param value
     */
    void publish(KeyValue value);

    /**
     * Sets an arbitrary value to a key
     * @param key
     * @param value
     */
    void publish(String key, Object value);
    
    /**
     * Stops producer from sending messages* 
     */
    void shutdown();
}
