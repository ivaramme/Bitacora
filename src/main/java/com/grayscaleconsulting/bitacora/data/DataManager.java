package com.grayscaleconsulting.bitacora.data;

import com.grayscaleconsulting.bitacora.storage.LocalStorage;
import com.grayscaleconsulting.bitacora.data.metadata.KeyValue;
import com.grayscaleconsulting.bitacora.kafka.Producer;

/**
 * Represents the main API needed to interact with the data of the system.
 *
 * Created by ivaramme on 6/29/15.
 */
public interface DataManager {
    boolean contains(String key);

    /**
     * Returns the KeyValue metadata attached to a key. Lookups are local only 
     * @param key
     * @return
     */
    KeyValue getRaw(String key);

    /**
     * Returns the KeyValue metadata attached to a key. 
     * 
     * <p>In case data is not found locally, will forward to other node in cluster.</p>
     * @param key
     * @return
     */
    KeyValue getRaw(String key, boolean forwardIfMissing);

    /**
     * Returns the value attached to the key. Lookups are local only
     * @param key
     * @return
     */
    String get(String key);
    /**
     * Returns the value attached to the key. 
     * 
     * <p>In case data is not found locally, will forward to other node in cluster.</p>
     * @param key
     * @return
     */
    String get(String key, boolean forwardIfMissing);

    /**
     * Sets a new value in the system. In reality the value is not persisted locally but
     * sent to the source of truth which in turn will eventually forward to all nodes in the cluster
     *  
     * @param key
     * @param value
     */
    void set(String key, String value);

    /**
     * Sets a value to in-local hashmap from an external request.
     * @param value
     */
    void setFromCluster(KeyValue value);

    /**
     * Sets a value to in-local hashmap from the source of truth
     * @param value
     */
    void setFromLog(KeyValue value); // responds messages sent via kafka

    /**
     * Sets a value to in-local hashmap from the source of truth
     * @param value
     */
    void setFromLog(String key, KeyValue value); // responds messages sent via kafka

    /**
     * Marks a key as available for deletion and sends it down the log.
     * 
     * <p>The data is not deleted locally, it is just expired and then sent to the log as an immutable event </p>
     * @param key
     */
    void delete(String key);
    
    void setExternal(DataManagerExternal external);
    void setProducer(Producer producer);
    void setLocalStorage(LocalStorage storage);
}
