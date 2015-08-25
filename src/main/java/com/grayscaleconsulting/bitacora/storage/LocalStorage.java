package com.grayscaleconsulting.bitacora.storage;

import com.grayscaleconsulting.bitacora.data.metadata.KeyValue;

/**
 * Represents an instance that handles the storage of data locally in whatever format/technology the user wants to use.
 * 
 * Created by ivaramme on 8/19/15.
 */
public interface LocalStorage {

    /**
     * Persists key-value
     * @param key
     * @param value
     * @throws Exception
     */
    void set(String key, KeyValue value) throws Exception;

    /**
     * Returns a value using a key from local storage, null if not found. 
     * @param key
     * @return
     * @throws Exception
     */
    KeyValue get(String key) throws Exception;

    /**
     * Removes a value from local storage
     * @param key
     * @throws Exception
     */
    void delete(String key) throws Exception;

    /**
     * Starts a new instance of this class
     * @throws Exception
     */
    void start();
    
    /**
     * Closes the LocalStorage object
     * @throws InterruptedException
     */
    void shutdown();

    /**
     * Returns status of the service
     * @return
     */
    boolean isReady();
}
