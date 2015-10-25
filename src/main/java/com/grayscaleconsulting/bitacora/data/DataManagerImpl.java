package com.grayscaleconsulting.bitacora.data;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.grayscaleconsulting.bitacora.data.external.ExternalRequest;
import com.grayscaleconsulting.bitacora.kafka.Producer;
import com.grayscaleconsulting.bitacora.metrics.Metrics;
import com.grayscaleconsulting.bitacora.model.KeyValue;
import com.grayscaleconsulting.bitacora.storage.LocalStorage;
import com.grayscaleconsulting.bitacora.util.Utils;
import com.yammer.metrics.core.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Exposes and API to interact with the main memory structure.
 * 
 * Created by ivaramme on 7/1/15.
 */
public class DataManagerImpl implements DataManager {
    private static Logger logger = LoggerFactory.getLogger(DataManagerImpl.class);

    private final Counter getRawRequests = Metrics.getDefault().newCounter(DataManagerImpl.class, "get-raw-requests");
    private final Counter getRawMissingRequests = Metrics.getDefault().newCounter(DataManagerImpl.class, "get-raw-missing-requests");
    private final Counter getValueRequests = Metrics.getDefault().newCounter(DataManagerImpl.class, "get-value-requests");
    private final Counter getValueMissingRequests = Metrics.getDefault().newCounter(DataManagerImpl.class, "get-value-missing-requests");
    private final Counter setValueFromLog = Metrics.getDefault().newCounter(DataManagerImpl.class, "set-value-from-log");
    private final Counter setValueFromCluster = Metrics.getDefault().newCounter(DataManagerImpl.class, "set-value-in-cluster");
    private final Counter deleteValue = Metrics.getDefault().newCounter(DataManagerImpl.class, "delete-value");
    private final Counter expiredKeyValue = Metrics.getDefault().newCounter(DataManagerImpl.class, "expired-key-value");


    private DataManagerExternal dataManagerExternal;
    private Producer producer;
    private Cache<String, KeyValue> data;
    private LocalStorage storage;

    public DataManagerImpl() {
        Properties props = Utils.loadProperties();
        long maxSize = 5000L;
        
        try {
            maxSize = Long.parseLong(props.getProperty("cache.items.max"));
        } catch (NumberFormatException nfe) {}
        
        this.data = CacheBuilder.
                newBuilder().
                maximumSize(maxSize).
                recordStats().
                build();
    }

    @Override
    public boolean contains(String key) {
        if(null == data.getIfPresent(key)) {
            if(null != storage) { // check present in local persisted cache
                try {
                    if(null != storage.get(key)) {
                        return true;
                    }
                } catch (Exception e) { }
            }
            return false;
        } 
        
        return true;
    }

    @Override
    public KeyValue getRaw(String key) {
        return getRaw(key, true);
    }

    @Override
    public KeyValue getRaw(String key, boolean forwardIfMissing) {
        getRawRequests.inc();
        
        KeyValue keyValue = data.getIfPresent(key);
        if(null == keyValue) {
            if(null != storage && storage.isReady()) {
                try {
                    keyValue = storage.get(key);
                } catch (Exception e) {
                    logger.error("Error loading key {} from local storage", key);
                }
            }
        }
        
        if(keyValue != null) {
            if(isValidKeyValue(keyValue)) {
                return keyValue;
            }
        } else if(forwardIfMissing && null != dataManagerExternal) {
            // Send query to cluster
            ExternalRequest request = dataManagerExternal.initiateExternalRequest(key);
            if(request != null && dataManagerExternal.isStillValidRequest(key, request.getToken())) {
                dataManagerExternal.invalidateExternalRequest(key);
                keyValue = request.getKeyValue();
                if (null != keyValue && isValidKeyValue(keyValue)) {
                        setFromCluster(keyValue);
                        return keyValue;
                }
            }
        }

        getRawMissingRequests.inc();
        return null;
    }

    @Override
    public String get(String key) {
        return get(key, true);
    }

    @Override
    public String get(String key, boolean forwardIfMissing) {
        getValueRequests.inc();
        
        KeyValue keyValue = getRaw(key, forwardIfMissing);
        if(keyValue != null) {
            keyValue.getStats().incRequests();
            return keyValue.getValue();
        }
        
        getValueMissingRequests.inc();
        return null;
    }

    @Override
    public void set(String key, String value) {
        KeyValue keyValue = KeyValue.createNewKeyValue(key, value, System.currentTimeMillis(), KeyValue.TTL_FOREVER);
        if(null != producer) {
            producer.publish(keyValue);
        } else {
            data.put(key, keyValue);
        }
    }

    @Override
    public KeyValue setFromCluster(KeyValue value) {
        logger.info("Attempting to set key: " + value.getKey() + " from another node in the cluster");
        
        if(value.getTtl() != KeyValue.TTL_EXPIRED && isValidKeyValue(value)) {
            value.setSource(KeyValue.SOURCE_CLUSTER);
            try {
                data.get(value.getKey(), new Callable<KeyValue>() {
                    @Override
                    public KeyValue call() {
                        setValueFromCluster.inc();
                        return value;
                    }
                });
                return value;
            } catch (ExecutionException e) {
                logger.error("Unable to add to local cache from cluster due to error with local cache: {}", e);
            }
        } 
        
        return null;
    }

    @Override
    public KeyValue setFromLog(KeyValue value) {
        return setFromLog(value.getKey(), value);
    }
    
    @Override
    public KeyValue setFromLog(String key, KeyValue value) {
        if(null != value) {
            setValueFromLog.inc();
            
            if(null != dataManagerExternal) {
                dataManagerExternal.invalidateExternalRequest(key); // invalidate any pending RPC call for this key
            }

            logger.info("Setting key: " + key + " from log");
            value.setSource(KeyValue.SOURCE_LOG);
            data.put(key, value);
            
            if(null != storage && storage.isReady()) {
                try {
                    storage.set(key, value);
                } catch (Exception e) {
                    logger.error("Error persisting object to local storage {}", e);
                }
            }
        } else {
            internalDeleteKey(key);
        }
        
        return value;
    }
    

    private void internalDeleteKey(String key) {
        if(null != storage && storage.isReady()) {
            try {
                storage.delete(key);
            } catch (Exception e) {
                logger.error("Error deleting key {} from local storage.", key);
            }
        }
            
        data.invalidate(key);
    }

    @Override
    public void delete(String key) {
        deleteValue.inc();
        
        KeyValue value = getRaw(key, true);
        if(null != value) {
            value.expire();
            producer.publish(value.getKey(), null);
        }
    }

    @Override
    public boolean useLocal() {
        if(null != storage && storage.isValid()) {
            return true;
        }
        return false;
    }
    
    @Override
    public void setExternal(DataManagerExternal external) {
        this.dataManagerExternal = external;
    }

    @Override
    public void setProducer(Producer producer) {
        this.producer = producer;
    }
    
    @Override
    public void setLocalStorage(LocalStorage storage) {
        this.storage = storage;
    }
    
    private boolean isValidKeyValue(KeyValue keyValue) {
        if(keyValue.getTtl() == KeyValue.TTL_FOREVER || keyValue.getTtl() >= System.currentTimeMillis()) {
            return true;
        }

        expiredKeyValue.inc();
        return false;
    }

}
