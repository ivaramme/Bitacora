package com.grayscaleconsulting.streaming.data;

import com.grayscaleconsulting.streaming.kafka.Producer;
import com.grayscaleconsulting.streaming.data.external.ExternalRequest;
import com.grayscaleconsulting.streaming.data.metadata.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by ivaramme on 7/1/15.
 */
public class DataManagerImpl implements DataManager {
    private static Logger logger = LoggerFactory.getLogger(DataManagerImpl.class);

    private DataManagerExternal dataManagerExternal;
    private Producer producer;
    private Map<String, KeyValue> data;
    
    public DataManagerImpl() {
        this.data = new ConcurrentHashMap<>();
    }

    @Override
    public boolean contains(String key) {
        // TODO: implement this as an RPC or remove it from API
        return data.containsKey(key);
    }

    @Override
    public KeyValue getRaw(String key) {
        return getRaw(key, true);
    }

    @Override
    public KeyValue getRaw(String key, boolean forwardIfMissing) {
        KeyValue keyValue = data.get(key);
        if(keyValue != null) {
            return keyValue;
        } else if(forwardIfMissing && null != dataManagerExternal) {
            // Execute query to cluster
            ExternalRequest request = dataManagerExternal.initiateExternalRequest(key);
            if(request != null && dataManagerExternal.isStillValidRequest(key, request.getToken())) {
                keyValue = request.getKeyValue();
                if (keyValue != null) {
                    setFromCluster(keyValue);
                    return keyValue;
                }
            }
        }
        return null;
    }

    @Override
    public String get(String key) {
        return get(key, true);
    }

    @Override
    public String get(String key, boolean forwardIfMissing) {
        KeyValue keyValue = getRaw(key, forwardIfMissing);
        if(keyValue != null) {
            return keyValue.getValue();
        }
        return null;
    }

    @Override
    public void set(String key, String value) {
        KeyValue keyValue = KeyValue.createKeyValueFromClusterValue(key, value, System.currentTimeMillis(), KeyValue.TTL_FOREVER);
        if(null != producer) {
            producer.publish(keyValue);
        }
    }

    @Override
    public void setFromCluster(KeyValue value) {
        logger.info("Setting key: " + value.getKey() + " from another node in the cluster");
        
        if(value.getTtl() != KeyValue.TTL_EXPIRED) {
            value.setSource(KeyValue.SOURCE_CLUSTER);
            data.put(value.getKey(), value);
        } 
    }

    @Override
    public void setFromLog(KeyValue value) {
        if(null != dataManagerExternal) {
            dataManagerExternal.initiateExternalRequest(value.getKey());
        }
        
        if(value.getTtl() != KeyValue.TTL_EXPIRED) {
            logger.info("Setting key: " + value.getKey() + " from log");
            value.setSource(KeyValue.SOURCE_KAFKA);
            data.put(value.getKey(), value);
        } else {
            internalDeleteKey(value);
        }
    }

    private void internalDeleteKey(KeyValue value) {
        data.remove(value.getKey());
    }

    @Override
    public void delete(String key) {
        KeyValue value = getRaw(key, true);
        if(null != value) {
            value.expire();
            producer.publish(value);
        }
    }

    @Override
    public void setExternal(DataManagerExternal external) {
        this.dataManagerExternal = dataManagerExternal;
    }

    public void setProducer(Producer producer) {
        this.producer = producer;
    }
}
