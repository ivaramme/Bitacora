package com.grayscaleconsulting.bitacora.model;

import com.grayscaleconsulting.bitacora.util.UIDGenerator;

import java.io.Serializable;

/**
 * A KeyValue instance represents and contains the data that users want to persist or query.
 *
 * Created by ivaramme on 6/29/15.
 */
public class KeyValue implements Serializable, Comparable<KeyValue> {
    public static final int SOURCE_MISSING    = 0;
    public static final int SOURCE_LOG        = 1;
    public static final int SOURCE_CLUSTER    = 2;
    public static final int SOURCE_PRODUCER   = 3;

    public static char SEPARATOR = '^'; // TODO: change this to a system wide separator
    
    public static int TTL_EXPIRED       = -1;
    public static int TTL_FOREVER       = 0;
    
    private String key;
    private String value;
    private long timestamp;
    private int source;
    private long ttl;
    
    private String uuid;

    transient private KeyValueStats stats;

    private KeyValue(String key, String value, long timestamp, int source, long ttl) {
        this(key, value, timestamp, source, ttl, String.valueOf(UIDGenerator.getInstance().getUID()));
    }

    private KeyValue(String key, String value, long timestamp, int source, long ttl, String uuid) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.source = source;
        this.ttl = ttl;
        this.uuid = uuid;

        this.stats = new KeyValueStats();
    }
    
    private KeyValue(String key) {
        this.key = key;
        this.value = null;
        this.timestamp = System.currentTimeMillis();
        
        stats = new KeyValueStats();
        stats.incRequests();
        stats.incMissing();
        
        this.source = SOURCE_MISSING;
    }

    public static KeyValue createMissingKeyValue(String key) {
        return new KeyValue(key);
    }
    
    public static KeyValue createKeyValueFromLog(String key, String value, long timestamp, long ttl, String uuid) {
        return new KeyValue(key, value, timestamp, SOURCE_LOG, ttl, uuid);
    }

    public static KeyValue createKeyValueFromClusterValue(String key, String value, long timestamp, long ttl, String uuid) {
        return new KeyValue(key, value, timestamp, SOURCE_CLUSTER, ttl, uuid);
    }

    public static KeyValue createNewKeyValue(String key, String value, long timestamp, long ttl) {
        return new KeyValue(key, value, timestamp, SOURCE_PRODUCER, ttl);
    }

    public static KeyValue createNewKeyValue(String key, String value, long timestamp, long ttl, int source) {
        return new KeyValue(key, value, timestamp, source, ttl);
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getSource() {
        return source;
    }
    
    public void setSource(int source) {
        this.source = source;
    }

    public long getTtl() {
        return ttl;
    }

    public String getUuid() {
        return uuid;
    }

    @Override
    public int compareTo(KeyValue other) {
        return (int) (other.getTimestamp() - getTimestamp());
    }
    
    public String serialize() {
        return getKey() + SEPARATOR + getValue() + SEPARATOR + System.currentTimeMillis() + SEPARATOR + getUuid() + SEPARATOR + getTtl();
    }
    
    public KeyValueStats getStats() {
        if(null == stats) {
            stats = new KeyValueStats();
        }
        
        return stats;
    }

    public void expire() {
        this.ttl = TTL_EXPIRED;
    }
}
