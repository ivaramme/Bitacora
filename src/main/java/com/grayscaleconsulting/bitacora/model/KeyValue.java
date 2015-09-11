package com.grayscaleconsulting.bitacora.model;

import java.io.Serializable;
import java.util.UUID;

/**
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
        this(key, value, timestamp, source, ttl, UUID.randomUUID().toString());
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

    /**
     * Checks whether a String has the right format or not to be parsed as a KeyValue object
     * @param message
     * @return
     */
    public static boolean isValidKeyValue(String message) {
        if(null == message || 
                message.isEmpty() ||
                message.indexOf(SEPARATOR) == -1 ||
                message.length() <= 2 ||
                message.charAt(message.length()-1) == SEPARATOR ||
                message.split("\\"+String.valueOf(SEPARATOR)).length != 5 ) {
            return false;
        }
        
        try {
            Integer.valueOf(message.substring(message.lastIndexOf(SEPARATOR)+1));
        } catch (NumberFormatException nfe) {
            return false; // invalid timestamp
        }
        
        return true;
    }

    public void expire() {
        this.ttl = TTL_EXPIRED;
    }
}
