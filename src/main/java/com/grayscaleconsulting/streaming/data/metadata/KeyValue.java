package com.grayscaleconsulting.streaming.data.metadata;

import java.io.Serializable;

/**
 * Created by ivaramme on 6/29/15.
 */
public class KeyValue implements Serializable, Comparable<KeyValue> {
    public static int SOURCE_MISSING    = 0;
    public static int SOURCE_KAFKA      = 1;
    public static int SOURCE_CLUSTER    = 2;
    
    public static char SEPARATOR = '^'; // TODO: change this to a system wide separator
    
    public static int TTL_EXPIRED       = -1;
    public static int TTL_FOREVER       = 0;
    
    private String key;
    private String value;
    private long timestamp;
    private int source;
    private int ttl; 
    transient private KeyValueStats stats;

    private KeyValue(String key, String value, long timestamp, int source, int ttl) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.source = source;
        this.ttl = ttl;
        
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
    
    public static KeyValue createKeyValueFromLog(String key, String value, long timestamp, int ttl) {
        return new KeyValue(key, value, timestamp, SOURCE_KAFKA, ttl);
    }

    public static KeyValue createKeyValueFromClusterValue(String key, String value, long timestamp, int ttl) {
        return new KeyValue(key, value, timestamp, SOURCE_CLUSTER, ttl);
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

    public int getTtl() {
        return ttl;
    }

    @Override
    public int compareTo(KeyValue other) {
        if(other.getTimestamp() > getTimestamp()) 
            return (int) (other.getTimestamp() - getTimestamp());
        else if(other.getTimestamp() < getTimestamp())
            return (int) (other.getTimestamp() - getTimestamp());
        else
            return 0;
    }
    
    public String serialize() {
        return getKey() + SEPARATOR + getValue() + SEPARATOR + System.currentTimeMillis() + SEPARATOR + getTtl();
    }

    /**
     * Checks whether a String has the right format or not to be parsed as a KeyValue object
     * @param message
     * @return
     */
    public static boolean isValidKeyValue(String message) {
        if(message.isEmpty() ||
                message.indexOf(SEPARATOR) == -1 ||
                message.length() <= 2 ||
                message.charAt(message.length()-1) == SEPARATOR ||
                message.split("\\"+String.valueOf(SEPARATOR)).length != 4 ) {
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
