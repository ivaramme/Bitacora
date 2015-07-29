package com.grayscaleconsulting.streaming.data.metadata;

import java.io.Serializable;

/**
 * Created by ivaramme on 6/29/15.
 */
public class KeyValueStats implements Serializable {
    private int requests = 0;
    private int countMissing = 0;
    private long lastModified = 0;
    private long lastAccessed = 0;
    
    public int incRequests() {
        requests++;
        return requests;
    }
    
    public int incMissing() {
        countMissing++;
        return countMissing;
    }

    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    public void setLastAccessed(long lastAccessed) {
        this.lastAccessed = lastAccessed;
    }

    public int getRequests() {
        return requests;
    }

    public int getCountMissing() {
        return countMissing;
    }

    public long getLastModified() {
        return lastModified;
    }

    public long getLastAccessed() {
        return lastAccessed;
    }
}
