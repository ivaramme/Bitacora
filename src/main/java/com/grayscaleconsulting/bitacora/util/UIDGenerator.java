package com.grayscaleconsulting.bitacora.util;

import com.google.common.base.Preconditions;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Util class to generate Unique Ids
 * 
 * Created by ivaramme on 9/29/15.
 */
public class UIDGenerator {
    private final long CURRENT_EPOCH = 1388534400000L;
    private AtomicLong sequence = new AtomicLong();
    protected String nodeName = null;
    private static UIDGenerator instance;
    
    protected UIDGenerator() {
    }

    public static UIDGenerator getInstance() {
        if(null == instance)  {
            instance = new UIDGenerator();
        } 
        return instance;
    }
    
    public void setNodeName(String nodeName) {
        if(null == this.nodeName) {
            this.nodeName = nodeName;
        }
    }

    /**
     * Updates the sequence number and returns its value
     * @return
     */
    protected long getNextSequence() {
        return sequence.incrementAndGet();
    }

    /**
     * Returns a sequential UID based on the timestamp + node name  + (sequence number % 512)
     *
     * @return
     */
    public long getUID() {
        Preconditions.checkNotNull(nodeName);
        
        long res = 0;
        res |= getCurrentTime() << 23;
        res |= nodeName.chars().sum() << 10;
        res |= getNextSequence() % 1024; // 1024 messages with the same ID at the same time

        return res;
    }
    
    protected long getCurrentTime() {
        return  (System.currentTimeMillis() - CURRENT_EPOCH);
    }
    
}
