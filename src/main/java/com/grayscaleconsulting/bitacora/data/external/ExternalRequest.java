package com.grayscaleconsulting.bitacora.data.external;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.grayscaleconsulting.bitacora.data.metadata.KeyValue;

/**
 * An instance of the class ExternalRequest represents an external request made to another node
 * in a cluster with the intention of fetching data
 * 
 * Created by ivaramme on 6/30/15.
 */
public class ExternalRequest {
    private static Logger logger = LoggerFactory.getLogger(ExternalRequest.class);

    public static int STATUS_OK                    = 200;
    public static int STATUS_UNKNOWN               = -1;
    public static int STATUS_CANCELLED             = 2;
    public static int STATUS_FAILED             = 3;
    public static int STATUS_INCOMPLETE            = 4;

    private KeyValue keyValue;
    private String key;
    private long started;
    private long finished = -1;
    private int status = STATUS_UNKNOWN;
    private UUID token; // acts as token to check if request is still valid
    private int totalRequests = 0;
    
    private Map<String, String> endpointResponse;

    public ExternalRequest(String key, int totalRequests) {
        this.key = key;
        this.totalRequests = totalRequests;
        this.started = System.currentTimeMillis();
        this.token = UUID.randomUUID();
        this.endpointResponse = new HashMap<>();
    }

    public long elapsed() {
        return System.currentTimeMillis() - started;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public KeyValue getKeyValue() {
        return keyValue;
    }

    public long getStarted() {
        return started;
    }

    public int getStatus() {
        return status;
    }

    public long getFinished() {
        return finished;
    }

    public UUID getToken() {
        return token;
    }

    public void setEndpointResponse(String endpoint, String content) {
        endpointResponse.put(endpoint, content);
    }

    public void setKeyValue(KeyValue keyValue) {
        this.keyValue = keyValue;
    }

    public void complete() {
        this.finished = System.currentTimeMillis();
    }

    public void cancel() {
        this.status = STATUS_CANCELLED;
    }

    public void incomplete() {
        this.status = STATUS_INCOMPLETE;
    }

    public int getTotalRequests() {
        return totalRequests;
    }

    public String getKey() {
        return key;
    }
}
