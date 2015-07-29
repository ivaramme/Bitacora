package com.grayscaleconsulting.streaming.cluster.nodes;

/**
 * Created by ivaramme on 6/30/15.
 */
public class NodeStats {
    int countRequests = 0;
    int countErrors = 0;
    int lastStatusResponse = 0;
    int lastRequestTime = 0;

    public int incRequests() {
        countRequests++;
        return countRequests;
    }

    public int incErrors() {
        countErrors++;
        return countErrors;
    }

    public void setLastStatusResponse(int lastStatusResponse) {
        this.lastStatusResponse = lastStatusResponse;
    }

    public void setLastRequestTime(int lastRequestTime) {
        this.lastRequestTime = lastRequestTime;
    }

    public int getLastRequestTime() {
        return lastRequestTime;
    }

    public int getCountRequests() {
        return countRequests;
    }

    public int getCountErrors() {
        return countErrors;
    }

    public int getLastStatusResponse() {
        return lastStatusResponse;
    }
}
