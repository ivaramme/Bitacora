package com.grayscaleconsulting.bitacora.data.external;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import com.grayscaleconsulting.bitacora.data.metadata.KeyValue;
import com.grayscaleconsulting.bitacora.rpc.RPCUtils;

/**
 * Represents a formal request to other nodes for an specific piece of data. 
 * 
 * <p>This class handles all the communication logic, response parsing and quorum.</p>
 * 
 * Created by ivaramme on 6/30/15.
 */
public class ExternalRequestTask implements Callable<ExternalRequest> {
    private static Logger logger = LoggerFactory.getLogger(ExternalRequestTask.class);
    
    private static PoolingHttpClientConnectionManager poolingCM = new PoolingHttpClientConnectionManager();
    private static HttpClient client = HttpClients.custom()
            .setConnectionManager(poolingCM)
            .setMaxConnPerRoute(5)
            .setMaxConnTotal(5).build();

    private static ExecutorService executor = Executors.newFixedThreadPool(4);
    
    private List<String> endpoints;
    private String key;
    private double quorum;
    private List<Future<HttpResponse>> pendingRequests;
    
    private ExternalRequest request;
    
    private Gson jsonParser = new GsonBuilder().create();

    public ExternalRequestTask(final List<String> endpoints, final String key, final double quorum) {
        this.endpoints = endpoints;
        this.key = key;
        this.quorum = quorum;
        this.request = new ExternalRequest(key, endpoints.size());
    }

    public ExternalRequest call() {
        final int requestsMade = endpoints.size();
        Preconditions.checkArgument(requestsMade > 0);

        // Define requests
        List<Callable<HttpResponse>> requests = new ArrayList<>();
        for(String endpoint : endpoints) {
            requests.add(new Callable<HttpResponse>() {
                @Override
                public HttpResponse call() throws Exception {
                    HttpGet request = new HttpGet("http://"+endpoint + RPCUtils.RPC_CLUSTER_ENDPOINT+"?key=" + key);
                    try {
                        return client.execute(request);
                    }
                    catch(IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            });
        }

        // Send them to completion service
        pendingRequests = new ArrayList<>();
        CompletionService<HttpResponse> completionService = new ExecutorCompletionService<HttpResponse>(executor);
        for(Callable<HttpResponse> req : requests) {
            pendingRequests.add(completionService.submit(req));
        }

        // Process responses
        int requestsCompleted = 0;
        int successRequests = 0;
        List<KeyValue> results = new ArrayList<>();
        
        // Wait for all requests or until we have enough quorum of success responses
        while(requestsCompleted < requestsMade) {
            HttpResponse response = null;
            try {
                response = completionService.take().get();
            } catch (InterruptedException|ExecutionException e) {
                logger.error("Error waiting for responses to external nodes, possibly tasks have been cancelled.");
                e.printStackTrace();
                break;
            } 
            
            requestsCompleted++;
            if (response != null) {
                if(HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                    logger.info("Successful response from another node");
                    try {
                        String content = IOUtils.toString(response.getEntity().getContent()); 
                        results.add(jsonParser.fromJson(content, KeyValue.class));
                        successRequests++;
                    } catch (IOException e) {
                        logger.error("Unable to access response content from request");
                        e.printStackTrace();
                    }
                }
            }

        }
        
        if(requestsCompleted > 0 && !(((double)((double)successRequests/(double)requestsCompleted)) > quorum)) {
            successRequests = 0;
        }
        
        request.setFailedRequests(requestsMade-successRequests);
        request.setSuccessfulRequests(successRequests);
        
        if(0 < successRequests) {
            // Sort responses by timestamp or return an empty key if empty
            request.setKeyValue(results.stream()
                    .sorted((s1, s2) -> s1.compareTo(s2))
                    .findFirst()
                    .orElse(results.get(0)));
        }
        request.complete();
        
        logger.info("External request completed");
        return request;
    }
    
    public ExternalRequest getRequest() {
        return request;
    }
    
    public void cancel() {
        if(pendingRequests != null) {
            pendingRequests.forEach(future -> future.cancel(true) );
        }
    }
    
}


