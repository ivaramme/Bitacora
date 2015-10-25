package com.grayscaleconsulting.bitacora.data.external;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.grayscaleconsulting.bitacora.model.KeyValue;
import com.grayscaleconsulting.bitacora.rpc.HttpRPCHandler;
import com.grayscaleconsulting.bitacora.util.Utils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Represents an HTTP request to other nodes for an specific piece of data. Requests are timed out after `external.request.timeout`
 * 
 * <p>This class handles all the communication logic, response parsing and quorum.</p>
 * 
 * Created by ivaramme on 6/30/15.
 */
public class ExternalRequestTask implements Callable<ExternalRequest> {
    private static Logger logger = LoggerFactory.getLogger(ExternalRequestTask.class);
    
    private static PoolingHttpClientConnectionManager poolingCM;
    private static CloseableHttpClient client;
    private final RequestConfig requestConfig;

    private static ExecutorService executor = Executors.newFixedThreadPool(3);

    private List<String> endpoints;
    private String key;
    private double quorum;
    private List<Future<CloseableHttpResponse>> pendingRequests;
    private int timeout;
    
    private ExternalRequest request;
    
    private Gson jsonParser = new GsonBuilder().create();

    public ExternalRequestTask(final List<String> endpoints, final String key, final double quorum) {
        this.endpoints = endpoints;
        this.key = key;
        this.quorum = quorum;
        this.request = new ExternalRequest(key, endpoints.size());
        this.timeout = 60;
        try {
            timeout = Integer.parseInt(Utils.loadProperties().getProperty("external.request.timeout"));
        } catch (NumberFormatException nfe) { }

        poolingCM = new PoolingHttpClientConnectionManager();
        poolingCM.setValidateAfterInactivity(1000);

        requestConfig = RequestConfig.custom()
                .setSocketTimeout(timeout)
                .setConnectTimeout(timeout / 3)
                .setConnectionRequestTimeout(timeout * 2)
                .build();
        
        client = HttpClients.custom()
                .setConnectionManager(poolingCM)
                .setMaxConnPerRoute(10)
                .setDefaultRequestConfig(requestConfig)
                .setMaxConnTotal(20)
                .build();
    }

    public ExternalRequest call() {
        final int requestsMade = endpoints.size();
        Preconditions.checkArgument(requestsMade > 0);

        // Define requests
        List<Callable<CloseableHttpResponse>> requests = new ArrayList<>();
        for(String endpoint : endpoints) {
            requests.add(new Callable<CloseableHttpResponse>() {
                private HttpGet request;

                @Override
                public CloseableHttpResponse call() throws Exception {
                    request = new HttpGet("http://" + endpoint + HttpRPCHandler.RPC_CLUSTER_ENDPOINT + "?key=" + key);
                    long start = System.currentTimeMillis();
                    try {
                        return client.execute(request);
                    } catch (IOException e) {
                        logger.error("Timed out an external request to endpoint {} with duration {}", endpoint, ( System.currentTimeMillis() - start ));
                        e.printStackTrace();
                    }
                    return null;
                }
            });
        }

        // Send requests to a completion service executor
        pendingRequests = new ArrayList<>();
        CompletionService<CloseableHttpResponse> completionService = new ExecutorCompletionService<CloseableHttpResponse>(executor);
        for(Callable<CloseableHttpResponse> req : requests) {
            pendingRequests.add(completionService.submit(req));
        }

        // Process responses
        int requestsCompleted = 0;
        int successRequests = 0;
        int abortedRequests = 0;
        List<KeyValue> results = new ArrayList<>();
        
        // Wait for all requests or until we have enough quorum of success responses
        while(requestsCompleted < requestsMade) {
            HttpResponse response = null;
            try {
                Future<CloseableHttpResponse> resp = completionService.take();
                if(null != resp) {
                    response = resp.get();
                }
            } catch (InterruptedException|ExecutionException e) {
                logger.error("Error waiting for responses to external nodes, possibly tasks have been cancelled.");
                e.printStackTrace();
                //break;
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
            } else {
                abortedRequests++;
            }
        }
        
        if(requestsCompleted > 0 && !(((double)((double)successRequests/(double)requestsCompleted)) > quorum)) {
            successRequests = 0;
        }
        
        request.setFailedRequests(requestsMade-successRequests);
        request.setSuccessfulRequests(successRequests);
        request.setAbortedRequests(abortedRequests);
        
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
        if(null != pendingRequests) {
            pendingRequests.forEach(future -> future.cancel(true) );
        }
    }
    
}


