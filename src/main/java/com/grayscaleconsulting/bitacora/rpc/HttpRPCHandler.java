package com.grayscaleconsulting.bitacora.rpc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.grayscaleconsulting.bitacora.data.DataManager;
import com.grayscaleconsulting.bitacora.kafka.Consumer;
import com.grayscaleconsulting.bitacora.metrics.Metrics;
import com.grayscaleconsulting.bitacora.model.KeyValue;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Creates an HTTP interface that exposes methods to share data with other nodes in the cluster as well as 
 * with consumers of this service
 *
 * Created by ivaramme on 7/3/15.
 */
public class HttpRPCHandler extends AbstractHandler {
    protected static final Logger logger = LoggerFactory.getLogger(HttpRPCHandler.class);

    public static final String HEALTH_ENDPOINT      = "/health";
    public static final String RPC_ENDPOINT         = "/rpc";
    public static final String RPC_CLUSTER_ENDPOINT = "/rpc/cluster";

    private final Counter requests = Metrics.getDefault().newCounter(HttpRPCHandler.class, "rpc-http-requests");
    private final Counter clusterRequests = Metrics.getDefault().newCounter(HttpRPCHandler.class, "rpc-http-cluster-requests");
    private final Counter publicRequests = Metrics.getDefault().newCounter(HttpRPCHandler.class, "rpc-http-public-requests");
    private final Counter missedRequests = Metrics.getDefault().newCounter(HttpRPCHandler.class, "rpc-http-missedRequests-requests");
    private final Counter errors = Metrics.getDefault().newCounter(HttpRPCHandler.class, "rpc-http-error-requests");

    private final Timer requestDuration = Metrics.getDefault().newTimer(HttpRPCHandler.class, "rpc-request-latency");
    
    private Gson jsonParser;
    private DataManager dataManager;
    private Consumer consumer;
    
    public HttpRPCHandler(DataManager dataManager, final Consumer consumer) {
        this.dataManager = dataManager;
        this.consumer = consumer;
        jsonParser = new GsonBuilder().create();
    }
    
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        if(target.startsWith(HEALTH_ENDPOINT)) {
            returnHealth(response);
            response.flushBuffer();
            baseRequest.setHandled(true);
            return;
        }
        
        if(target.startsWith(RPC_ENDPOINT) || target.startsWith(RPC_CLUSTER_ENDPOINT)) {
            logger.info("New request for endpoint: " + target);
            requests.inc();
            final TimerContext context = requestDuration.time();

            try {
                if(null == dataManager) {
                    logger.error("RPC service is not configured correctly");
                    sendErrorResponse(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                    context.stop();
                    baseRequest.setHandled(true);
                    return;
                }

                if(request.getMethod().equalsIgnoreCase("GET")) {
                    String key = request.getParameter("key");
                    if (null == key || key.isEmpty() || key.trim().isEmpty()) {
                        logger.info("Missing request information");
                        sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST);
                        response.flushBuffer();
                        context.stop();
                        return;
                    }

                    // Forward request to cluster in case this is a normal RPC request. 
                    // If the request came from the cluster rpc endpoint, do not forward
                    boolean forwardRPCRequest = endpoint.equals(RPC_ENDPOINT);
                    KeyValue keyValue = dataManager.getRaw(key, forwardRPCRequest);
                    if (null == keyValue) {
                        logger.info(key + " was not found, returning null value");
                        missedRequests.inc();
                        sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND, "{ \"message\": \"Key " + key + " not found\"} ");
                    } else {
                        sendResponse(keyValue, resp, forwardRPCRequest);
                    }
                    if(forwardRPCRequest)
                        publicRequests.inc(); // public requests will always forward
                    else
                        clusterRequests.inc();
                    
                } else
                // Set a new value
                if(request.getMethod().equalsIgnoreCase("POST")) {
                    logger.info("RPC: Creating a new key");
                    String key = request.getParameter("key");
                    String value = request.getParameter("value");
                    if ((null != key || !key.isEmpty() || !key.trim().isEmpty()) &&
                            (null != value || !value.isEmpty() || !value.trim().isEmpty())) {
                        dataManager.set(key, value);
                    }
                } else
                // Delete value
                if(request.getMethod().equalsIgnoreCase("DELETE")) {
                    logger.info("RPC: Deleting key");
                    String key = request.getParameter("key");
                    if (null != key || !key.isEmpty() || !key.trim().isEmpty()) {
                        dataManager.delete(key);
                    }
                } else {
                    logger.info("Unknown method for endpoint: " + endpoint + " -> " + request.getMethod());
                    sendErrorResponse(resp, HttpServletResponse.SC_NOT_IMPLEMENTED);
                    errors.inc();
                }
            } catch (IOException ioe) {
                logger.error("Error sending response to request. ");
                ioe.printStackTrace();
                errors.inc();
            }

            context.stop();
            baseRequest.setHandled(true);
        }else {
            logger.info("Invalid endpoint: " + target);
            sendErrorResponse(response, HttpServletResponse.SC_NOT_IMPLEMENTED);
            errors.inc();
            baseRequest.setHandled(true);
        }

        response.flushBuffer();
    }

    private void sendResponse(KeyValue value, HttpServletResponse resp, boolean forwardRequest) throws IOException {
        logger.info(value + " was found " + (!forwardRequest ? "in local cache returning to fellow node" : ""));

        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println(jsonParser.toJson(value, KeyValue.class));
    }
    
    private void sendErrorResponse(HttpServletResponse resp, int code) throws IOException {
        sendErrorResponse(resp, code, "{ \"message\": \"Error: "+ code +"\"} ");
    }

    private void sendErrorResponse(HttpServletResponse resp, int code, String message) throws IOException {
        response.setContentType("application/json");
        response.setStatus(code);
        response.getWriter().print(message);
    }
    
    protected void returnHealth(HttpServletResponse resp) throws IOException {
        if((null != dataManager) && (null != consumer) && (consumer.isReady())) {
            response.setStatus(200);
            response.getWriter().print("OK");
        } else {
            response.setStatus(500);
            response.getWriter().print("DOWN");
        }
    }
}
