package com.grayscaleconsulting.bitacora.rpc;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Utilitarian methods for RPC interaction
 * * *
 * Created by ivaramme on 7/2/15.
 */
public class RPCUtils {
    public static final String GET_ENDPOINT         = "/get";
    public static final String POST_ENDPOINT        = "/post";
    public static final String RPC_ENDPOINT         = "/rpc";
    public static final String RPC_CLUSTER_ENDPOINT = "/rpc/cluster";
    
    public static void sendNotFoundResponse(String key, HttpServletResponse resp) throws IOException {
        // TODO: this will probably fail, test it!
        resp.setContentType("application/javascript");
        resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Key "+ key +" not found");
    }

    public static void sendServerErrorResponse(HttpServletResponse resp) throws IOException {
        resp.setContentType("application/javascript");
        resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }

    public static void sendInvalidEndpoint(String endpoint, HttpServletResponse resp) throws IOException {
        resp.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED);
    }

    public static void sendMissingData(HttpServletResponse resp) throws IOException {
        resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
    }
}
