package com.grayscaleconsulting.bitacora.client;

/**
 * Represents an exception thrown by the binary socket Client.
 * 
 * Created by ivaramme on 8/31/15.
 */
public class ClientConnectionException extends Exception {
    
    public ClientConnectionException(String message) {
        super(message);
    }
}
