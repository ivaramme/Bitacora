package com.grayscaleconsulting.bitacora.client;

import com.grayscaleconsulting.bitacora.rpc.avro.RPCHandlerAvro;
import com.grayscaleconsulting.bitacora.util.Utils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation of a Binary Client that uses AVRO for RPC serialization.
 * 
 * <p>The client connects to Zookeeper to find out the list of available servers waiting for connections
 * and handles certain cases for reconnection or at least notification of connection issues via exceptions </p>
 * *
 * Created by ivaramme on 8/29/15.
 */
public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);
    
    private final int MAX_RETRIES = 3;
    
    private ZkClient zkClient;
    private NettyTransceiver client; 
    private RPCHandlerAvro proxy;
    private boolean connected  = false;
    private int retries = 0;
    
    private List<String> serversAvailable;

    public Client(String zookeeperHosts) {
        zkClient = new ZkClient(zookeeperHosts);
        zkClient.subscribeChildChanges(Utils.SOCKET_AVAILABLE_SERVERS, new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                logger.info("Received list of available servers");
                serversAvailable = currentChilds;
            }
        });
    }
    
    private void connect() throws InterruptedException {
        if(connected) {
            return;
        }
        
        String host = findServerAvailable();
        int port = -1;
        if(null != host && host.indexOf(':') > -1) {
            try {
                port = Integer.parseInt(host.split(":")[1]);
                host = host.split(":")[0];
            } catch (NumberFormatException nfe) { }
        }
        
        if(-1 != port) {
            try {
                client = new NettyTransceiver(new InetSocketAddress(host, port));
                proxy = (RPCHandlerAvro) SpecificRequestor.getClient(RPCHandlerAvro.class, client);
            } catch (IOException ioe) {
                logger.error("Error connecting to server: {}:{}", host, port);
                Thread.sleep(1000);
                return;
            }

            connected = true;
        } else if(retries == MAX_RETRIES) {
            logger.error("Unable to start find a valid host to connect to, aborting.");
            throw new RuntimeException("Unable to start client, can't find a valid host to connect to, aborting.");
        }
    }
    
    public void shutdown() {
        checkNotNull(client);
        client.close(true);
    }

    public String get(String key) throws ClientConnectionException {
        checkConnection();
        checkState(connected);

        CharSequence ret;
        try {
            ret = proxy.getValue(key);
            if(null != ret) {
                return ret.toString();
            }
        } catch (AvroRemoteException e) {
            logger.error("Error getting key: {}, exception: {}", key, e);
            throw new ClientConnectionException("Unable to get the value for this key, try reconnecting");
        }
        return null;
    }
    
    
    public void set(String key, String value) throws ClientConnectionException {
        checkConnection();
        checkState(connected);

        try {
            proxy.setValue(key, value);
        } catch (AvroRemoteException e) {
            logger.error("Error setting value for key: {}", key);
            throw new ClientConnectionException("Unable to set the value for this key, try reconnecting");
        }
    }
    
    public void delete(String key) throws ClientConnectionException {
        checkConnection();
        checkState(connected);

        try {
            proxy.delete(key);
        } catch (AvroRemoteException e) {
            logger.error("Error deleting value for key: {}", key);
            throw new ClientConnectionException("Unable to delete the value for this key, try reconnecting");
        }
    }
    
    private String findServerAvailable() {
        checkNotNull(zkClient, "Zookeeper client can't be null");

        if(null == serversAvailable || serversAvailable.size() == 0) {
            logger.info("Fetching available servers");
            serversAvailable = zkClient.getChildren(Utils.SOCKET_AVAILABLE_SERVERS);
        }
        
        // For now, return a random number from the list of available ones
        // but we can introduce some sort of load balancing here
        if(null != serversAvailable && serversAvailable.size() > 0) {
            Random random = new Random();
            return serversAvailable.get(random.nextInt(serversAvailable.size()));
        } 
        
        return null;
    }
    
    private void checkConnection() {
        if(connected) {
            retries = 0;
            return;
        }

        while(retries < MAX_RETRIES && !connected) {
            try {
                retries++;
                connect();
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("Error while trying to reconnect: {}", e);
            }
        }
    }
    
    
}
