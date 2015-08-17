package com.grayscaleconsulting.bitacora.rpc;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;

import java.net.InetSocketAddress;

import com.grayscaleconsulting.bitacora.data.DataManager;
import com.grayscaleconsulting.bitacora.rpc.avro.RPCHandlerAvro;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Socket RPC implementation using AVRO
 * 
 * Created by ivaramme on 8/13/15.
 */
public class AvroSocketRPCHandler implements RPCHandlerAvro {
    private static final Logger logger = LoggerFactory.getLogger(AvroSocketRPCHandler.class);
    
    private final Server server;
    private final DataManager dataManager;
    private final int port;

    public AvroSocketRPCHandler(DataManager dataManager, int port) {
        this.dataManager = dataManager;
        this.port = port;
        
        logger.info("Starting Avro socket server port: " + port);
        server = new NettyServer(new SpecificResponder(RPCHandlerAvro.class, this), new InetSocketAddress(port));
    }

    @Override
    public CharSequence getValue(CharSequence key) throws AvroRemoteException {
        logger.info("SocketRPCServer:getValue " + key.toString());
        return dataManager.get(key.toString());
    }
}
