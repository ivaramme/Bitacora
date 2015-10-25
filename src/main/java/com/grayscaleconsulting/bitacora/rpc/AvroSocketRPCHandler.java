package com.grayscaleconsulting.bitacora.rpc;

import com.grayscaleconsulting.bitacora.data.DataManager;
import com.grayscaleconsulting.bitacora.rpc.avro.RPCHandlerAvro;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Socket RPC implementation using AVRO
 * 
 * Created by ivaramme on 8/13/15.
 */
public class AvroSocketRPCHandler implements RPCHandlerAvro {
    private static final Logger logger = LoggerFactory.getLogger(AvroSocketRPCHandler.class);
    
    private Server server;
    private final DataManager dataManager;
    private final int port;

    public AvroSocketRPCHandler(DataManager dataManager, int port) {
        this.dataManager = dataManager;
        this.port = port;
    }

    public void start() {
        logger.info("Starting Avro socket server port: " + port);
        server = new NettyServer(new SpecificResponder(RPCHandlerAvro.class, this), new InetSocketAddress(port));
    }
    
    @Override
    public CharSequence getValue(CharSequence key) throws AvroRemoteException {
        logger.info("AvroRPCServer:getValue {}",key.toString());
        return dataManager.get(key.toString());
    }

    @Override
    public Void setValue(CharSequence key, CharSequence value) throws AvroRemoteException {
        logger.info("AvroRPCServer:setValue for key {}" + key.toString() );
        dataManager.set(key.toString(), value.toString());
        return null;
    }

    @Override
    public Void delete(CharSequence key) throws AvroRemoteException {
        logger.info("AvroRPCServer:deleteValue for key {}" + key.toString() );
        dataManager.delete(key.toString());
        return null;
    }

    public void shutdown() {
        server.close();
    }
}
