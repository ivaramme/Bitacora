package com.grayscaleconsulting.streaming;

import com.grayscaleconsulting.streaming.rpc.avrogenerated.RPCHandlerAvro;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by ivaramme on 8/13/15.
 */
public class SocketClient {
    public static void main(String[] args) throws IOException {
        NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(8183));
        RPCHandlerAvro proxy = (RPCHandlerAvro) SpecificRequestor.getClient(RPCHandlerAvro.class, client);
        
        System.out.println(proxy.getValue("one22"));
    }
}
