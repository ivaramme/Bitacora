package com.grayscaleconsulting.bitacora;

import com.grayscaleconsulting.bitacora.client.Client;
import com.grayscaleconsulting.bitacora.client.ClientConnectionException;

/**
 * Example class with usage of the binary Client class
 * Created by ivaramme on 8/13/15.
 */
public class AvroClient {
    public static void main(String[] args) throws ClientConnectionException, InterruptedException {
        String zookeeperHosts = System.getenv("ZOOKEEPER_HOSTS");
        if (null == zookeeperHosts)
            zookeeperHosts = "localhost:2181";
        
        Client client = new Client(zookeeperHosts);
        
        String val = client.get("a_key");
        
        if(null == val) {
            client.set("a_key", "a_value");
            Thread.sleep(500); // eventual consistency :)
        }
        System.out.println(client.get("a_key"));

        client.set("another_key", "another_value");
        
        System.out.println(client.get("another_key"));
        System.out.println(client.get("a_key"));
        System.out.println(client.get("a_key"));
        client.delete("another_key");
        client.shutdown();
    }
}
