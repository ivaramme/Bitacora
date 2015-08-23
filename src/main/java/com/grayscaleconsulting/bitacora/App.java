package com.grayscaleconsulting.bitacora;

import com.grayscaleconsulting.bitacora.cluster.ClusterMembership;
import com.grayscaleconsulting.bitacora.cluster.ClusterMembershipImpl;
import com.grayscaleconsulting.bitacora.data.DataManager;
import com.grayscaleconsulting.bitacora.data.DataManagerExternal;
import com.grayscaleconsulting.bitacora.data.DataManagerExternalImpl;
import com.grayscaleconsulting.bitacora.data.DataManagerImpl;
import com.grayscaleconsulting.bitacora.kafka.Consumer;
import com.grayscaleconsulting.bitacora.kafka.KafkaConsumerImpl;
import com.grayscaleconsulting.bitacora.kafka.KafkaProducerImpl;
import com.grayscaleconsulting.bitacora.kafka.Producer;
import com.grayscaleconsulting.bitacora.metrics.Metrics;
import com.grayscaleconsulting.bitacora.rpc.AvroSocketRPCHandler;
import com.grayscaleconsulting.bitacora.rpc.HttpRPCHandler;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;

import java.net.InetAddress;

/**
 * Main class to execute the server
 *
 * Created by ivaramme on 7/1/15.
 */
public class App {
    public static void main(String[] args) throws Exception {
        
        //Address of the zookeeper host
        String zookeeperHosts = System.getenv("ZOOKEEPER_HOSTS");
        if(null == zookeeperHosts)
            zookeeperHosts = "localhost:2181";
        
        // Name of this node
        String nodeName = System.getenv("NODENAME");
        if(null == nodeName)
            nodeName = InetAddress.getLocalHost().getHostName();

        // Port to serve http API calls
        int apiPort = 8082;
        if(null != System.getenv("API_PORT")) {
            try {
                apiPort = Integer.valueOf(System.getenv("API_PORT"));
            } catch (NumberFormatException nfe) {}
        }
        
        // Kafka server to publish messages to
        String brokerList = System.getenv("BROKER_LIST");
        if(null == brokerList)
            brokerList = "localhost:9092";
        
        // Where to read and publish messages to
        String topic = System.getenv("KAFKA_TOPIC");
        if(null == topic)
            topic = "test";

        Metrics.initJmxReporter();

        // Instantiate objects to execute service
        ClusterMembership clusterMembership = new ClusterMembershipImpl(zookeeperHosts, nodeName, apiPort);
        clusterMembership.initialize();

        Producer kafkaProducer = new KafkaProducerImpl(brokerList, topic);
        
        DataManagerExternal externalData = new DataManagerExternalImpl(clusterMembership);
        DataManager dataManager = new DataManagerImpl();
        dataManager.setExternal(externalData);
        dataManager.setProducer(kafkaProducer);
        
        Consumer consumer = new KafkaConsumerImpl(topic, nodeName, zookeeperHosts, 1);
        consumer.setDataManager(dataManager);
        consumer.start();
        
        // Start Socket RPC Server
        AvroSocketRPCHandler rpcServer = new AvroSocketRPCHandler(dataManager, apiPort+100);
        
        
        // Start HTTP RPC server
        Server server = new Server();
        server.addHandler(new HttpRPCHandler(dataManager));
        server.setStopAtShutdown(true);
        
        SelectChannelConnector connector0 = new SelectChannelConnector();
        connector0.setPort(apiPort);
        connector0.setHost(nodeName);
        connector0.setMaxIdleTime(30000);
        
        server.addConnector(connector0);
        
        server.start();
        server.join();

        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                consumer.shutdown();
                kafkaProducer.shutdown();
                try {
                    server.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }
}
