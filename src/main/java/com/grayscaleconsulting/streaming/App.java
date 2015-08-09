package com.grayscaleconsulting.streaming;

import com.grayscaleconsulting.streaming.cluster.ClusterMembership;
import com.grayscaleconsulting.streaming.cluster.ClusterMembershipImpl;
import com.grayscaleconsulting.streaming.data.DataManager;
import com.grayscaleconsulting.streaming.data.DataManagerExternal;
import com.grayscaleconsulting.streaming.data.DataManagerExternalImpl;
import com.grayscaleconsulting.streaming.data.DataManagerImpl;
import com.grayscaleconsulting.streaming.kafka.KafkaConsumerImpl;
import com.grayscaleconsulting.streaming.kafka.KafkaProducerImpl;
import com.grayscaleconsulting.streaming.kafka.Producer;
import com.grayscaleconsulting.streaming.metrics.Metrics;
import com.grayscaleconsulting.streaming.rpc.HttpRPCHandler;

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
        String zookeeperHosts = System.getenv("ZOOKEEPER_HOST");
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
        
        KafkaConsumerImpl consumer = new KafkaConsumerImpl(topic, nodeName, zookeeperHosts);
        consumer.setDataManager(dataManager);
        consumer.start(1);
        
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
