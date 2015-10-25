package com.grayscaleconsulting.bitacora;

import com.grayscaleconsulting.bitacora.cluster.ClusterMembership;
import com.grayscaleconsulting.bitacora.cluster.ClusterMembershipImpl;
import com.grayscaleconsulting.bitacora.data.DataManager;
import com.grayscaleconsulting.bitacora.data.DataManagerExternal;
import com.grayscaleconsulting.bitacora.data.DataManagerExternalImpl;
import com.grayscaleconsulting.bitacora.data.DataManagerImpl;
import com.grayscaleconsulting.bitacora.kafka.Consumer;
import com.grayscaleconsulting.bitacora.kafka.KafkaProducerImpl;
import com.grayscaleconsulting.bitacora.kafka.KafkaSimpleConsumerImpl;
import com.grayscaleconsulting.bitacora.kafka.Producer;
import com.grayscaleconsulting.bitacora.metrics.Metrics;
import com.grayscaleconsulting.bitacora.rpc.AvroSocketRPCHandler;
import com.grayscaleconsulting.bitacora.rpc.HttpRPCHandler;
import com.grayscaleconsulting.bitacora.storage.LocalStorage;
import com.grayscaleconsulting.bitacora.storage.LocalStorageRocksDBImpl;
import com.grayscaleconsulting.bitacora.util.UIDGenerator;
import com.grayscaleconsulting.bitacora.util.Utils;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Main class to execute the server
 *
 * Created by ivaramme on 7/1/15.
 */
public class Bitacora {
    public static void main(String[] args) throws Exception {
        Properties properties = Utils.loadProperties();
        
        //Address of the zookeeper host
        String zookeeperHosts = System.getenv("ZOOKEEPER_HOSTS");
        if (null == zookeeperHosts) {
            zookeeperHosts = "127.0.0.1:2181";
        }

        // Name of this node
        String nodeName = InetAddress.getLocalHost().getHostName();
        String nodeAddress = System.getProperty("NODE_ADDRESS", System.getenv("NODE_ADDRESS"));
        if (null == nodeAddress)
            nodeAddress = InetAddress.getLocalHost().getHostAddress();

        // Port to serve http API calls
        int apiPort = 8080;
        if (null != System.getenv("API_PORT")) {
            try {
                apiPort = Integer.valueOf(System.getenv("API_PORT"));
            } catch (NumberFormatException nfe) {
            }
        }

        int avroPort = apiPort + 1;
        if (null != System.getenv("AVRO_PORT")) {
            try {
                avroPort = Integer.valueOf(System.getenv("AVRO_PORT"));
            } catch (NumberFormatException nfe) {
            }
        }

        // Kafka server to publish messages to
        String brokerList = System.getenv("BROKER_LIST");
        if (null == brokerList) {
            brokerList = "127.0.0.1:9092";
        }

        // Where to read and publish messages to
        String topic = System.getenv("KAFKA_TOPIC");
        if (null == topic) {
            topic = properties.getProperty("kafka.default.topic", "bitacora");
        }

        Metrics.initJmxReporter();
        
        // Instantiate objects to execute service
        ClusterMembership clusterMembership = new ClusterMembershipImpl(zookeeperHosts, nodeAddress, apiPort, avroPort);
        clusterMembership.initialize();

        UIDGenerator generator = UIDGenerator.getInstance();
        generator.setNodeName(clusterMembership.registrationName());

        Producer kafkaProducer = new KafkaProducerImpl(brokerList, topic);
        kafkaProducer.start();

        String databaseName = properties.getProperty("local.database.name", "bitacora.db");
        LocalStorage localStorage = new LocalStorageRocksDBImpl(databaseName);
        localStorage.start();
        
        DataManagerExternal externalData = new DataManagerExternalImpl(clusterMembership);
        
        DataManager dataManager = new DataManagerImpl();
        dataManager.setExternal(externalData);
        dataManager.setProducer(kafkaProducer);
        dataManager.setLocalStorage(localStorage);

        // Prepare the list of Kafka servers from the global variable
        List<String> brokers = new ArrayList<>();
        String[] _kafkaServers = brokerList.split(",");
        for(int ind = 0; ind < _kafkaServers.length; ind++) {
            brokers.add(_kafkaServers[ind]);
        }

        Consumer consumer = new KafkaSimpleConsumerImpl(nodeName, brokers, topic, 0, zookeeperHosts);
        consumer.setDataManager(dataManager);
        consumer.start();
        
        // Start Socket RPC Server
        AvroSocketRPCHandler rpcServer = new AvroSocketRPCHandler(dataManager, avroPort);
        rpcServer.start();

        // Start HTTP RPC server
        Server server = new Server();
        server.addHandler(new HttpRPCHandler(dataManager, consumer));
        server.setStopAtShutdown(true);
        
        SelectChannelConnector connector0 = new SelectChannelConnector();
        connector0.setPort(apiPort);
        connector0.setHost(nodeName);
        connector0.setMaxIdleTime(30000);
        
        server.addConnector(connector0);
        
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                consumer.shutdown();
                kafkaProducer.shutdown();
                localStorage.shutdown();
                rpcServer.shutdown();

                try {
                    clusterMembership.shutdown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                try {
                    server.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        server.start();
        server.join();

    }
}
