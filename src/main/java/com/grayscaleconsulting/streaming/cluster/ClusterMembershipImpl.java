package com.grayscaleconsulting.streaming.cluster;

import com.google.common.base.Preconditions;
import com.grayscaleconsulting.streaming.cluster.nodes.Node;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Created by ivaramme on 7/1/15.
 */
public class ClusterMembershipImpl implements ClusterMembership {
    public static final String NODE_AVAILABLE_SERVERS = "memdb-nodes";
    private static Logger logger = LoggerFactory.getLogger(ClusterMembershipImpl.class);
    
    private ZooKeeper zooKeeper;
    private String zkHosts;
    private boolean initialized = false;
    private boolean registered = false;
    private String registrationName;
    private String nodeName;
    private int apiPort;
    private List<Node> nodesAvailable = new ArrayList<>();

    public ClusterMembershipImpl(String zkHosts, String nodeName, int apiPort) {
        this.zkHosts = zkHosts;
        this.registrationName = nodeName + ":" + apiPort;
        this.nodeName = nodeName;
        this.apiPort = apiPort;
    }

    public void initialize() {
        try {
            zooKeeper = new ZooKeeper(zkHosts, 2500, new Watcher() {
                        @Override
                        public void process(WatchedEvent event) { 
                            // Takes care of the global events for this session
                            logger.info("Zookeeper event: " +event);
                            if(event.getState().equals(Event.KeeperState.Disconnected)) {
                                registered = false;
                            } else if(event.getState().equals(Event.KeeperState.Expired)) {
                                initialize();
                            } else if(event.getState().equals(Event.KeeperState.SyncConnected)) {
                                try {
                                    createParentZNode("/"+NODE_AVAILABLE_SERVERS, new byte[0]);
                                    registerNode();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    });
            initialized = true;
        } catch (IOException e) {
            logger.error("Unable to connect to Zookeeper. Signing off: " + e);
            e.printStackTrace();
            System.exit(0);
        }
    }
    
    @Override
    public boolean registerNode() throws Exception {
        Preconditions.checkState(initialized);
        Preconditions.checkState(!registered);

        // Sync request for creation as you want to make sure it works
        try {
            zooKeeper.create("/"+NODE_AVAILABLE_SERVERS+"/"+registrationName, registrationName.getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
            registered = true;
            logger.info("Registered as /"+NODE_AVAILABLE_SERVERS+"/"+registrationName);
            
            watchForNodes();
        } catch (KeeperException|InterruptedException ke) {
            registered = false;
            logger.error("Error registering node: " + ke);
            if(!(ke instanceof KeeperException.NodeExistsException)) { // NodeExistsException
                ke.printStackTrace();
                // throw new Exception("Unable to register node, something happened: " + ke.getMessage());
                Thread.sleep(1000);
                registerNode();
            }
        }
        
        return registered;
    }

    @Override
    public void watchForNodes() {
        internalGetChildren(NODE_AVAILABLE_SERVERS);
    }

    @Override
    public List<Node> getAvailableNodes() throws IllegalStateException{
        checkState(registered, "Cluster is not registered, can't return list");
        return nodesAvailable;
    }

    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public boolean isRegistered() { 
        return registered; 
    }

    /**
     * Generic wrapper when watching at the node's children.
     *
     * <p>By default this wrapper will throw an event.</p>
     * @param zNode
     */
    private void internalGetChildren(String zNode) {
        String slash = zNode.indexOf('/') == 0 ? "" : "/";
        zooKeeper.getChildren(slash+zNode, new Watcher() {
                String role_ = zNode;
                @Override
                public void process(WatchedEvent event) {
                    if (Event.EventType.NodeChildrenChanged.equals(event.getType())) {
                        internalGetChildren(role_); // explicitly re-watch when the children change
                    }
                }
            }, new AsyncCallback.ChildrenCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, List<String> children) {
                    switch (KeeperException.Code.get(rc)) {
                        case CONNECTIONLOSS:
                            internalGetChildren(ctx.toString()); // retry
                            break;
                        case OK:
                            logger.info("Got children info for " + ctx.toString());
                            
                            // Update list of nodes, skip itself
                            List<Node> nodesFound = children.stream()
                                    .filter(nodename -> !nodename.equals(registrationName))
                                    .map(nodename -> new Node(nodename.substring(0, nodename.indexOf(':')), nodename) )
                                    .collect(Collectors.toList());

                            // Keep stats for existing nodes
                            nodesFound.forEach(node -> {
                                for(Node n : nodesAvailable) {
                                    if(n.getName().equalsIgnoreCase(node.getName())) {
                                        node.setStats(n.getStats());
                                    }
                                }
                            });
                            
                            logger.info("Updating cluster members list to: " + nodesFound);
                            
                            // Update list
                            nodesAvailable.clear();
                            nodesAvailable.addAll(nodesFound);
                            
                            break;
                    }
                }
            }, zNode );
    }

    /**
     * Provides async node creation
     */
    private void createParentZNode(String path, byte[] data) {
        checkNotNull(zooKeeper, "Zookeeper has not been instantiated");

        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                parentNodeCallback, data);
    }

    AsyncCallback.StringCallback parentNodeCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParentZNode(path, (byte[]) ctx); // retry in cse of connection error
                    break;
                case NODEEXISTS:
                    logger.info("Parents already created");
                    break;
                case OK:
                    logger.info("Parents created");
                    break;
                default:
                    logger.error("Something went wrong", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    public void deleteData(String zNode) {
        getNodeData(zNode, this::deleteData, this::internalDelete);
    }

    private void internalDelete(String zNode, int version) {
        String slash = zNode.indexOf('/') == 0 ? "" : "/";
        zooKeeper.delete(slash + zNode, version, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        deleteData(path);
                        break;
                    case OK:
                        logger.info("Node: " + path + " deleted");
                        break;
                    case NONODE:
                        break;
                    default:
                        logger.error("Error deleting node: " + KeeperException.Code.get(rc));
                        break;
                }
            }
        }, zNode);
    }

    /**
     * This is a generic wrapper function to perform operations in a node (set data, delete, etc)
     *
     * <p>A version number is needed for these operations which requires a getData request.
     * This wrapper provides that and by using callback allowing passing the callbacks needed
     * by this pattern</p>
     * @param zNode node to modify
     * @param self invoking function. Needed in case of connection loss
     * @param internalCall function that we want to apply to the znode after the version is available
     */
    private void getNodeData(String zNode, Consumer<String> self, ObjIntConsumer<String> internalCall) {
        zooKeeper.getData(zNode, false, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        self.accept((String) ctx);
                        break;
                    case OK:
                        internalCall.accept((String) ctx, stat.getVersion());
                        break;
                }
            }
        }, zNode);
    }
    
    public void shutdown() throws InterruptedException {
        checkNotNull(zooKeeper, "Zookeeper has not been instantiated");

        initialized = false;
        zooKeeper.close();
    }
}