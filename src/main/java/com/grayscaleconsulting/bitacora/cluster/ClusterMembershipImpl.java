package com.grayscaleconsulting.bitacora.cluster;

import com.google.common.base.Preconditions;
import com.grayscaleconsulting.bitacora.cluster.nodes.Node;
import com.grayscaleconsulting.bitacora.util.Utils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation of a ClusterMembership interface using Zookeeper
 *
 * Created by ivaramme on 7/1/15.
 */
public class ClusterMembershipImpl implements ClusterMembership {
    private static Logger logger = LoggerFactory.getLogger(ClusterMembershipImpl.class);
    
    private ZooKeeper zooKeeper;
    private String zkHosts;
    private boolean initialized = false;
    private boolean registered = false;
    private String registrationName;
    private String nodeName;
    private int apiPort;
    private String socketEndpoint;
    private List<Node> nodesAvailable = new CopyOnWriteArrayList<>();

    public ClusterMembershipImpl(String zkHosts, String nodeName, int apiPort) {
        this(zkHosts, nodeName, apiPort, -1);
    }
    
    /**
     * Creates a new instance of a ClusterMembership class
     * * 
     * @param zkHosts String connection to access ZK servers
     * @param nodeName Name to make accessible this node from other nodes.
     * @param apiPort Port to listen to for HTTP access.
     * @param socketPort Port to listen to for Socket Based access. If not used, passed -1
     */
    public ClusterMembershipImpl(String zkHosts, String nodeName, int apiPort, int socketPort) {
        this.zkHosts = zkHosts;
        this.registrationName = nodeName + ":" + apiPort;
        this.nodeName = nodeName;
        this.apiPort = apiPort;
        if(socketPort > 0)
            this.socketEndpoint = nodeName + ":" + socketPort;
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
                                    createParentZNode(Utils.BASE_NODE_NAME, new byte[0]);
                                    createParentZNode(Utils.AVAILABLE_SERVERS, new byte[0]);
                                    createParentZNode(Utils.SOCKET_AVAILABLE_SERVERS, new byte[0]);
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
            zooKeeper.create(Utils.AVAILABLE_SERVERS + "/" +registrationName, registrationName.getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
            registered = true;
            logger.info("Registered as " + Utils.AVAILABLE_SERVERS +"/"+registrationName);
            
            if(null != socketEndpoint) {
                zooKeeper.create(Utils.SOCKET_AVAILABLE_SERVERS +"/"+socketEndpoint, socketEndpoint.getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
            }
            
            watchForNodes();
        } catch (KeeperException ke) {
            registered = false;
            logger.error("Error registering node: " + ke);
                ke.printStackTrace();
            if(ke instanceof KeeperException.SessionExpiredException) { // NodeExistsException
                // throw new Exception("Unable to register node, something happened: " + ke.getMessage());
                Thread.sleep(1000);
                initialize();
            }

        } catch (InterruptedException ie) {
            logger.error("Interrupted exception while registering node: " + ie);
        }
        
        return registered;
    }

    @Override
    public void watchForNodes() {
        internalGetChildren(Utils.AVAILABLE_SERVERS);
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
                                    .map(nodename -> new Node(nodename.substring(0, nodename.indexOf(':')), nodename))
                                    .collect(Collectors.toList());

                            // Keep stats for existing nodes
                            // TODO: think of moving this to the registry module instead of this.
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