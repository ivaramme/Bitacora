Bitacora - A Kafka Distributed Memory Database 

[ ![Codeship Status for ivaramme/Bitacora](https://codeship.com/projects/af715930-26b3-0133-0bf7-42218616331f/status?branch=master)](https://codeship.com/projects/97183)

# Description:

This project is an implementation of a distributed, eventually consistent and replicated in-memory key-value datastore that relies on Kafka and accessible from a Web API or a Socket RPC clients, purely for research purposes (meaning there's a lot more to be done)

The main reason of using Kafka is to piggy back on it's great architecture that works as a distributed, highly scalable, serializable (in order) database log.

For a better performance, your Kafka cluster must have Key-based compaction enabled to make sure all keys (that are needed) exist but at the same time the log doesn't grow up infinitely. More information [here](https://cwiki.apache.org/confluence/display/KAFKA/Log+Compaction)

# Architecture:

Bitacora is an eventually consistent, multi-master, key value database that uses Kafka as its WAL (Write Ahead Log). Kafka is the source of truth of the data but local nodes use local storage as well.

On top of that Bitacora allows communication between peers to fetch data that is not currently present in a node if it is behind in the log consumption for whatever reason. The delayed node will return the value to the client with the highest write timestamp from the responses received from the peers. A concept of cluster membership is applied to accomplish this.

**Overall Diagram:**

![alt tag] (docs/diagram.png)

### Nodes
Bitacora's nodes expose different interfaces to access the data that is kept in memory for fast access - perhaps in a TOO simple structure. The main flavors to access this data is via an HTTP request or using an AVRO client (provided as well).

All data that is also persisted in disk for cases where a restart to the node is needed or there's an incident with the server to avoid going through the whole log to recreate the data (which could be expensive if there's a lot of data/operations)   
 
![alt tag] (docs/node-description.png)

### Considerations:
#### Starting up:
- Nodes connect to a set of Kafka nodes and listen to topic, each node connects as a different consumer group making sure that each node receives all updates needed for that topic.
- When registering, get list of all available nodes in cluster and then connect to Kafka
- Node will read the log from the last offset assigned to its consumer group. Any previous data (if any) will be fetch from the local storage to memory on demand.

#### When inserting data:
- Request is made to a node via the web API which in turn pushes to the kafka topic
- Data is added to Kafka which in turn will notify all consumers about the new data.

#### When new data is received from Kafka:
- Single Threaded - Data will be inserted in a memory structure, a hashmap for now but in the future will change(hash map? tree? trie?)

#### When deleting data:
- Initially the design was to set the item as expired in the log and from there the nodes will treat that key/value as deleted.
- Decided to move to a 'tombstone' approach that would set a null value in the log to a key being deleted since that would trigger an eventual removal of the key from the log therefore reducing its length.
- After some consideration, the current approach is to move back to the first case. The reason behind it is that since Bitacora supports local storing of data, there could be cases where one node might go down for some time and key was deleted from log, but never removed from local storage and there would be no way to know what got removed.

#### When a request for data is made:
- Look at local data for that key, if missing: send HTTP request to other members in cluster
- wait for response, if kafka value is received for that key, it will take precedence as kafka is the source of truth

#### When receiving a request from another node for data:
- Check at local cache, if not found return not found
- If found, return the value for the key and its metadata.

#### Anti-Patterns:
- Set-then-Get is considered an **anti-pattern**. When a set instruction is received, it is sent immediately to the log to make sure it gets distributed to all listening nodes, including the one that received the 'set' instruction. This means that data won't be immediately available. At the same time, when you set you already have the data so there's no need to perform such operation.

# Executing:

Assuming you have a Zookeper and Kafka Cluster, try `mvn clean package` and execute the jar file in different servers by using: `java -jar target/bitacora-1.0-SNAPSHOT-jar-with-dependencies.jar`. Also there's an option to run it as a docker container, see below.

# System Environment Variables (not java variables):

You can specify the hostname for your zookeeper cluster as well for your Kafka servers and other settings by using `export <VARIABLE_NAME>=<VALUE>`

`ZOOKEEPER_HOSTS` Zookeeper hosts in the format <host1>:<port1>,<host2>:<port2>". Defaults to 127.0.0.1:2181 if empty.

`BROKER_LIST` At least one Kafka Server to serve as contact to publish events Defaults to 127.0.0.1:9092 if empty.

`API_PORT` is used to specify the port to listen for HTTP calls to interact with the service.

`NODE_ADDRESS` is used in case you want to specify the public name or IP used by other nodes in the cluster to request data

# Java properties:

`external.request.timeout` Sets the connection timeout for any HTTP External requests between nodes.. Defaults to 60 ms.

# Local Development:

A Vagrant machine has been included that creates an instance of a Zookeeper server and a Kafka Server and that exposes ports 2181 and 9092 automatically.

To execute it just `vagrant up` and then run *Bitacora* without any parameters as it will default to local servers.

# Docker Support:

Support for a Docker image to execute the app is provided.

### Instructions 
After you run `mvn clean package`, build a new Docker image by executing `docker build -t bitacora`. 

To run a docker container with the app embedded execute: `docker run -e "ZOOKEEPER_HOSTS=<YOUR_ZOOKEEPER_HOSTS>" -e "BROKER_LIST=<YOUR_KAFKA_SERVERS>" -d bitacora`. 

The docker base image is included in the repo, located inside: `src/main/docker/Dockerfile`. You can access both the base image and the service image from the Docker public registry: `https://hub.docker.com/r/ivaramme/bitacora-base/` and `https://hub.docker.com/r/ivaramme/bitacora/`.
 
More information about Docker [here|https://www.docker.com/toolbox].

### Docker AWS

A Docker image that runs in AWS using ECS has been created as well under the name `ivaramme/bitacora-aws` in the public registry. The main difference is that the container needs to know the IP address of its host in order to support JMX and receive connections from other containers.

# Web API (very limited right now):

- Return the value for a given key: `curl  "http://localhost:8082/rpc?key=one"`

- Set the value for a given key: `curl -X POST  "http://localhost:8082/rpc?key=one&value=abc"`

- Delete a value: `curl -X DELETE  "http://localhost:8082/rpc?key=one"`

- Inter-Cluster call to get value from another node: `curl -I "http://localhost:8082/rpc/cluster?key=one"`

# AVRO RPC

A binary, socket based RPC handler exists that uses AVRO for communication and schema definition.

The client works as follows:

- Connects to Zookeeper (same ZK cluster that the nodes connect to) and gets the list of available servers.

- Chooses one randomly and connects to it. In case of disconnection retries or throws an exception. There should be some load balancing in place.

- Invokes RPC methods directly, throws a custom exception in case of errors.

- Handles only operations for read, set and delete.

# High Availability:

Since the project implements two flavors for communication (Binary RPC and HTTP API) the options are:

- HTTP API: put all nodes behind a load balancer and let it do its magic. HTTP API exposes a basic endpoint for health status under /health

- RPC: the client uses zookeeper to maintain updates of the nodes that are available. More logic needs to be added in terms of load balancing as it currently chooses a random server.
