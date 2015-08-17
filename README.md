Bitacora - A Kafka Distributed Memory Database 

[ ![Codeship Status for ivaramme/Bitacora](https://codeship.com/projects/af715930-26b3-0133-0bf7-42218616331f/status?branch=master)](https://codeship.com/projects/97183)

# Description:

This project is an implementation of a distributed, eventually consistent and replicated in-memory key-value datastore that relies on Kafka and accessible from a Web API or a Socket RPC clients, purely for research purposes (meaning there's a lot more to be done)

The main reason of using Kafka is to piggy back on it's great architecture that works as a distributed, highly scalable, serializable (in order) database log.

For a better performance, your Kafka cluster must have Key-based compaction enabled to make sure all keys (that are needed) exist but at the same time the log doesn't grow up infinitely. More information [here](https://cwiki.apache.org/confluence/display/KAFKA/Log+Compaction)

# Architecture:

![alt tag] (docs/diagram.png)

#### Starting up:
- Nodes connect to a set of Kafka nodes and listen to topic, each node connects as a different consumer group making sure that each node receives all updates needed for that topic.
- When registering, get list of all available nodes in cluster and then connect to Kafka

#### When inserting data:
- Request is made to a node via the web API which in turn pushes to the kafka topic
- Data is added to Kafka which in turn will notify all consumers about the new data.

#### When new data is received from Kafka:
- Single Threaded - Data will be inserted in a memory structure, a hashmap for now but in the future will change(hash map? tree? trie?)

#### When a request for data is made:
- Look at local data for that key, if missing: send HTTP request to other members in cluster
- wait for response, if kafka value is received for that key, it will take precedence as kafka is the source of truth

#### When receiving a request from another node for data:
- Check at local cache, if not found return not found
- If found, return the value for the key and its metadata.

# Running:

Assuming you have a Zookeper and Kafka Cluster, try `mvn clean package` and execute the jar file in different servers by using: `java -jar target/Streaming-1.0-SNAPSHOT-jar-with-dependencies.jar`

# System Environment Variables (not java variables):

You can specify the hostname for your zookeeper cluster as well for your Kafka servers and other settings by using `export <VARIABLE_NAME>=<VALUE>`

`ZOOKEEPER_HOST` Zookeeper hosts in the format <host1>:<port1>,<host2>:<port2>". Defaults to localhost:2181 if empty.

`BROKER_LIST` At least one Kafka Server to serve as contact to publish events Defaults to localhost:9092 if empty.

`API_PORT` is used to specify the port to listen for HTTP calls to interact with the service.

`NODENAME` is used in case you want to specify the name of the node or IP used by other nodes in the cluster to request daa

# Web API (very limited right now):

- Return the value for a given key: `curl  "http://localhost:8082/rpc?key=one"`

- Set the value for a given key: `curl -X POST  "http://localhost:8082/rpc?key=one&value=abc"`

- Delete a value: `curl -X DELETE  "http://localhost:8082/rpc?key=one"`

- Inter-Cluster call to get value from another node: `curl -I "http://localhost:8082/rpc/cluster?key=one"`

# AVRO RPC

A binary, socket based RPC handler exists that uses AVRO for communication and schema definition but it is truly a work in progress. Eventually more AVRO should be included in more parts of the project to take advantage of its benefits.
