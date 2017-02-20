## Project Description
This is an application/plugin to track the offsets for all the consumer groups in a Kafka cluster. This app will collect offset information for both a regular Kafka consumer and also Storm-Kafka Spout. 

The app provides a rest endpoint that can be called on-demand to get this information. It can also be used as an input plugin for Collectd.   

## Installation (install commands)
 

## Running project
```java -jar target/kafka-monitoring-tool-0.0.1.jar server```

By default the application assumes the zookeeper is running localhost on port 2181. If you need to provide a zookeeper host, pass it as a jvm parameter like this:

java -Ddw.zookeeperUrls=ZKHOST:ZKPORT,ZKHOST:ZKPORT,ZKHOST:PORT -jar kafka-monitoring-tool-0.0.1.jar server

Once the server is up, run the following command from localhost to get the information in a json format.
curl -X GET http://localhost:8080/kafka/offset

## Configuration (config commands)
The application can also be passed a yml file as a configuration file, instead of passing the zookeeper urls on the command line. Default.yml file is available in the project. The way you start the project with yml file is like this:

java -jar kafka-monitoring-tool-0.0.1.jar server default.yml

### API parameters
There are few Query Params you can pass to the API to get specific results. Examples are:

If you would like the output in a HTML format instead of Json format, try this:
http://localhost:8080/kafka/offset?outputType=html

If you would like to get only the offsets from a regular consumers, try with the consumerType=regular parameter, example:
http://localhost:8080/kafka/offset?consumerType=regular

If you would only like the storm spout consumers, try this:
http://localhost:8080/kafka/offset?consumerType=spout

## As a Collectd Plugin