## Project Description
This is an application/plugin to track the offsets for all the consumer groups in a Kafka cluster. This app will collect offset information for both a regular Kafka consumer and also Storm-Kafka Spout. 

The app provides a rest endpoint that can be called on-demand to get this information. It can also be used as an input plugin for Collectd.   

## Installation (install commands)

Pre-requisites:

1. Java 8
2. Maven 3+ 

Run:
```
git clone https://github.com/srotya/kafka-monitoring-tool.git
mvn clean package
``` 

## Running project
```java -jar target/kafka-lag-monitor*.jar server```

By default the application assumes the zookeeper is running localhost:2181 and kafka on localhost:9092 

## Configuration 

```yaml
zookeeperUrls: xxx.xxx.com
refreshSeconds: 60
jaasConf: /root/jaas.conf
kerberos: true
kafkaBroker: xxxx.xxx.com
kafkaPort: 6667
commonZkRoot: /

server:
  applicationConnectors:
    - type: http
      port: 9090
      maxRequestHeaderSize: 32KiB
      maxResponseHeaderSize: 32KiB
```

Once the server is up, run the following command from localhost to get the information in a json format.
curl -X GET http://localhost:9090/kafka/offset

### As a Collectd Plugin

This tool can also run as a Collectd input plugin using the same Jar as the standalone dropwizard application. 

Here's sample Collectd Plugin configuration (please edit the location of the jar file for lag monitor)

```
LoadPlugin java
<Plugin "java">
  JVMARG "-Djava.class.path=/opt/collectd/share/collectd/java/collectd-api.jar:/home/centos/kafka-lag-monitor-0.0.1-SNAPSHOT.jar"
   
  LoadPlugin "com.srotya.monitoring.kafka.collectd.KafkaLagMonitor"
  <Plugin "lag">
	jaas "/home/centos/jaas.conf"
	kerberos true
	kafkahost "xxx.xxx.com"
	kafkaport 6667
	zkurl "xxx.xxx.com"
	zkroot "/"
	alias "mycluster"
  </Plugin>
</Plugin>

LoadPlugin csv
<Plugin "csv">
	DataDir "/tmp/metrics"
</Plugin>
```

## Kerberos Configuration

To use the plugin / monitoring tool with Kerberos set kerberos=true in the configuration. Along with that you will also need to provide a JAAS configuration file to provide credentials for the tool to authenticate against Kafka or Zookeeper.


```
KrbLogin{
 com.sun.security.auth.module.Krb5LoginModule required 
 storeKey=true
 useKeyTab=true
 renewTGT=false
 doNotPrompt=true
 principal="kafka/xxx.xxx.com"
 keyTab="/etc/security/keytabs/kafka.service.keytab"
 useTicketCache=false;
};

Client{
 com.sun.security.auth.module.Krb5LoginModule required 
 storeKey=true
 useKeyTab=true
 renewTGT=false
 doNotPrompt=true
 principal="kafka/xxx.xxx.com"
 keyTab="/etc/security/keytabs/kafka.service.keytab"
 serviceName="kafka"
 useTicketCache=false;
};

KafkaClient{
 com.sun.security.auth.module.Krb5LoginModule required
 storeKey=true
 useKeyTab=true
 renewTGT=false
 doNotPrompt=true
 principal="kafka/xxx.xxx.com"
 keyTab="/etc/security/keytabs/kafka.service.keytab"
 serviceName="kafka"
 useTicketCache=false;
};
```

### API parameters
There are few Query Params you can pass to the API to get specific results. Examples are:

If you would like the output in a HTML format instead of Json format, try this:
http://localhost:8080/kafka/offset?outputType=html

If you would like to get only the offsets from a regular consumers, try with the consumerType=regular parameter, example:
http://localhost:8080/kafka/offset?consumerType=regular

If you would only like the storm spout consumers, try this:
http://localhost:8080/kafka/offset?consumerType=spout