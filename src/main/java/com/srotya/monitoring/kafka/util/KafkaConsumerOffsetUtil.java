/**
 * Copyright 2016 Symantec Corporation.
 * 
 * Licensed under the Apache License, Version 2.0 (the “License”); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.monitoring.kafka.util;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.srotya.monitoring.kafka.KafkaMonitorConfiguration;
import com.srotya.monitoring.kafka.core.kafka.KafkaConsumerGroupMetadata;
import com.srotya.monitoring.kafka.core.kafka.KafkaOffsetMonitor;
import com.srotya.monitoring.kafka.core.kafka.KafkaSpoutMetadata;
import com.srotya.monitoring.kafka.core.managed.ZKClient;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import org.apache.kafka.common.TopicPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;

/**
 * Edited by
 * 
 * @author ambud
 */
public class KafkaConsumerOffsetUtil {

	public static final String OFFSET = "offset";
	public static final String TOPIC = "topic";
	public static final String CONSUMER = "consumer";
	public static final String PRODUCER = "producer";
	public static final String MEASUREMENT_NAME = "lag";
	public static final String DB_NAME = "kafka";

	private static final Logger log = Logger.getLogger(KafkaConsumerOffsetUtil.class.getName());
	private static final Map<String, KafkaConsumer> consumerMap = new HashMap<String, KafkaConsumer>();
	private static final String clientName = "GetOffsetClient";
	private KafkaMonitorConfiguration kafkaConfiguration;
	private static KafkaConsumerOffsetUtil kafkaConsumerOffsetUtil = null;
	private ZKClient zkClient;
	private Map<String, Map<String, KafkaOffsetMonitor>> consumerOffsetMap = new ConcurrentHashMap<>();
	private Queue<String> brokerHosts;
	private Set<String> topics;
	private boolean enableHistory;
	private StorageEngine server;

	public static KafkaConsumerOffsetUtil getInstance(KafkaMonitorConfiguration kafkaConfiguration, ZKClient zkClient,
			boolean enableHistory, StorageEngine server) {
		if (kafkaConsumerOffsetUtil == null) {
			kafkaConsumerOffsetUtil = new KafkaConsumerOffsetUtil(kafkaConfiguration, zkClient, enableHistory, server);
			kafkaConsumerOffsetUtil.setupMonitoring();
		}
		System.out.println("############# Kafka Monitor Configuration ############# ");
		System.out.println(kafkaConfiguration);
		return kafkaConsumerOffsetUtil;
	}

	private KafkaConsumerOffsetUtil(KafkaMonitorConfiguration kafkaConfiguration, ZKClient zkClient,
			boolean enableHistory, StorageEngine server) {
		this.kafkaConfiguration = kafkaConfiguration;
		this.zkClient = zkClient;
		this.enableHistory = enableHistory;
		this.server = server;
		this.topics = new ConcurrentSkipListSet<>();
		brokerHosts = new ArrayBlockingQueue<>(kafkaConfiguration.getKafkaBroker().length);
		for (String broker : kafkaConfiguration.getKafkaBroker()) {
			brokerHosts.add(broker);
		}
		Thread th = new Thread(new KafkaNewConsumerOffsetThread(this));
		th.setDaemon(true);
		th.start();
	}

	public void setupMonitoring() {
		ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
		executorService.scheduleAtFixedRate(new KafkaConsumerOffsetThread(), 2, kafkaConfiguration.getRefreshSeconds(),
				TimeUnit.SECONDS);
		log.info("Monitoring thread for zookeeper offsets online");
	}

	/**
	 * ./kafka-console-consumer.sh --consumer.config /tmp/consumer.config
	 * --formatter
	 * "kafka.coordinator.GroupMetadataManager\$OffsetsMessageFormatter"
	 * --zookeeper localhost:2181 --topic __consumer_offsets --from-beginning
	 * 
	 * @author ambud
	 */
	private class KafkaNewConsumerOffsetThread implements Runnable {

		private KafkaConsumerOffsetUtil util;

		public KafkaNewConsumerOffsetThread(KafkaConsumerOffsetUtil util) {
			this.util = util;
		}

		@Override
		public void run() {
			try {
				Properties props = new Properties();
				props.put("bootstrap.servers", brokerHosts.peek() + ":" + kafkaConfiguration.getKafkaPort());
				props.put("group.id", kafkaConfiguration.getConsumerGroupName());
				props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
				props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
				props.put("enable.auto.commit", "false");
				props.put("auto.offset.reset", "latest");
				props.put("security.protocol", System.getProperty("security.protocol"));
				@SuppressWarnings("resource")
				KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
				consumer.subscribe(Arrays.asList("__consumer_offsets"));
				log.info("Connected to Kafka consumer offset topic");
				Schema schema = new Schema(new Field("group", Schema.STRING), new Field(TOPIC, Schema.STRING),
						new Field("partition", Schema.INT32));
				while (true) {
					ConsumerRecords<byte[], byte[]> records = consumer.poll(10000);
					for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
						if (consumerRecord.value() != null && consumerRecord.key() != null) {
							ByteBuffer key = ByteBuffer.wrap(consumerRecord.key());
							short version = key.getShort();
							if (version < 2) {
								try {
									Struct struct = (Struct) schema.read(key);
									if (struct.getString("group")
											.equalsIgnoreCase(kafkaConfiguration.getConsumerGroupName())) {
										continue;
									}
									String group = struct.getString("group");
									String topic = struct.getString(TOPIC);
									int partition = struct.getInt("partition");
									System.out.println("# group - " + group);
									System.out.println("topic - " + topic);
									System.out.println("partition - " + partition);
									/*
									KafkaConsumer con = util.getConsumer(util.brokerHosts.peek(),
											util.kafkaConfiguration.getKafkaPort(), clientName);
									long realOffset = util.getLastOffset(con, struct.getString(TOPIC), partition, -1,
											clientName);
									long consumerOffset = readOffsetMessageValue(
											ByteBuffer.wrap(consumerRecord.value()));
									long lag = realOffset - consumerOffset;
									KafkaOffsetMonitor mon = new KafkaOffsetMonitor(group, topic, partition, realOffset,
											consumerOffset, lag);
									topics.add(topic);
									Map<String, KafkaOffsetMonitor> map = consumerOffsetMap.get(topic);
									if (map == null) {
										map = new ConcurrentHashMap<>();
										consumerOffsetMap.put(topic, map);
									}
									map.put(group + "%" + partition, mon);
									if (enableHistory) {
										server.writeDataPoint(new DataPoint(DB_NAME, MEASUREMENT_NAME, MEASUREMENT_NAME,
												Arrays.asList(group, topic, String.valueOf(partition)),
												System.currentTimeMillis(), lag));
										server.writeDataPoint(new DataPoint(DB_NAME, MEASUREMENT_NAME, PRODUCER,
												Arrays.asList(group, topic, String.valueOf(partition)),
												System.currentTimeMillis(), realOffset));
										server.writeDataPoint(new DataPoint(DB_NAME, MEASUREMENT_NAME, CONSUMER,
												Arrays.asList(group, topic, String.valueOf(partition)),
												System.currentTimeMillis(), consumerOffset));
									}*/
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				util.brokerHosts.add(util.brokerHosts.remove());
			}

		}

	}

	private long readOffsetMessageValue(ByteBuffer buffer) {
		buffer.getShort(); // read and ignore version
		long offset = buffer.getLong();
		return offset;
	}

	private class KafkaConsumerOffsetThread implements Runnable {
		@Override
		public void run() {
			try {
				Subject subject = null;
				if (kafkaConfiguration.isKerberos()) {
					LoginContext lc = new LoginContext("Client");
					lc.login();
					subject = lc.getSubject();
					System.out.println("## KafakaConsumerOffsetTread login, subject - " + subject);
				} else {
					Subject.getSubject(AccessController.getContext());
				}
				Subject.doAs(subject, new PrivilegedAction<Void>() {

					@Override
					public Void run() {
						try {
							ArrayList<KafkaOffsetMonitor> kafkaOffsetMonitors = new ArrayList<KafkaOffsetMonitor>();
							kafkaOffsetMonitors.addAll(getTopicOffsets());
							kafkaOffsetMonitors.addAll(getSpoutKafkaOffsetMonitors());
							kafkaOffsetMonitors.addAll(getRegularKafkaOffsetMonitors());
							for (KafkaOffsetMonitor mon : kafkaOffsetMonitors) {
								Map<String, KafkaOffsetMonitor> map = consumerOffsetMap.get(mon.getTopic());
								if (map == null) {
									map = new ConcurrentHashMap<>();
									consumerOffsetMap.put(mon.getTopic(), map);
								}
								map.put(mon.getConsumerGroupName() + "%" + mon.getPartition(), mon);
							}
							log.info("Updating new lag information");
						} catch (Exception e) {
							log.error("Error while collecting kafka consumer offset metrics:"
									+ kafkaConfiguration.getKafkaBroker() + ":" + kafkaConfiguration.getKafkaPort(), e);
						}
						return null;
					}
				});
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public List<KafkaOffsetMonitor> getTopicOffsets() throws Exception {
		List<KafkaOffsetMonitor> monitors = new ArrayList<>();
		try {
			String host = brokerHosts.peek();
			int port = kafkaConfiguration.getKafkaPort();
			KafkaConsumer consumer = getConsumer(host, port, clientName);
			for (String topic : zkClient.getTopics()) {
				System.out.println("## getTopicOffsets, topic - " + topic);
				topics.add(topic);
				List<TopicPartition> partitions = getPartitions(consumer, topic);
				partitions.stream().forEach(p -> {
					System.out.println("TopicPartition - " + p.topic() + ", " + p.partition());
				});
				/*log.warn("Topic offset fetching:" + topic + "\tpartitions:" + partitions);
				for (TopicPartition partition : partitions) {
					consumer = getConsumer(host, port, clientName);
					long kafkaTopicOffset = getLastOffset(consumer, topic, partition.getPartitionId(), -1, clientName);
					KafkaOffsetMonitor kafkaOffsetMonitor = new KafkaOffsetMonitor("_root", topic,
							partition.getPartitionId(), kafkaTopicOffset, 0, 0);
					if (enableHistory) {
						server.writeDataPoint(new DataPoint(DB_NAME, TOPIC, OFFSET,
								Arrays.asList(topic, String.valueOf(partition.getPartitionId())),
								System.currentTimeMillis(), kafkaTopicOffset));
					}
					monitors.add(kafkaOffsetMonitor);
				}*/
			}
		} catch (ClosedChannelException e) {
			e.printStackTrace();
			brokerHosts.add(brokerHosts.remove());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		return monitors;
	}

	public List<KafkaOffsetMonitor> getSpoutKafkaOffsetMonitors() throws Exception {
		List<KafkaOffsetMonitor> kafkaOffsetMonitors = new ArrayList<KafkaOffsetMonitor>();
		List<String> activeSpoutConsumerGroupList = zkClient.getActiveSpoutConsumerGroups();
		System.out.println("ActiveSpoutConsumerGroupList - " + activeSpoutConsumerGroupList);
		List<String> partitions = new ArrayList<String>();

		/*for (String consumerGroup : activeSpoutConsumerGroupList) {
			try {
				partitions = zkClient.getChildren(kafkaConfiguration.getCommonZkRoot() + "/" + consumerGroup);
			} catch (Exception e) {
				log.warn("Error while listing partitions for the consumer group: " + consumerGroup);
				continue;
			}
			try {
				for (String partition : partitions) {
					byte[] byteData = zkClient
							.getData(kafkaConfiguration.getCommonZkRoot() + "/" + consumerGroup + "/" + partition);
					String data = "";
					if (byteData != null) {
						data = new String(byteData);
					}
					if (!data.trim().isEmpty()) {
						KafkaSpoutMetadata kafkaSpoutMetadata = new ObjectMapper().readValue(data,
								KafkaSpoutMetadata.class);
						KafkaConsumer consumer = getConsumer(kafkaSpoutMetadata.getBroker().getHost(),
								kafkaSpoutMetadata.getBroker().getPort(), clientName);
						long realOffset = getLastOffset(consumer, kafkaSpoutMetadata.getTopic(),
								kafkaSpoutMetadata.getPartition(), -1, clientName);
						long lag = realOffset - kafkaSpoutMetadata.getOffset();
						KafkaOffsetMonitor kafkaOffsetMonitor = new KafkaOffsetMonitor(consumerGroup,
								kafkaSpoutMetadata.getTopic(), kafkaSpoutMetadata.getPartition(), realOffset,
								kafkaSpoutMetadata.getOffset(), lag);
						topics.add(kafkaSpoutMetadata.getTopic());
						kafkaOffsetMonitors.add(kafkaOffsetMonitor);
						if (enableHistory) {
							server.writeDataPoint(new DataPoint(DB_NAME,
									MEASUREMENT_NAME, MEASUREMENT_NAME, Arrays.asList(consumerGroup,
											kafkaSpoutMetadata.getTopic(), String.valueOf(partition)),
									System.currentTimeMillis(), lag));
							server.writeDataPoint(new DataPoint(DB_NAME,
									MEASUREMENT_NAME, PRODUCER, Arrays.asList(consumerGroup,
											kafkaSpoutMetadata.getTopic(), String.valueOf(partition)),
									System.currentTimeMillis(), realOffset));
							server.writeDataPoint(new DataPoint(DB_NAME, MEASUREMENT_NAME, CONSUMER,
									Arrays.asList(consumerGroup, kafkaSpoutMetadata.getTopic(),
											String.valueOf(partition)),
									System.currentTimeMillis(), kafkaSpoutMetadata.getOffset()));
						}
					}
				}
			} catch (Exception e) {
				log.warn("Skipping znode:" + consumerGroup + " as it doesn't seem to be a topology consumer group");
			}
		}*/
		return kafkaOffsetMonitors;
	}

	public List<KafkaOffsetMonitor> getRegularKafkaOffsetMonitors() throws Exception {
		List<KafkaConsumerGroupMetadata> kafkaConsumerGroupMetadataList = zkClient
				.getActiveRegularConsumersAndTopics();
		List<KafkaOffsetMonitor> kafkaOffsetMonitors = new ArrayList<KafkaOffsetMonitor>();
		return kafkaOffsetMonitors;

		/*try {


			KafkaConsumer consumer = getConsumer(brokerHosts.peek(), kafkaConfiguration.getKafkaPort(), clientName);
			for (KafkaConsumerGroupMetadata kafkaConsumerGroupMetadata : kafkaConsumerGroupMetadataList) {
				List<TopicPartition> partitions = getPartitions(consumer, kafkaConsumerGroupMetadata.getTopic());
				for (TopicPartition partition : partitions) {
					consumer = getConsumer(consumer, partition.getLeaderPort(), clientName);
					long kafkaTopicOffset = getLastOffset(consumer, kafkaConsumerGroupMetadata.getTopic(),
							partition.getPartitionId(), -1, clientName);
					long consumerOffset = 0;
					if (kafkaConsumerGroupMetadata.getPartitionOffsetMap()
							.get(Integer.toString(partition.getPartitionId())) != null) {
						consumerOffset = kafkaConsumerGroupMetadata.getPartitionOffsetMap()
								.get(Integer.toString(partition.getPartitionId()));
					}
					long lag = kafkaTopicOffset - consumerOffset;
					KafkaOffsetMonitor kafkaOffsetMonitor = new KafkaOffsetMonitor(
							kafkaConsumerGroupMetadata.getConsumerGroup(), kafkaConsumerGroupMetadata.getTopic(),
							partition.getPartitionId(), kafkaTopicOffset, consumerOffset, lag);
					topics.add(kafkaConsumerGroupMetadata.getTopic());
					kafkaOffsetMonitors.add(kafkaOffsetMonitor);
					if (enableHistory) {
						server.writeDataPoint(new DataPoint(DB_NAME, MEASUREMENT_NAME, MEASUREMENT_NAME,
								Arrays.asList(kafkaConsumerGroupMetadata.getConsumerGroup(),
										kafkaConsumerGroupMetadata.getTopic(), String.valueOf(partition)),
								System.currentTimeMillis(), lag));
						server.writeDataPoint(new DataPoint(DB_NAME, MEASUREMENT_NAME, PRODUCER,
								Arrays.asList(kafkaConsumerGroupMetadata.getConsumerGroup(),
										kafkaConsumerGroupMetadata.getTopic(), String.valueOf(partition)),
								System.currentTimeMillis(), kafkaTopicOffset));
						server.writeDataPoint(new DataPoint(DB_NAME, MEASUREMENT_NAME, CONSUMER,
								Arrays.asList(kafkaConsumerGroupMetadata.getConsumerGroup(),
										kafkaConsumerGroupMetadata.getTopic(), String.valueOf(partition)),
								System.currentTimeMillis(), consumerOffset));
					}
				}
			}
			return kafkaOffsetMonitors;
		} catch (ClosedChannelException e) {
			brokerHosts.add(brokerHosts.remove());
			e.printStackTrace();
			return new ArrayList<>();
		} catch (Exception e) {
			throw e;
		}*/
	}

	public List<TopicPartition> getPartitions(KafkaConsumer consumer, String topic)
			throws ClosedChannelException {
		List<PartitionInfo> partitions = consumer.partitionsFor(topic);
		return partitions.stream().map(it -> new TopicPartition(topic,it.partition()))
				.collect(Collectors.toList());
	}

/*	public List<Long, Long> getLastOffset(KafkaConsumer consumer, String topic, int partition, long whichTime, String clientName) {
		//new TopicPartition tp = new TopicPartition(consumer.partitionsFor(topic).g

		List<PartitionInfo> partitions = consumer.partitionsFor(topic);
		List<TopicPartition> topicPartitions = partitions.stream()
				.map(it -> new TopicPartition(topic,it.partition())).collect(Collectors.toList());
		consumer.assign(partitions);
		consumer.seekToEnd(partitions);
		offsets = partitions.map(it -> consumer.position(it))
		return -1L;
	}*/

	/*public long getLastOffset(KafkaConsumer consumer, String topic, int partition, long whichTime, String clientName) {
		long lastOffset = 0;
		try {
			List<String> topics = Collections.singletonList(topic);
			TopicMetadataRequest req = new TopicMetadataRequest(topics);
			kafka.javaapi.TopicMetadataResponse topicMetadataResponse = null;
			try {
				topicMetadataResponse = consumer.send(req);
			} catch (Exception e) {
				e.printStackTrace();
				return -1L;
			}
			TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
			for (TopicMetadata topicMetadata : topicMetadataResponse.topicsMetadata()) {
				for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
					if (partitionMetadata.partitionId() == partition) {
						String partitionHost = partitionMetadata.leader().host();
						consumer = getConsumer(partitionHost, partitionMetadata.leader().port(), clientName);
						break;
					}
				}
			}
			Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
			requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
			kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
					kafka.api.OffsetRequest.CurrentVersion(), clientName);
			OffsetResponse response = consumer.getOffsetsBefore(request);
			if (response.hasError()) {
				log.error(
						"Error fetching Offset Data from the Broker. Reason: " + response.errorCode(topic, partition));
				lastOffset = 0;
			}
			long[] offsets = response.offsets(topic, partition);
			lastOffset = offsets[0];
		} catch (Exception e) {
			log.error("Error while collecting the log Size for topic: " + topic + ", and partition: " + partition, e);
		}
		return lastOffset;
	}*/

	public KafkaConsumer getConsumer(String host, int port, String clientName) {
		KafkaConsumer consumer = consumerMap.get(host);
		if (consumer == null) {

			Properties props = new Properties();
			props.put("bootstrap.servers", host + ":" + port);

			props.put("enable.auto.commit", "false");
			props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put("group.id", kafkaConfiguration.getConsumerGroupName());
			props.put("enable.auto.commit", "false");
			props.put("auto.offset.reset", "latest");
			props.put("security.protocol", System.getProperty("security.protocol"));
			consumer = new KafkaConsumer(props);


			log.info("Created a new Kafka Consumer for host: " + host);
			System.out.println("Created a new Kafka Consumer for host:port - " + host + ":" + port
					+ ", with secProtocal - " + System.getProperty("security.protocol")
					+ ", with clientName - " + clientName
					+ ", with group.id - " + kafkaConfiguration.getConsumerGroupName());
			consumerMap.put(host, consumer);
		}
		return consumer;
	}

	public static void closeConnection() {
		for (KafkaConsumer consumer : consumerMap.values()) {
			//log.info("Closing connection for: " + consumer.host());
			consumer.close();
		}
	}

	public static String htmlOutput(List<KafkaOffsetMonitor> kafkaOffsetMonitors) {
		StringBuilder sb = new StringBuilder();
		sb.append("<html><body><pre>");
		sb.append(String.format("%s \t %s \t %s \t %s \t %s \t %s \n", StringUtils.rightPad("Consumer Group", 40),
				StringUtils.rightPad("Topic", 40), StringUtils.rightPad("Partition", 10),
				StringUtils.rightPad("Log Size", 10), StringUtils.rightPad("Consumer Offset", 15),
				StringUtils.rightPad("Lag", 10)));
		for (KafkaOffsetMonitor kafkaOffsetMonitor : kafkaOffsetMonitors) {
			sb.append(String.format("%s \t %s \t %s \t %s \t %s \t %s \n",
					StringUtils.rightPad(kafkaOffsetMonitor.getConsumerGroupName(), 40),
					StringUtils.rightPad(kafkaOffsetMonitor.getTopic(), 40),
					StringUtils.rightPad("" + kafkaOffsetMonitor.getPartition(), 10),
					StringUtils.rightPad("" + kafkaOffsetMonitor.getLogSize(), 10),
					StringUtils.rightPad("" + kafkaOffsetMonitor.getConsumerOffset(), 15),
					StringUtils.rightPad("" + kafkaOffsetMonitor.getLag(), 10)));
		}
		sb.append("</pre></body></html>");
		return sb.toString();
	}

	// fetch all available brokers from the zookeeper

	public Map<String, Map<String, KafkaOffsetMonitor>> getNewConsumer() {
		return consumerOffsetMap;
	}

	/**
	 * @return the topics
	 */
	public Set<String> getTopics() {
		return topics;
	}

	/**
	 * https://prometheus.io/docs/instrumenting/exposition_formats/
	 * 
	 * @param kafkaOffsetMonitors
	 * 
	 * @return
	 */
	public static String toPrometheusFormat(List<KafkaOffsetMonitor> kafkaOffsetMonitors) {
		StringBuilder builder = new StringBuilder();
		for (KafkaOffsetMonitor kafkaOffsetMonitor : kafkaOffsetMonitors) {
			builder.append(String.format("# metrics for topic-partition:%s-%d and consumer-group:%s\n",
					kafkaOffsetMonitor.getTopic(), kafkaOffsetMonitor.getPartition(),
					kafkaOffsetMonitor.getConsumerGroupName()));
			builder.append(String.format("%s{topic=\"%s\",group=\"%s\",partition=\"%d\"} %d\n", "kafka_lag",
					kafkaOffsetMonitor.getTopic(), kafkaOffsetMonitor.getConsumerGroupName(),
					kafkaOffsetMonitor.getPartition(), kafkaOffsetMonitor.getLag()));
			builder.append(String.format("%s{topic=\"%s\",group=\"%s\",partition=\"%d\"} %d\n", "kafka_consumer_offset",
					kafkaOffsetMonitor.getTopic(), kafkaOffsetMonitor.getConsumerGroupName(),
					kafkaOffsetMonitor.getPartition(), kafkaOffsetMonitor.getConsumerOffset()));
			builder.append(String.format("%s{topic=\"%s\",partition=\"%d\"} %d\n", "kafka_producer_offset",
					kafkaOffsetMonitor.getTopic(), kafkaOffsetMonitor.getPartition(), kafkaOffsetMonitor.getLogSize()));
			builder.append("\n");
		}
		return builder.toString();
	}
}