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
package com.symantec.cpe.analytics.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.symantec.cpe.analytics.KafkaMonitorConfiguration;
import com.symantec.cpe.analytics.core.kafka.KafkaConsumerGroupMetadata;
import com.symantec.cpe.analytics.core.kafka.KafkaOffsetMonitor;
import com.symantec.cpe.analytics.core.kafka.KafkaSpoutMetadata;
import com.symantec.cpe.analytics.core.kafka.TopicPartitionLeader;
import com.symantec.cpe.analytics.core.managed.ZKClient;

import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.api.TopicMetadataResponse;
import kafka.client.ClientUtils;
import kafka.cluster.BrokerEndPoint;
import kafka.producer.ProducerConfig;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.Set;

public class KafkaConsumerOffsetUtil {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerOffsetUtil.class);

	private KafkaMonitorConfiguration kafkaConfiguration;
	private static KafkaConsumerOffsetUtil kafkaConsumerOffsetUtil = null;
	private ZKClient zkClient;
	private AtomicReference<ArrayList<KafkaOffsetMonitor>> references = null;
	private String zkRoot;

	private ProducerConfig config;

	private Seq<BrokerEndPoint> brokerList;

	public static KafkaConsumerOffsetUtil getInstance(final KafkaMonitorConfiguration kafkaConfiguration,
			ZKClient zkClient) {
		if (kafkaConsumerOffsetUtil == null) {
			kafkaConsumerOffsetUtil = new KafkaConsumerOffsetUtil(kafkaConfiguration, zkClient);
		}
		return kafkaConsumerOffsetUtil;
	}

	private KafkaConsumerOffsetUtil(KafkaMonitorConfiguration kafkaConfiguration, ZKClient zkClient) {
		this.kafkaConfiguration = kafkaConfiguration;
		this.zkClient = zkClient;
		this.zkRoot = kafkaConfiguration.getCommonZkRoot();
		this.references = new AtomicReference<>(new ArrayList<KafkaOffsetMonitor>());
		getConsumer(kafkaConfiguration.getKafkaBroker(), kafkaConfiguration.getKafkaPort());
	}
	
	public void setupMonitoring() {
		ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
		executorService.scheduleAtFixedRate(new KafkaConsumerOffsetThread(), 5, kafkaConfiguration.getRefreshSeconds(),
				TimeUnit.SECONDS);
	}

	private class KafkaConsumerOffsetThread implements Runnable {
		@Override
		public void run() {
			try {
				ArrayList<KafkaOffsetMonitor> kafkaOffsetMonitors = new ArrayList<KafkaOffsetMonitor>();
				kafkaOffsetMonitors.addAll(getSpoutKafkaOffsetMonitors());
				kafkaOffsetMonitors.addAll(getRegularKafkaOffsetMonitors());
				Collections.sort(kafkaOffsetMonitors, new KafkaOffsetMonitorComparator());
				references.set(kafkaOffsetMonitors);
				LOG.info("Updating new lag information");
			} catch (Exception e) {
				LOG.error("Error while collecting kafka consumer offset metrics", e);
			}
		}
	}
	
	public void fetchOffsets() {
		Set<String> asScalaSet = JavaConversions.asScalaSet(new java.util.HashSet<>(Arrays.asList("topicA")));
		TopicMetadataResponse fetchTopicMetadata = ClientUtils.fetchTopicMetadata(asScalaSet, brokerList, config, 5000);
		TopicMetadata metadata = fetchTopicMetadata.topicsMetadata().head();
	}

	public List<KafkaOffsetMonitor> getSpoutKafkaOffsetMonitors() throws Exception {
		List<KafkaOffsetMonitor> kafkaOffsetMonitors = new ArrayList<KafkaOffsetMonitor>();
		List<String> activeSpoutConsumerGroupList = zkClient.getActiveSpoutConsumerGroups(zkRoot);
		List<String> partitions = new ArrayList<String>();
		for (String consumerGroup : activeSpoutConsumerGroupList) {
			try {
				partitions = zkClient.getChildren(zkRoot + "/" + consumerGroup);
			} catch (Exception e) {
				LOG.error("Error while listing partitions for the consumer group: " + consumerGroup);
			}
			try {
				for (String partition : partitions) {
					byte[] byteData = zkClient.getData(zkRoot + "/" + consumerGroup + "/" + partition);
					String data = "";
					if (byteData != null) {
						data = new String(byteData);
					}
					if (!data.trim().isEmpty()) {
						KafkaSpoutMetadata kafkaSpoutMetadata = new ObjectMapper().readValue(data,
								KafkaSpoutMetadata.class);
						long realOffset = 0;
//						consumer.position(
//								new TopicPartition(kafkaSpoutMetadata.getTopic(), kafkaSpoutMetadata.getPartition()));
						long lag = realOffset - kafkaSpoutMetadata.getOffset();
						KafkaOffsetMonitor kafkaOffsetMonitor = new KafkaOffsetMonitor(consumerGroup,
								kafkaSpoutMetadata.getTopic(), kafkaSpoutMetadata.getPartition(), realOffset,
								kafkaSpoutMetadata.getOffset(), lag);
						kafkaOffsetMonitors.add(kafkaOffsetMonitor);
					}
				}
			} catch (Exception e) {
				LOG.warn("Skipping znode:" + consumerGroup + " as it doesn't seem to be a topology consumer group");
			}
		}
		return kafkaOffsetMonitors;
	}

	public List<KafkaOffsetMonitor> getRegularKafkaOffsetMonitors() throws Exception {
		List<KafkaConsumerGroupMetadata> kafkaConsumerGroupMetadataList = zkClient.getActiveRegularConsumersAndTopics();
		List<KafkaOffsetMonitor> kafkaOffsetMonitors = new ArrayList<KafkaOffsetMonitor>();
		for (KafkaConsumerGroupMetadata kafkaConsumerGroupMetadata : kafkaConsumerGroupMetadataList) {
			List<TopicPartitionLeader> partitions = getPartitions(null, kafkaConsumerGroupMetadata.getTopic());
			for (TopicPartitionLeader partition : partitions) {
				long kafkaTopicOffset = 0;
//				consumer
//						.position(new TopicPartition(partition.getTopic(), partition.getPartitionId()));
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
				kafkaOffsetMonitors.add(kafkaOffsetMonitor);
			}
		}
		return kafkaOffsetMonitors;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<TopicPartitionLeader> getPartitions(KafkaConsumer consumer, String topic) {
		List<TopicPartitionLeader> partitions = new ArrayList<TopicPartitionLeader>();
		List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);
		for (PartitionInfo partitionInfo : partitionsFor) {
			TopicPartitionLeader topicPartitionLeader = new TopicPartitionLeader(topic, partitionInfo.partition(),
					partitionInfo.leader().host(), partitionInfo.leader().port());
			partitions.add(topicPartitionLeader);
		}
		return partitions;
	}

	public void getConsumer(String host, int port) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", host + ":" + port);
		properties.put("enable.auto.commit", "false");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("session.timeout.ms", "30000");
		properties.put("security.protocol", "SASL_PLAINTEXT");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		
		config = new ProducerConfig(properties);
		brokerList = ClientUtils.parseBrokerList(host);
		
		LOG.info("Created a new Kafka Consumer for host: " + host);
	}

	public void closeConnection() {
	}

	public String htmlOutput(List<KafkaOffsetMonitor> kafkaOffsetMonitors) {
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

	public AtomicReference<ArrayList<KafkaOffsetMonitor>> getReferences() {
		return references;
	}
}
