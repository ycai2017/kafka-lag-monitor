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
package com.symantec.cpe.analytics.core.managed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.symantec.cpe.analytics.KafkaMonitorConfiguration;
import com.symantec.cpe.analytics.core.kafka.KafkaConsumerGroupMetadata;
import com.symantec.cpe.analytics.kafka.KafkaConsumerOffsetUtil;

import io.dropwizard.lifecycle.Managed;

public class ZKClient implements Managed {

	private KafkaMonitorConfiguration kafkaConfiguration;
	private RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
	private CuratorFramework client;
	private static final List<String> nonSpoutConsumerNodes = Arrays.asList("storm", "config", "consumers",
			"controller_epoch", "zookeeper", "admin", "controller", "brokers");

	public ZKClient(KafkaMonitorConfiguration kafkaConfiguration) {
		this.kafkaConfiguration = kafkaConfiguration;
	}

	@Override
	public void start() throws Exception {
		client = CuratorFrameworkFactory.newClient(kafkaConfiguration.getZookeeperUrls(), retryPolicy);
		client.start();
	}

	@Override
	public void stop() throws Exception {
		client.close();
		KafkaConsumerOffsetUtil.closeConnection();
	}

	public List<KafkaConsumerGroupMetadata> getActiveRegularConsumersAndTopics() throws Exception {
		List<KafkaConsumerGroupMetadata> kafkaConsumerGroupMetadataList = new ArrayList<KafkaConsumerGroupMetadata>();
		Set<String> consumerGroups = new HashSet<String>((client.getChildren().forPath("/consumers")));
		for (String consumerGroup : consumerGroups) {
			if (client.checkExists().forPath("/consumers/" + consumerGroup + "/offsets") != null) {
				List<String> topics = client.getChildren().forPath("/consumers/" + consumerGroup + "/offsets");
				for (String topic : topics) {
					List<String> partitions = client.getChildren()
							.forPath("/consumers/" + consumerGroup + "/offsets/" + topic);
					Map<String, Long> partitionOffsetMap = new HashMap<String, Long>();
					for (String partition : partitions) {
						byte[] data = client.getData()
								.forPath("/consumers/" + consumerGroup + "/offsets/" + topic + "/" + partition);
						if (data != null) {
							long offset = Long.parseLong(new String(data));
							partitionOffsetMap.put(partition, offset);
						}
					}
					KafkaConsumerGroupMetadata kafkaConsumerGroupMetadata = new KafkaConsumerGroupMetadata(
							consumerGroup, topic, partitionOffsetMap);
					kafkaConsumerGroupMetadataList.add(kafkaConsumerGroupMetadata);
				}
			}
		}
		return kafkaConsumerGroupMetadataList;
	}

	public List<String> getActiveSpoutConsumerGroups() throws Exception {
		List<String> rootChildren = (client.getChildren().forPath(kafkaConfiguration.getCommonZkRoot()));
		List<String> activeSpoutConsumerGroupList = new ArrayList<String>();
		for (String rootChild : rootChildren) {
			if (!nonSpoutConsumerNodes.contains(rootChild)) {
				activeSpoutConsumerGroupList.add(rootChild);
			}
		}
		return activeSpoutConsumerGroupList;
	}

	public List<String> getChildren(String path) throws Exception {
		return client.getChildren().forPath(path);
	}

	public byte[] getData(String path) throws Exception {
		return client.getData().forPath(path);
	}
}