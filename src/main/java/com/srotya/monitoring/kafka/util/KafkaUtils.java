/**
 * Copyright 2017 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.monitoring.kafka.util;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.srotya.monitoring.kafka.core.managed.ZKClient;

/**
 * @author ambud
 */
public class KafkaUtils {

	private static KafkaUtils instance;
	private ZKClient zkClient;
	private Map<String, TopicMetadata> topicMap;
	private volatile int brokerCount;

	public static KafkaUtils getInstance(ZKClient zkClient) {
		if (instance == null) {
			instance = new KafkaUtils(zkClient);
		}
		return instance;
	}

	private KafkaUtils(ZKClient zkClient) {
		this.zkClient = zkClient;
		this.topicMap = new ConcurrentHashMap<>();
		ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
		es.scheduleAtFixedRate(() -> {
			try {
				brokerCount = brokerCount();
				for (String topic : getTopics()) {
					TopicMetadata md = new TopicMetadata(getPartitionCountForTopic(topic), getReplicationFactor(topic));
					topicMap.put(topic, md);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}, 5, 10, TimeUnit.SECONDS);
	}
	
	public int brokerCount() throws Exception {
		return zkClient.getChildren("/brokers/ids").size();
	}

	public List<String> getTopics() throws Exception {
		List<String> children = zkClient.getChildren("/brokers/topics");
		return children;
	}

	public int getPartitionCountForTopic(String topic) throws Exception {
		topic = new String(zkClient.getData("/brokers/topics/" + topic));
		Gson gson = new Gson();
		JsonObject obj = gson.fromJson(topic, JsonObject.class);
		obj = obj.get("partitions").getAsJsonObject();
		Set<Entry<String, JsonElement>> entrySet = obj.entrySet();
		return entrySet.size();
	}

	public int getReplicationFactor(String topic) throws Exception {
		topic = new String(zkClient.getData("/brokers/topics/" + topic));
		Gson gson = new Gson();
		JsonObject obj = gson.fromJson(topic, JsonObject.class);
		obj = obj.get("partitions").getAsJsonObject();
		Set<Entry<String, JsonElement>> entrySet = obj.entrySet();
		Entry<String, JsonElement> next = entrySet.iterator().next();
		return next.getValue().getAsJsonArray().size();
	}

	/**
	 * @return the brokerCount
	 */
	public int getBrokerCount() {
		return brokerCount;
	}

	/**
	 * @return the topicMap
	 */
	public Map<String, TopicMetadata> getTopicMap() {
		return topicMap;
	}

	public static class TopicMetadata {

		private int partitionCount;
		private int replicationFactor;

		public TopicMetadata(int partitionCount, int replicationFactor) {
			this.partitionCount = partitionCount;
			this.replicationFactor = replicationFactor;
		}

		/**
		 * @return the partitionCount
		 */
		public int getPartitionCount() {
			return partitionCount;
		}

		/**
		 * @param partitionCount
		 *            the partitionCount to set
		 */
		public void setPartitionCount(int partitionCount) {
			this.partitionCount = partitionCount;
		}

		/**
		 * @return the replicationFactor
		 */
		public int getReplicationFactor() {
			return replicationFactor;
		}

		/**
		 * @param replicationFactor
		 *            the replicationFactor to set
		 */
		public void setReplicationFactor(int replicationFactor) {
			this.replicationFactor = replicationFactor;
		}

	}
}
