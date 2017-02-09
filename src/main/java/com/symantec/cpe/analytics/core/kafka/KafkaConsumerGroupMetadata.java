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
package com.symantec.cpe.analytics.core.kafka;

import java.util.Map;

public class KafkaConsumerGroupMetadata {
    private String consumerGroup;
    private String topic;
    private Map<String, Long> partitionOffsetMap;

    public KafkaConsumerGroupMetadata(String consumerGroup, String topic, Map<String, Long> partitionOffsetMap) {
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.partitionOffsetMap = partitionOffsetMap;
    }

    public KafkaConsumerGroupMetadata() {
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Map<String, Long> getPartitionOffsetMap() {
        return partitionOffsetMap;
    }

    public void setPartitionOffsetMap(Map<String, Long> partitionOffsetMap) {
        this.partitionOffsetMap = partitionOffsetMap;
    }

    @Override
    public String toString() {
        return "KafkaConsumerGroupMetadata{" +
                "consumerGroup='" + consumerGroup + '\'' +
                ", topic='" + topic + '\'' +
                ", partitionOffsetMap=" + partitionOffsetMap +
                '}';
    }
}
