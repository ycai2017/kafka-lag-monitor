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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.symantec.cpe.analytics.core.kafka.Broker;

import java.util.List;

public class KafkaTopicPartitionMetadata {

    @JsonProperty
    Broker leader;

    @JsonProperty
    List<Broker> replicas;

    @JsonProperty
    List<Broker> isr;

    @JsonProperty
    Integer partitionId;

    public Broker getLeader() {
        return leader;
    }

    public void setLeader(Broker leader) {
        this.leader = leader;
    }

    public List<Broker> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<Broker> replicas) {
        this.replicas = replicas;
    }

    public List<Broker> getIsr() {
        return isr;
    }

    public void setIsr(List<Broker> isr) {
        this.isr = isr;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(Integer partitionId) {
        this.partitionId = partitionId;
    }
}
