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

import com.google.common.collect.ComparisonChain;
import com.symantec.cpe.analytics.core.kafka.KafkaOffsetMonitor;

import java.util.Comparator;

public class KafkaOffsetMonitorComparator implements Comparator<KafkaOffsetMonitor> {
    public int compare(KafkaOffsetMonitor kafkaOffsetMonitor1, KafkaOffsetMonitor kafkaOffsetMonitor2) {
        return ComparisonChain.start()
                .compare(kafkaOffsetMonitor1.getConsumerGroupName(), kafkaOffsetMonitor2.getConsumerGroupName())
                .compare(kafkaOffsetMonitor1.getTopic(), kafkaOffsetMonitor2.getTopic())
                .compare(kafkaOffsetMonitor1.getPartition(), kafkaOffsetMonitor2.getPartition())
                .result();
    }
}