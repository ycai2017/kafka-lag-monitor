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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.srotya.monitoring.kafka.core.kafka.KafkaOffsetMonitor;

/**
 * @author ambud
 */
public class TestKafkaConsumerOffsetUtil {

	@Test
	public void testPrometheusSerialization() {
		List<KafkaOffsetMonitor> values = new ArrayList<>();
		values.add(new KafkaOffsetMonitor("cg1", "test1", 0, 123123L, 123123L, 0L));
		values.add(new KafkaOffsetMonitor("cg1", "test1", 1, 123123L, 123123L, 0L));
		values.add(new KafkaOffsetMonitor("cg1", "test1", 2, 123123L, 123123L, 0L));
		System.out.println(KafkaConsumerOffsetUtil.toPrometheusFormat(values));
	}
	
}
