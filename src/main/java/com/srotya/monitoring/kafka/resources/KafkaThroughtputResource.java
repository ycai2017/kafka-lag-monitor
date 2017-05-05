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
package com.srotya.monitoring.kafka.resources;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.srotya.monitoring.kafka.KafkaMonitorConfiguration;
import com.srotya.monitoring.kafka.core.managed.ZKClient;
import com.srotya.monitoring.kafka.util.KafkaConsumerOffsetUtil;
import com.srotya.monitoring.kafka.util.KafkaMBeanUtil;
import com.srotya.sidewinder.core.aggregators.AggregationFunction;
import com.srotya.sidewinder.core.aggregators.windowed.DerivativeFunction;
import com.srotya.sidewinder.core.filters.AnyFilter;
import com.srotya.sidewinder.core.filters.ContainsFilter;
import com.srotya.sidewinder.core.filters.Filter;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
public class KafkaThroughtputResource {
	
	private StorageEngine server;
	private KafkaMonitorConfiguration kafkaConfiguration;
	
	public KafkaThroughtputResource(KafkaMonitorConfiguration kafkaConfiguration, ZKClient zkClient, StorageEngine server) {
		this.kafkaConfiguration = kafkaConfiguration;
		this.server = server;
	}
	
	@Path("/topics/{topic}/throughput")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public long getTopicThroughput(@PathParam("topic") String topic, @QueryParam("last") long last) throws Exception {
		long now = System.currentTimeMillis();
		if (last == 0) {
			last = now - 60_000;
		} else {
			last = now - (last * 60_000);
		}
		AggregationFunction aggregagateFunction = new DerivativeFunction();
		int window = kafkaConfiguration.getRefreshSeconds();
		aggregagateFunction.init(new Object[] { window });
		Filter<List<String>> filter = new ContainsFilter<String, List<String>>(topic);
		Map<String, List<DataPoint>> results = server.queryDataPoints(KafkaConsumerOffsetUtil.DB_NAME,
				KafkaConsumerOffsetUtil.TOPIC, KafkaConsumerOffsetUtil.OFFSET, last, now, Arrays.asList(topic), filter,
				null, aggregagateFunction);
		long throughput = 0;

		for (Entry<String, List<DataPoint>> entry : results.entrySet()) {
			long sum = 0;
			List<DataPoint> values = entry.getValue();
			for (DataPoint dp : values) {
				sum += dp.getLongValue();
			}
			throughput += (sum / values.size());
		}
		return throughput;
	}

	@Path("/throughputm")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public long getMessageThroughput(@QueryParam("last") long last) throws Exception {
		long now = System.currentTimeMillis();
		if (last == 0) {
			last = now - 60_000;
		} else {
			last = now - (last * 60_000);
		}
		AggregationFunction aggregagateFunction = new DerivativeFunction();
		int window = kafkaConfiguration.getRefreshSeconds();
		aggregagateFunction.init(new Object[] { window });
		Filter<List<String>> filter = new AnyFilter<>();
		Map<String, List<DataPoint>> results = server.queryDataPoints(KafkaConsumerOffsetUtil.DB_NAME,
				KafkaMBeanUtil.THROUGHPUT, KafkaConsumerOffsetUtil.OFFSET, last, now, null, filter, null,
				aggregagateFunction);
		long throughput = 0;

		for (Entry<String, List<DataPoint>> entry : results.entrySet()) {
			long sum = 0;
			List<DataPoint> values = entry.getValue();
			for (DataPoint dp : values) {
				sum += dp.getLongValue();
			}
			throughput += (sum / values.size());
		}
		return throughput;
	}

	@Path("/throughputb")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public long getThroughput(@QueryParam("last") long last) throws Exception {
		long now = System.currentTimeMillis();
		if (last == 0) {
			last = now - 60_000;
		} else {
			last = now - (last * 60_000);
		}
		AggregationFunction aggregagateFunction = new DerivativeFunction();
		int window = kafkaConfiguration.getRefreshSeconds();
		aggregagateFunction.init(new Object[] { window });
		Filter<List<String>> filter = new AnyFilter<>();
		Map<String, List<DataPoint>> results = server.queryDataPoints(KafkaConsumerOffsetUtil.DB_NAME,
				KafkaMBeanUtil.THROUGHPUT, KafkaMBeanUtil.BYTES, last, now, null, filter, null, aggregagateFunction);
		long throughput = 0;

		for (Entry<String, List<DataPoint>> entry : results.entrySet()) {
			long sum = 0;
			List<DataPoint> values = entry.getValue();
			for (DataPoint dp : values) {
				sum += dp.getLongValue();
			}
			throughput += (sum / values.size());
		}
		return throughput;
	}

}