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
package com.srotya.monitoring.kafka.resources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.srotya.monitoring.kafka.KafkaMonitorConfiguration;
import com.srotya.monitoring.kafka.core.ResponseMessage;
import com.srotya.monitoring.kafka.core.kafka.KafkaOffsetMonitor;
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

@Path("/")
@Consumes(MediaType.APPLICATION_JSON)
public class KafkaResource {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaResource.class);
	private KafkaConsumerOffsetUtil kafkaConsumerOffsetUtil;
	private StorageEngine server;
	private KafkaMonitorConfiguration kafkaConfiguration;

	public KafkaResource(KafkaMonitorConfiguration kafkaConfiguration, ZKClient zkClient, StorageEngine server) {
		this.kafkaConfiguration = kafkaConfiguration;
		this.server = server;
		kafkaConsumerOffsetUtil = KafkaConsumerOffsetUtil.getInstance(kafkaConfiguration, zkClient,
				kafkaConfiguration.isEnableHistory(), server);
	}

	@Path("/topics")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public List<String> getTopics() {
		return new ArrayList<>(kafkaConsumerOffsetUtil.getTopics());
	}

	@Path("/topics/{topic}/offset")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public Response getKafkaConsumerOffset(@PathParam("topic") String topic) {
		String output = null;
		String responseType = MediaType.APPLICATION_JSON;
		try {
			List<KafkaOffsetMonitor> kafkaOffsetMonitors = new ArrayList<>();
			Map<String, KafkaOffsetMonitor> map = kafkaConsumerOffsetUtil.getNewConsumer().get(topic);
			if (map != null) {
				kafkaOffsetMonitors.addAll(map.values());
			}
			ObjectMapper mapper = new ObjectMapper();
			output = mapper.writeValueAsString(kafkaOffsetMonitors);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(new ResponseMessage("Error Occurred during processing")).type(responseType).build();
		}
		return Response.status(Response.Status.OK).entity(output).type(responseType).build();
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

	@Path("/brokers/throughput")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public long getBrokerThroughput(@QueryParam("last") long last) throws Exception {
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

	@Path("/offset")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public Response getKafkaConsumerOffset() {
		String output = null;
		String responseType = MediaType.APPLICATION_JSON;
		try {
			List<KafkaOffsetMonitor> kafkaOffsetMonitors = new ArrayList<>();
			for (Map<String, KafkaOffsetMonitor> map : kafkaConsumerOffsetUtil.getNewConsumer().values()) {
				kafkaOffsetMonitors.addAll(map.values());
			}
			kafkaOffsetMonitors.addAll(kafkaConsumerOffsetUtil.getTopicOffsets());
			ObjectMapper mapper = new ObjectMapper();
			output = mapper.writeValueAsString(kafkaOffsetMonitors);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(new ResponseMessage("Error Occurred during processing")).type(responseType).build();
		}
		return Response.status(Response.Status.OK).entity(output).type(responseType).build();
	}

}
