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
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
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
import com.srotya.monitoring.kafka.util.KafkaUtils;
import com.srotya.sidewinder.core.storage.StorageEngine;

@Path("/")
@Consumes(MediaType.APPLICATION_JSON)
public class KafkaResource {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaResource.class);
	private KafkaConsumerOffsetUtil kafkaConsumerOffsetUtil;
	private KafkaUtils utils;

	public KafkaResource(KafkaMonitorConfiguration kafkaConfiguration, ZKClient zkClient, StorageEngine server) {
		kafkaConsumerOffsetUtil = KafkaConsumerOffsetUtil.getInstance(kafkaConfiguration, zkClient,
				kafkaConfiguration.isEnableHistory(), server);
		utils = KafkaUtils.getInstance(zkClient);
	}

	@Path("/nodes")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public int getNodes() throws Exception {
		return utils.getBrokerCount();
	}

	@Path("/topics")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public List<String> getTopics() throws Exception {
		return new ArrayList<>(utils.getTopicMap().keySet());
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

	@Path("/replication")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public int getAverageReplication() throws Exception {
		try {
			int sum = utils.getTopicMap().values().stream().mapToInt(v -> v.getReplicationFactor()).sum();
			return sum / utils.getTopicMap().size();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Path("/partitions")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public int getPartitions() {
		return utils.getTopicMap().values().stream().mapToInt(v -> v.getPartitionCount()).sum();
	}

	@Path("/topic/{topic}/partitions")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public int getPartitions(@PathParam("topic") String topic) {
		return utils.getTopicMap().get(topic).getPartitionCount();
	}

	@Path("/topic/{topic}/replication")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public int getReplication(@PathParam("topic") String topic) {
		return utils.getTopicMap().get(topic).getReplicationFactor();
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
