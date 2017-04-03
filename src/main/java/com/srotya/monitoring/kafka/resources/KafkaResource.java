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

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
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

@Path("/kafka")
@Consumes(MediaType.APPLICATION_JSON)
public class KafkaResource {
	
	private KafkaMonitorConfiguration kafkaConfiguration;
	private static final Logger LOG = LoggerFactory.getLogger(KafkaResource.class);
	private ZKClient zkClient;

	public KafkaResource(KafkaMonitorConfiguration kafkaConfiguration, ZKClient zkClient) {
		this.kafkaConfiguration = kafkaConfiguration;
		this.zkClient = zkClient;
		KafkaConsumerOffsetUtil kafkaConsumerOffsetUtil = KafkaConsumerOffsetUtil.getInstance(kafkaConfiguration,
				zkClient);
		kafkaConsumerOffsetUtil.setupMonitoring();
	}

	@Path("/offset")
	@GET
	@Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_HTML })
	public Response getKafkaConsumerOffset(@DefaultValue("json") @QueryParam("outputType") String outputType) {
		String output = null;
		String responseType = MediaType.APPLICATION_JSON;
		try {
			KafkaConsumerOffsetUtil kafkaConsumerOffsetUtil = KafkaConsumerOffsetUtil.getInstance(kafkaConfiguration,
					zkClient);
			List<KafkaOffsetMonitor> kafkaOffsetMonitors = new ArrayList<>(kafkaConsumerOffsetUtil.getReferences().get());
			kafkaOffsetMonitors.addAll(kafkaConsumerOffsetUtil.getNewConsumer().values());
			kafkaOffsetMonitors.addAll(kafkaConsumerOffsetUtil.getTopicOffsets());
			switch (outputType) {
			case "html":
				responseType = MediaType.TEXT_HTML;
				output = kafkaConsumerOffsetUtil.htmlOutput(kafkaOffsetMonitors);
				break;
			case "json":
				ObjectMapper mapper = new ObjectMapper();
				output = mapper.writeValueAsString(kafkaOffsetMonitors);
				break;
			case "prometheus":
				output = KafkaConsumerOffsetUtil.toPrometheusFormat(kafkaOffsetMonitors);
				break;
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(new ResponseMessage("Error Occurred during processing")).type(responseType).build();
		}
		return Response.status(Response.Status.OK).entity(output).type(responseType).build();
	}

}
