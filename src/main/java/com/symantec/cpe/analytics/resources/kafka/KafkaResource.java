package com.symantec.cpe.analytics.resources.kafka;

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
import com.symantec.cpe.analytics.KafkaMonitorConfiguration;
import com.symantec.cpe.analytics.core.ResponseMessage;
import com.symantec.cpe.analytics.core.kafka.KafkaOffsetMonitor;
import com.symantec.cpe.analytics.core.managed.ZKClient;
import com.symantec.cpe.analytics.kafka.KafkaConsumerOffsetUtil;

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
			List<KafkaOffsetMonitor> kafkaOffsetMonitors = kafkaConsumerOffsetUtil.getReferences().get();
			if (outputType.equals("html")) {
				responseType = MediaType.TEXT_HTML;
				output = kafkaConsumerOffsetUtil.htmlOutput(kafkaOffsetMonitors);
			} else {
				ObjectMapper mapper = new ObjectMapper();
				output = mapper.writeValueAsString(kafkaOffsetMonitors);
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(new ResponseMessage("Error Occurred during processing")).type(responseType).build();
		}
		return Response.status(Response.Status.OK).entity(output).type(responseType).build();
	}
}
