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

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.srotya.monitoring.kafka.KafkaMonitorConfiguration;
import com.srotya.monitoring.kafka.core.kafka.KafkaOffsetMonitor;
import com.srotya.monitoring.kafka.core.managed.ZKClient;
import com.srotya.monitoring.kafka.util.KafkaConsumerOffsetUtil;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.common.TextFormat;

/**
 * @author ambud
 */
@Path("/metrics")
public class PrometheusResource {

	private KafkaMonitorConfiguration kafkaConfiguration;
	private ZKClient zkClient;

	public PrometheusResource(KafkaMonitorConfiguration kafkaConfiguration, ZKClient zkClient) {
		this.kafkaConfiguration = kafkaConfiguration;
		this.zkClient = zkClient;
		KafkaConsumerOffsetUtil kafkaConsumerOffsetUtil = KafkaConsumerOffsetUtil.getInstance(kafkaConfiguration,
				zkClient);
		kafkaConsumerOffsetUtil.setupMonitoring();
	}

	@GET
	@Produces({ MediaType.TEXT_PLAIN })
	public String getKafkaOffsets() throws IOException {
		CollectorRegistry registry = new CollectorRegistry();
		KafkaConsumerOffsetUtil kafkaConsumerOffsetUtil = KafkaConsumerOffsetUtil.getInstance(kafkaConfiguration,
				zkClient);
		List<KafkaOffsetMonitor> kafkaOffsetMonitors = new ArrayList<>(kafkaConsumerOffsetUtil.getReferences().get());
		kafkaOffsetMonitors.addAll(kafkaConsumerOffsetUtil.getNewConsumer().values());

		Gauge conOffset = Gauge.build("consumer_offset", "Consumer offsets").labelNames("topic", "group", "partition")
				.create();
		Gauge prodOffset = Gauge.build("producer_offset", "Producer offsets").labelNames("topic", "group", "partition")
				.create();
		Gauge lag = Gauge.build("lag", "Producer-Consumer lag").labelNames("topic", "group", "partition").create();
		
		registry.register(lag);
		registry.register(prodOffset);
		registry.register(conOffset);

		for (KafkaOffsetMonitor mon : kafkaOffsetMonitors) {
			conOffset.labels(mon.getTopic(), mon.getConsumerGroupName(), String.valueOf(mon.getPartition()))
					.set(mon.getConsumerOffset());
			prodOffset.labels(mon.getTopic(), mon.getConsumerGroupName(), String.valueOf(mon.getPartition()))
					.set(mon.getLogSize());
			lag.labels(mon.getTopic(), mon.getConsumerGroupName(), String.valueOf(mon.getPartition()))
					.set(mon.getLag());
		}

		StringWriter writer = new StringWriter();
		TextFormat.write004(writer, registry.metricFamilySamples());
		return writer.toString();
	}

}
