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
package com.srotya.monitoring.kafka.collectd;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.collectd.api.Collectd;
import org.collectd.api.CollectdConfigInterface;
import org.collectd.api.CollectdInitInterface;
import org.collectd.api.CollectdReadInterface;
import org.collectd.api.CollectdShutdownInterface;
import org.collectd.api.OConfigItem;
import org.collectd.api.PluginData;
import org.collectd.api.ValueList;

import com.srotya.monitoring.kafka.KafkaMonitorConfiguration;
import com.srotya.monitoring.kafka.core.kafka.KafkaOffsetMonitor;
import com.srotya.monitoring.kafka.core.managed.ZKClient;
import com.srotya.monitoring.kafka.util.KafkaConsumerOffsetUtil;
import com.srotya.monitoring.kafka.util.KafkaOffsetMonitorComparator;

/**
 * @author ambud
 */
public class KafkaLagMonitor
		implements CollectdConfigInterface, CollectdReadInterface, CollectdShutdownInterface, CollectdInitInterface {

	private static final String PLUGIN_NAME = "lag";
	private String jaas = "./jaas.conf";
	private boolean kerberos = false;
	private String kafkaBroker = "localhost";
	private int kafkaPort = 9092;
	private String zkRoot = "/";
	private String zkUrl = "localhost:2181";
	private KafkaMonitorConfiguration configuration;
	private Subject subject;
	private ZKClient zkClient;
	private String alias;

	public KafkaLagMonitor() {
		Collectd.registerConfig(PLUGIN_NAME, this);
		Collectd.registerInit(PLUGIN_NAME, this);
		Collectd.registerRead(PLUGIN_NAME, this);
		Collectd.registerShutdown(PLUGIN_NAME, this);
	}

	@Override
	public int shutdown() {
		return 0;
	}

	@Override
	public int read() {
		KafkaConsumerOffsetUtil util = KafkaConsumerOffsetUtil.getInstance(configuration, zkClient);
		try {
			Subject subject = null;
			if (configuration.isKerberos()) {
				LoginContext lc = new LoginContext("Client");
				lc.login();
				subject = lc.getSubject();
			} else {
				Subject.getSubject(AccessController.getContext());
			}
			AtomicReference<ArrayList<KafkaOffsetMonitor>> references = new AtomicReference<ArrayList<KafkaOffsetMonitor>>(
					null);
			Subject.doAs(subject, new PrivilegedAction<Void>() {

				@Override
				public Void run() {
					try {
						ArrayList<KafkaOffsetMonitor> kafkaOffsetMonitors = new ArrayList<KafkaOffsetMonitor>();
						kafkaOffsetMonitors.addAll(util.getSpoutKafkaOffsetMonitors());
						kafkaOffsetMonitors.addAll(util.getRegularKafkaOffsetMonitors());
						Collections.sort(kafkaOffsetMonitors, new KafkaOffsetMonitorComparator());
						references.set(kafkaOffsetMonitors);
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
					return null;
				}
			});
			if (references.get() != null) {
				for (KafkaOffsetMonitor monitor : references.get()) {
					PluginData pd = new PluginData();
					pd.setPluginInstance(monitor.getTopic() + "-" + monitor.getPartition());
					pd.setTime(System.currentTimeMillis());
					pd.setHost(alias);
					ValueList values = new ValueList(pd);
					values.setType("records");
					values.setPlugin(monitor.getConsumerGroupName());
					values.setTypeInstance("lag");
					values.setValues(Arrays.asList(monitor.getLag()));
					Collectd.logDebug("Dispatching data:" + values);
					Collectd.dispatchValues(values);

					values.setTypeInstance("offset");
					values.setValues(Arrays.asList(monitor.getConsumerOffset()));
					Collectd.logDebug("Dispatching data:" + values);
					Collectd.dispatchValues(values);

					values.setTypeInstance("log");
					values.setValues(Arrays.asList(monitor.getLogSize()));
					Collectd.logDebug("Dispatching data:" + values);
					Collectd.dispatchValues(values);
				}
			}
		} catch (Exception e) {
			Collectd.logError("Failed to get offsets:" + e.getMessage());
			return -1;
		}
		return 0;
	}

	@Override
	public int config(OConfigItem conf) {
		List<OConfigItem> subConf = conf.getChildren();
		for (OConfigItem item : subConf) {
			System.err.println("Configuring:" + item);
			switch (item.getKey().toLowerCase()) {
			case "jaas":
				jaas = item.getValues().iterator().next().getString();
				break;
			case "kerberos":
				kerberos = item.getValues().iterator().next().getBoolean();
				break;
			case "kafkahost":
				kafkaBroker = item.getValues().iterator().next().getString();
				break;
			case "kafkaport":
				kafkaPort = item.getValues().iterator().next().getNumber().intValue();
				break;
			case "zkurl":
				zkUrl = item.getValues().iterator().next().getString();
				break;
			case "zkroot":
				zkRoot = item.getValues().iterator().next().getString();
				break;
			case "alias":
				alias = item.getValues().iterator().next().getString();
				break;
			}
		}
		configuration = new KafkaMonitorConfiguration();
		configuration.setCommonZkRoot(zkRoot);
		configuration.setJaasConf(jaas);
		configuration.setKerberos(kerberos);
		configuration.setKafkaBroker(kafkaBroker);
		configuration.setKafkaPort(kafkaPort);
		configuration.setZookeeperUrls(zkUrl);
		try {
			if (configuration.isKerberos()) {
				System.setProperty("java.security.auth.login.config", configuration.getJaasConf());
				System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
				System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
				System.setProperty("security.protocol", "PLAINTEXTSASL");
				System.setProperty("sasl.kerberos.service.name", "kafka");
				System.out.println("Using kerberos");
				LoginContext lc = new LoginContext("Client");
				lc.login();
				subject = lc.getSubject();
			} else {
				System.setProperty("security.protocol", "PLAINTEXT");
				subject = Subject.getSubject(AccessController.getContext());
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		Subject.doAs(subject, new PrivilegedAction<Void>() {

			@Override
			public Void run() {
				zkClient = new ZKClient(configuration);
				try {
					zkClient.start();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				return null;
			}
		});
		return 0;
	}

	@Override
	public int init() {
		return 0;
	}

}
