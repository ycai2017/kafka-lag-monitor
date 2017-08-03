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
package com.srotya.monitoring.kafka;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.util.HashMap;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import com.srotya.monitoring.kafka.core.managed.ZKClient;
import com.srotya.monitoring.kafka.resources.KafkaResource;
import com.srotya.monitoring.kafka.resources.KafkaThroughtputResource;
import com.srotya.monitoring.kafka.resources.PrometheusResource;
import com.srotya.monitoring.kafka.util.KafkaMBeanUtil;
import com.srotya.monitoring.kafka.util.KafkaUtils;
import com.srotya.sidewinder.core.api.grafana.GrafanaQueryApi;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class KafkaMonitor extends Application<KafkaMonitorConfiguration> {

	private Subject subject = null;
	private StorageEngine storageEngine;

	@Override
	public void initialize(Bootstrap<KafkaMonitorConfiguration> bootstrap) {
		// bootstrap.addBundle(new AssetsBundle("/web", "/", "index.html"));
	}

	@Override
	public void run(final KafkaMonitorConfiguration configuration, final Environment environment) throws Exception {
		if (configuration.isEnableHistory()) {
			storageEngine = new MemStorageEngine();
			storageEngine.configure(new HashMap<>());
		}
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
		Subject.doAs(subject, new PrivilegedAction<Void>() {

			@Override
			public Void run() {
				ZKClient zkClient = new ZKClient(configuration);
				environment.lifecycle().manage(zkClient);
				environment.jersey().setUrlPattern("/api/*");
				KafkaUtils.getInstance(zkClient);
				if (configuration.isEnableJMX()) {
					try {
						KafkaMBeanUtil.getInstance(configuration, storageEngine);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if (configuration.isEnableHistory()) {
					KafkaThroughtputResource throughputResource = new KafkaThroughtputResource(configuration, zkClient,
							storageEngine);
					environment.jersey().register(throughputResource);
					try {
						environment.jersey().register(new GrafanaQueryApi(storageEngine));
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				KafkaResource kafkaResource = new KafkaResource(configuration, zkClient, storageEngine);
				environment.jersey().register(kafkaResource);
				PrometheusResource prometheusResource = new PrometheusResource(configuration, zkClient, storageEngine);
				environment.jersey().register(prometheusResource);
				return null;
			}
		});
	}

	@Override
	public String getName() {
		return "kafka-monitor";
	}

	public static void main(String[] args) throws Exception {
		new KafkaMonitor().run(args);
	}

}
