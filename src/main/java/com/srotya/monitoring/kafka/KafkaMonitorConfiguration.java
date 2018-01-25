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

import io.dropwizard.Configuration;

import javax.validation.Valid;
import java.util.Arrays;

public class KafkaMonitorConfiguration extends Configuration {

	@Valid
	private String jaasConf = "./jaas.conf";

	@Valid
	private boolean kerberos = false;

	@Valid
	private String zookeeperUrls = "localhost:2181";

	@Valid
	private int refreshSeconds = 5;

	@Valid
	private String commonZkRoot = "";

	@Override
	public String toString() {
		return "KafkaMonitorConfiguration{" +
				"super='" + super.toString() + '\'' +
				"jaasConf='" + jaasConf + '\'' +
				", kerberos=" + kerberos +
				", zookeeperUrls='" + zookeeperUrls + '\'' +
				", refreshSeconds=" + refreshSeconds +
				", commonZkRoot='" + commonZkRoot + '\'' +
				", kafkaBroker=" + Arrays.toString(kafkaBroker) +
				", kafkaPort=" + kafkaPort +
				", consumerGroupName='" + consumerGroupName + '\'' +
				", sidewinderConfigPath='" + sidewinderConfigPath + '\'' +
				", enableHistory=" + enableHistory +
				", enableJMX=" + enableJMX +
				", jmxPort=" + jmxPort +
				'}';
	}

	@Valid
	private String[] kafkaBroker = new String[] { "localhost" };

	@Valid
	private int kafkaPort = 9092;

	@Valid
	private String consumerGroupName = "_kafka_monitor";

	@Valid
	private String sidewinderConfigPath = "";

	@Valid
	private boolean enableHistory = false;
	
	@Valid
	private boolean enableJMX = false;

	@Valid
	private int jmxPort = 9999;

	public String getZookeeperUrls() {
		return zookeeperUrls;
	}

	public void setZookeeperUrls(String zookeeperUrls) {
		this.zookeeperUrls = zookeeperUrls;
	}

	public int getRefreshSeconds() {
		return refreshSeconds;
	}

	public void setRefreshSeconds(int refreshSeconds) {
		this.refreshSeconds = refreshSeconds;
	}

	/**
	 * @return the kerberos
	 */
	public boolean isKerberos() {
		return kerberos;
	}

	/**
	 * @param kerberos
	 *            the kerberos to set
	 */
	public void setKerberos(boolean kerberos) {
		this.kerberos = kerberos;
	}

	/**
	 * @return the jaasConf
	 */
	public String getJaasConf() {
		System.out.println("#####getJaasConf - " + jaasConf);
		return jaasConf;
	}

	/**
	 * @param jaasConf
	 *            the jaasConf to set
	 */
	public void setJaasConf(String jaasConf) {
		this.jaasConf = jaasConf;
	}

	/**
	 * @return the commonZkRoot
	 */
	public String getCommonZkRoot() {
		return commonZkRoot;
	}

	/**
	 * @param commonZkRoot
	 *            the commonZkRoot to set
	 */
	public void setCommonZkRoot(String commonZkRoot) {
		this.commonZkRoot = commonZkRoot;
	}

	/**
	 * @return the kafkaBroker
	 */
	public String[] getKafkaBroker() {
		return kafkaBroker;
	}

	/**
	 * @param kafkaBroker
	 *            the kafkaBroker to set
	 */
	public void setKafkaBroker(String[] kafkaBroker) {
		this.kafkaBroker = kafkaBroker;
	}

	/**
	 * @return the kafkaPort
	 */
	public int getKafkaPort() {
		return kafkaPort;
	}

	/**
	 * @param kafkaPort
	 *            the kafkaPort to set
	 */
	public void setKafkaPort(int kafkaPort) {
		this.kafkaPort = kafkaPort;
	}

	/**
	 * @return the consumerGroupName
	 */
	public String getConsumerGroupName() {
		return consumerGroupName;
	}

	/**
	 * @param consumerGroupName
	 *            the consumerGroupName to set
	 */
	public void setConsumerGroupName(String consumerGroupName) {
		this.consumerGroupName = consumerGroupName;
	}

	/**
	 * @return the enableHistory
	 */
	public boolean isEnableHistory() {
		return enableHistory;
	}

	/**
	 * @param enableHistory
	 *            the enableHistory to set
	 */
	public void setEnableHistory(boolean enableHistory) {
		this.enableHistory = enableHistory;
	}

	/**
	 * @return the sidewinderConfigPath
	 */
	public String getSidewinderConfigPath() {
		return sidewinderConfigPath;
	}

	/**
	 * @param sidewinderConfigPath
	 *            the sidewinderConfigPath to set
	 */
	public void setSidewinderConfigPath(String sidewinderConfigPath) {
		this.sidewinderConfigPath = sidewinderConfigPath;
	}

	/**
	 * @return the jmxPort
	 */
	public int getJmxPort() {
		return jmxPort;
	}

	/**
	 * @param jmxPort the jmxPort to set
	 */
	public void setJmxPort(int jmxPort) {
		this.jmxPort = jmxPort;
	}

	/**
	 * @return the enableJMX
	 */
	public boolean isEnableJMX() {
		return enableJMX;
	}

	/**
	 * @param enableJMX the enableJMX to set
	 */
	public void setEnableJMX(boolean enableJMX) {
		this.enableJMX = enableJMX;
	}
}
