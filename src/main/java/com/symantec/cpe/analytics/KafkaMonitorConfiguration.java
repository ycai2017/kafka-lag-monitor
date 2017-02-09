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
package com.symantec.cpe.analytics;

import io.dropwizard.Configuration;

import javax.validation.Valid;

public class KafkaMonitorConfiguration extends Configuration {

    @Valid
    private String zookeeperUrls = "localhost:2181";

    @Valid
    private int refreshSeconds = 10;

    @Valid
    private String statsDHost = "localhost";

    @Valid
    private int statsDPort = 8125;

    @Valid
    private String statsDPrefix = "kafka-monitoring";

    @Valid
    private boolean pushToStatsD = false;

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

    public String getStatsDHost() {
        return statsDHost;
    }

    public void setStatsDHost(String statsDHost) {
        this.statsDHost = statsDHost;
    }

    public int getStatsDPort() {
        return statsDPort;
    }

    public void setStatsDPort(int statsDPort) {
        this.statsDPort = statsDPort;
    }

    public String getStatsDPrefix() {
        return statsDPrefix;
    }

    public void setStatsDPrefix(String statsDPrefix) {
        this.statsDPrefix = statsDPrefix;
    }

    public boolean isPushToStatsD() {
        return pushToStatsD;
    }

    public void setPushToStatsD(boolean pushToStatsD) {
        this.pushToStatsD = pushToStatsD;
    }
}
