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

import com.symantec.cpe.analytics.core.managed.ZKClient;
import com.symantec.cpe.analytics.resources.kafka.KafkaResource;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class KafkaMonitor extends Application<KafkaMonitorConfiguration> {

    @Override
    public void initialize(Bootstrap<KafkaMonitorConfiguration> bootstrap) {
    }

    @Override
    public void run(KafkaMonitorConfiguration configuration, Environment environment)
            throws Exception {
        ZKClient zkClient = new ZKClient(configuration);
        environment.lifecycle().manage(zkClient);
        KafkaResource kafkaResource = new KafkaResource(configuration, zkClient);
        environment.jersey().register(kafkaResource);
    }

    @Override
    public String getName() {
        return "kafka-monitor";
    }

    public static void main(String[] args) throws Exception {
        new KafkaMonitor().run(args);
    }

}
