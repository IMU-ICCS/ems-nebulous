/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.service;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import static gr.iccs.imu.ems.util.EmsConstant.EMS_PROPERTIES_PREFIX;

@Slf4j
@Data
@Validated
@Configuration
@ConfigurationProperties(prefix = EMS_PROPERTIES_PREFIX + "external")
public class ExternalBrokerServiceProperties implements InitializingBean {
    private final static String NEBULOUS_TOPIC_PREFIX = "eu.nebulouscloud.";

    private boolean enabled = true;
    private String brokerAddress;
    private int brokerPort;
    private String brokerUsername;
    @ToString.Exclude
    private String brokerPassword;
    private int healthTimeout = 60;
    private int retryDelay = 10;

    private String componentName = "monitoring";
    private String applicationIdPropertyName = "application";

    private String commandsTopic         = NEBULOUS_TOPIC_PREFIX + String.format("%s.%s", componentName, "commands");
    private String commandsResponseTopic = NEBULOUS_TOPIC_PREFIX + String.format("%s.%s", componentName, "commands.reply");
    private String modelsTopic           = NEBULOUS_TOPIC_PREFIX + "ui.dsl.metric_model";
    private String modelsResponseTopic   = NEBULOUS_TOPIC_PREFIX + "ui.dsl.metric_model.reply";
    private String metricsTopicPrefix    = NEBULOUS_TOPIC_PREFIX + String.format("%s.realtime.", componentName);
    private String combinedSloTopic      = NEBULOUS_TOPIC_PREFIX + String.format("%s.slo.severity_value", componentName);

    @Override
    public void afterPropertiesSet() {
        log.debug("ExternalBrokerServiceProperties: {}", this);
    }
}