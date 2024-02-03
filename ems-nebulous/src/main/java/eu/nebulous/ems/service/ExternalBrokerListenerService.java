/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.service;

import eu.nebulouscloud.exn.core.Consumer;
import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Handler;
import gr.iccs.imu.ems.control.controller.ControlServiceCoordinator;
import gr.iccs.imu.ems.control.controller.ControlServiceRequestInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ExternalBrokerListenerService extends AbstractExternalBrokerService implements InitializingBean {
	private final ControlServiceCoordinator controlServiceCoordinator;
	private List<Consumer> consumers;

	public ExternalBrokerListenerService(ExternalBrokerServiceProperties properties,
										 TaskScheduler taskScheduler,
										 ControlServiceCoordinator controlServiceCoordinator)
	{
		super(properties, taskScheduler);
		this.controlServiceCoordinator = controlServiceCoordinator;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (checkProperties()) {
			initializeConsumers();
			connectToBroker(List.of(), consumers);
		} else {
			log.warn("ExternalBrokerListenerService: Not configured or misconfigured. Will not initialize");
		}
	}

	private void initializeConsumers() {
		// Create message handler
		Handler messageHandler = new Handler() {
			@Override
			public void onMessage(String key, String address, Map body, Message message, Context context) {
				try {
					super.onMessage(key, address, body, message, context);
					processMessage(key, address, body, message, context);
				} catch (Exception e) {
					log.warn("ExternalBrokerListenerService: Error while processing message: ", e);
				}
			}
		};

		// Create consumers for each topic of interest
		consumers = List.of(
				new Consumer(COMMANDS_TOPIC, COMMANDS_TOPIC, messageHandler),
				new Consumer(MODELS_TOPIC, MODELS_TOPIC, messageHandler)
		);
		log.debug("ExternalBrokerListenerService: initialized subscribers");
	}

	private void processMessage(String key, String address, Map body, Message message, Context context) throws ClientException {
		log.warn("ExternalBrokerListenerService: Message: key={}, address={}, body={}, message={}, context={}",
				key, address, body, message, context);
		if (COMMANDS_TOPIC.equals(key)) {
			log.warn("ExternalBrokerListenerService: Received a command from external broker: {}", body);
			String command = body.getOrDefault("command", "").toString();
			log.warn("ExternalBrokerListenerService: Command: {}", command);
		} else
		if (MODELS_TOPIC.equals(key)) {
			log.warn("ExternalBrokerListenerService: Received a new Metric Model message from external broker: {}", body);
			String modelStr = body.getOrDefault("model", "").toString();
			Object propApp = message.property(APPLICATION_PROPERTY);
			String appId = propApp != null ? propApp.toString() : null;
			if (StringUtils.isBlank(modelStr)) {
				log.warn("ExternalBrokerListenerService: No model found in Metric Model message: {}", body);
				return;
			}
			if (StringUtils.isBlank(appId)) {
				log.warn("ExternalBrokerListenerService: No Application Id found in Metric Model message: {}", body);
				return;
			}
			controlServiceCoordinator.processAppModel(modelStr, null, ControlServiceRequestInfo.EMPTY);
		}
	}
}