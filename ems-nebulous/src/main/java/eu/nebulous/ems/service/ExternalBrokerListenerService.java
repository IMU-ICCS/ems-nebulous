/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.service;

import eu.nebulous.ems.translate.NebulousEmsTranslatorProperties;
import eu.nebulouscloud.exn.core.Consumer;
import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Handler;
import eu.nebulouscloud.exn.core.Publisher;
import gr.iccs.imu.ems.control.controller.ControlServiceCoordinator;
import gr.iccs.imu.ems.control.controller.ControlServiceRequestInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

@Slf4j
@Service
public class ExternalBrokerListenerService extends AbstractExternalBrokerService implements InitializingBean {
	private final NebulousEmsTranslatorProperties translatorProperties;
	private final ControlServiceCoordinator controlServiceCoordinator;
	private final ArrayBlockingQueue<Command> commandQueue = new ArrayBlockingQueue<>(100);
	private List<Consumer> consumers;
	private Publisher commandsResponsePublisher;
	private Publisher modelsResponsePublisher;

	record Command(String key, String address, Map body, Message message, Context context) {
	}

	public ExternalBrokerListenerService(ExternalBrokerServiceProperties properties,
                                         TaskScheduler taskScheduler,
										 NebulousEmsTranslatorProperties translatorProperties,
                                         ControlServiceCoordinator controlServiceCoordinator)
	{
		super(properties, taskScheduler);
        this.translatorProperties = translatorProperties;
        this.controlServiceCoordinator = controlServiceCoordinator;
    }

	@Override
	public void afterPropertiesSet() throws Exception {
		if (!properties.isEnabled()) {
			log.info("ExternalBrokerListenerService: Disabled due to configuration");
			return;
		}
		if (checkProperties()) {
			initializeConsumers();
			initializePublishers();
			startCommandProcessor();
			connectToBroker(List.of(commandsResponsePublisher, modelsResponsePublisher), consumers);
			log.info("ExternalBrokerListenerService: Initialized listeners");
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
					commandQueue.add(new Command(key, address, body, message, context));
				} catch (IllegalStateException e) {
					log.warn("ExternalBrokerListenerService: Commands queue is full. Dropping command: queue-size={}", commandQueue.size());
				} catch (Exception e) {
					log.warn("ExternalBrokerListenerService: Error while processing message: ", e);
				}
			}
		};

		// Create consumers for each topic of interest
		consumers = List.of(
				new Consumer(properties.getCommandsTopic(), properties.getCommandsTopic(), messageHandler),
				new Consumer(properties.getModelsTopic(), properties.getModelsTopic(), messageHandler)
		);
		log.debug("ExternalBrokerListenerService: initialized subscribers");
	}

	private void initializePublishers() {
		commandsResponsePublisher = new Publisher(properties.getCommandsResponseTopic(), properties.getCommandsResponseTopic(), true, false);
		modelsResponsePublisher = new Publisher(properties.getModelsResponseTopic(), properties.getModelsResponseTopic(), true, false);
	}

	private void startCommandProcessor() {
		taskScheduler.schedule(()->{
			while (true) {
				try {
					Command command = commandQueue.take();
					processMessage(command);
				} catch (InterruptedException e) {
                    log.warn("ExternalBrokerListenerService: Command processor interrupted. Exiting process loop");
					break;
                } catch (Exception e) {
					log.warn("ExternalBrokerListenerService: Exception while processing command: {}\n", commandQueue, e);
				}
            }
		}, Instant.now());
	}

	private void processMessage(Command command) throws ClientException, IOException {
		log.warn("ExternalBrokerListenerService: Command: {}", command);
		if (properties.getCommandsTopic().equals(command.key)) {
			// Process command
			log.warn("ExternalBrokerListenerService: Received a command from external broker: {}", command.body);

			// Get application id
			String appId = getAppId(command, commandsResponsePublisher);
			if (appId == null) return;

			// Get command string
			String commandStr = command.body.getOrDefault("command", "").toString();
			log.warn("ExternalBrokerListenerService: Command: {}", commandStr);

			sendResponse(commandsResponsePublisher, appId, "ERROR: ---NOT YET IMPLEMENTED---: "+command.body);
		} else
		if (properties.getModelsTopic().equals(command.key)) {
			// Process metric model message
			log.warn("ExternalBrokerListenerService: Received a new Metric Model message from external broker: {}", command.body);

			// Get application id
			String appId = getAppId(command, modelsResponsePublisher);
			if (appId == null) return;

			// Get model string and/or model file
			String modelStr = command.body.getOrDefault("body", "").toString();
			if (StringUtils.isBlank(modelStr)) {
				modelStr = command.body.getOrDefault("model", "").toString();
			}
			String modelFile = command.body.getOrDefault("model-path", "").toString();

			// Check if 'model' or 'model-path' is provided
			if (StringUtils.isBlank(modelStr) && StringUtils.isBlank(modelFile)) {
				log.warn("ExternalBrokerListenerService: No model found in Metric Model message: {}", command.body);
				sendResponse(modelsResponsePublisher, appId, "ERROR: No model found in Metric Model message: "+command.body);
				return;
			}

			// If 'model' string is provided, store it in a file
			if (StringUtils.isNotBlank(modelStr)) {
				modelFile = StringUtils.isBlank(modelFile) ? getModelFile(appId) : modelFile;
				storeModel(modelFile, modelStr);
			}

			// Call control-service to process model, also pass a callback to get the result
			controlServiceCoordinator.processAppModel(modelFile, null,
					ControlServiceRequestInfo.create(appId, null, null, null,
							(result) -> {
								// Send message with the processing result
								log.info("ExternalBrokerListenerService: Metric model processing result: {}", result);
								sendResponse(modelsResponsePublisher, appId, result);
							}));
		}
	}

	private String getAppId(Command command, Publisher publisher) throws ClientException {
		Object propApp = command.message.property(properties.getApplicationIdPropertyName());
		String appId = propApp != null ? propApp.toString() : null;

		// Check if 'applicationId' is provided
		if (StringUtils.isBlank(appId)) {
			log.warn("ExternalBrokerListenerService: No Application Id found in message: {}", command.body);
			sendResponse(publisher, appId, "ERROR: No Application Id found in message: "+ command.body);
			return null;
		}
		return appId;
	}

	private String getModelFile(String appId) {
		return String.format("model-%s--%d.yml", appId, System.currentTimeMillis());
	}

	private void storeModel(String fileName, String modelStr) throws IOException {
		Path path = Paths.get(translatorProperties.getModelsDir(), fileName);
		Files.writeString(path, modelStr);
		log.info("ExternalBrokerListenerService: Stored metric model in file: {}", path);
	}

	private void sendResponse(Publisher publisher, String appId, Object response) {
		publisher.send(Map.of(
				"response", response
		), appId);
	}
}