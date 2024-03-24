/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.boot;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.nebulouscloud.exn.core.Publisher;
import gr.iccs.imu.ems.common.k8s.K8sClient;
import gr.iccs.imu.ems.util.PasswordUtil;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class BootService implements InitializingBean {
	private final EmsBootProperties properties;
	private final IndexService indexService;
	private final ObjectMapper objectMapper;

	@Override
	public void afterPropertiesSet() throws Exception {
		// Collect external broker connection info
		collectExternalBrokerConnectionInfo();
	}

	private void collectExternalBrokerConnectionInfo() {
		log.debug("BootService: collectExternalBrokerConnectionInfo: BEGIN: Mode: {}",
				properties.getExternalBrokerInfoCollectionMode());
		if (properties.getExternalBrokerInfoCollectionMode()==EmsBootProperties.MODE.K8S) {
			try (K8sClient client = K8sClient.create()) {
				@NonNull String serviceName = properties.getExternalBrokerServiceName();
				String serviceNamespace = properties.getExternalBrokerNamespace();
				log.debug("BootService: collectExternalBrokerConnectionInfo: service={}, namespace={}", serviceName, serviceNamespace);

				Map<String, Object> connectionInfo = client.getServiceConnectionInfo(serviceName, serviceNamespace);
				log.debug("BootService: collectExternalBrokerConnectionInfo: connectionInfo: {}", connectionInfo);

				properties.setExternalBrokerAddress(
						((List<String>) connectionInfo.get("external-addresses")).get(0) );
				properties.setExternalBrokerPort( (int) connectionInfo.get("node-port") );
			} catch (IOException e) {
				log.warn("BootService: EXCEPTION while querying Kubernetes API server: ", e);
				throw new RuntimeException(e);
			}
		}
		log.debug("BootService: collectExternalBrokerConnectionInfo: END: " +
						"External broker: address={}, port={}, username={}, password={}, service={}, namespace={}",
				properties.getExternalBrokerAddress(),
				properties.getExternalBrokerPort(),
				properties.getExternalBrokerUsername(),
				PasswordUtil.getInstance().encodePassword(properties.getExternalBrokerPassword()),
				properties.getExternalBrokerServiceName(),
				properties.getExternalBrokerNamespace() );
	}

	void processEmsBootMessage(Command command, String appId, Publisher emsBootResponsePublisher) throws IOException {
		// Process EMS Boot message
		log.debug("BootService: Received a new EMS Boot message from external broker: {}", command.body());

		// Load info from models store
		Map<String, String> entry = indexService.getFromIndex(appId);
		log.debug("BootService: Index entry for app-id: {},  entry: {}", appId, entry);
		if (entry==null) {
			log.warn("No EMS Boot entry found for app-id: {}", appId);
			return;
		}
		String modelFile = entry.get(ModelsService.MODEL_FILE_KEY);
		String bindingsFile = entry.get(ModelsService.BINDINGS_FILE_KEY);
		log.info("""
                BootService: Received EMS Boot request:
                         App-Id: {}
                     Model File: {}
                  Bindings File: {}
                """, appId, modelFile, bindingsFile);

		String modelStr = Files.readString(Paths.get(properties.getModelsDir(), modelFile));
		log.debug("BootService: Model file contents:\n{}", modelStr);
		String bindingsStr = Files.readString(Paths.get(properties.getModelsDir(), bindingsFile));
		Map bindingsMap = objectMapper.readValue(bindingsStr, Map.class);
		log.debug("BootService: Bindings file contents:\n{}", bindingsMap);

		// Send EMS Boot response message
		Map<Object, Object> message = Map.of(
				"application", appId,
				"metric-model", modelStr,
				"bindings", bindingsMap,
				"external-broker", Map.of(
						"address", properties.getExternalBrokerAddress(),
						"port", properties.getExternalBrokerPort(),
						"username", properties.getExternalBrokerUsername(),
						"password", properties.getExternalBrokerPassword()
				)
		);
		emsBootResponsePublisher.send(message, appId);
		log.info("BootService: EMS Boot response sent");
	}
}