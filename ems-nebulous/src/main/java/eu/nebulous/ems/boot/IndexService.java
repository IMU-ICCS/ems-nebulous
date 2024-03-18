/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.boot;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okio.Path;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class IndexService {
	private final EmsBootProperties properties;
	private final ObjectMapper objectMapper;
	private final Object LOCK = new Object();

	public void initIndexFile() throws IOException {
		try {
			// Load index file contents (if it exists)
			@NonNull Map<String, Object> indexContents = loadIndexContents();
			if (! indexContents.isEmpty()) {
				log.warn("Index file is not empty. Will not initialize it.");
				return;
			}
		} catch (IOException e) {
			log.debug("Initializing index file: ", e);
		}

		// Initialize index file
		storeIndexContents(Map.of());
		log.info("Index file initialized");
	}

	void storeToIndex(String appId, Map<String,String> values) throws IOException {
		log.debug("storeToIndex: BEGIN: app-id={}, values={}", appId, values);
		synchronized (LOCK) {
			// Load index file contents
			Map<String, Object> indexContents = loadIndexContents();

			// Create or update entry
			Map<String,String> entry = (Map<String, String>) indexContents.computeIfAbsent(appId, k -> new HashMap<>());
			values.forEach((key, val) -> {
				if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(val))
					entry.put(key, val);
			});

			// Store index contents back to file
			storeIndexContents(indexContents);
		}
	}

	Map<String,String> getFromIndex(String appId) throws IOException {
		log.debug("getFromIndex: BEGIN: app-id={}", appId);

		// Load info from models store
		Map<String, Object> indexContents = loadIndexContents();

		// Find the entry for 'appId'
		Object entry = indexContents.entrySet().stream()
				.filter(e -> e.getKey().equalsIgnoreCase(appId))
				.map(Map.Entry::getValue)
				.findFirst().orElse(null);
		log.debug("getFromIndex: END: app-id={}, result={}", appId, entry);

		return (Map) entry;
	}

	@NonNull
	private Map<String, Object> loadIndexContents() throws IOException {
		Map<String,Object> indexContents = null;
		if (Path.get(properties.getModelsIndexFile()).toFile().exists()) {
			try (FileReader reader = new FileReader(properties.getModelsIndexFile())) {
				indexContents = objectMapper.readValue(reader, Map.class);
			}
		}
		log.debug("Model Index contents loaded: {}", indexContents);
		if (indexContents==null)
			indexContents = new LinkedHashMap<>();
		return indexContents;
	}

	private void storeIndexContents(Map<String, Object> indexContents) throws IOException {
		log.debug("Model Index contents to write: {}", indexContents);
		if (indexContents==null) indexContents = Map.of();
		try (FileWriter writer = new FileWriter(properties.getModelsIndexFile())) {
			objectMapper.writeValue(writer, indexContents);
		}
	}
}