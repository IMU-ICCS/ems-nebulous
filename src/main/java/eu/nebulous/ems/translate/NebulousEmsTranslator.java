/*
 * Copyright (C) 2017-2023 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0, unless
 * Esper library is used, in which case it is subject to the terms of General Public License v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.translate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import eu.nebulous.ems.translate.analyze.MetricModelAnalyzer;
import eu.nebulous.ems.translate.generate.RuleGenerator;
import eu.nebulous.ems.translate.transform.GraphTransformer;
import gr.iccs.imu.ems.translate.TranslationContext;
import gr.iccs.imu.ems.translate.Translator;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

@Slf4j
@Service
@RequiredArgsConstructor
public class NebulousEmsTranslator implements Translator, InitializingBean {

	private final ApplicationContext applicationContext;
	private final NebulousEmsTranslatorProperties properties;

	@Override
	public void afterPropertiesSet() throws Exception {
		log.info("NebulousEmsTranslator: initialized");
	}

	// ================================================================================================================
	// Public API

	@Override
	public TranslationContext translate(String metricModelPath) {
		if (StringUtils.isBlank(metricModelPath)) {
			log.error("NebulousEmsTranslator: No metric model specified");
			throw new NebulousEmsTranslationException("No metric model specified");
		}

		log.info("NebulousEmsTranslator: Parsing metric model file: {}", metricModelPath);
		JsonNode modelRoot;
		try {
			final File modelFile = Paths.get(metricModelPath).toFile();
			final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
			modelRoot = mapper.readTree(modelFile);
		} catch (IOException e) {
			throw new NebulousEmsTranslationException("Error while parsing metric model YAML file", e);
		}
		log.info("NebulousEmsTranslator: Metric model root: {}", modelRoot);

		log.info("NebulousEmsTranslator: Translating metric model: {}", metricModelPath);
		TranslationContext _TC = translate(modelRoot);
		log.info("NebulousEmsTranslator: Translating metric model completed: {}", metricModelPath);
		return _TC;
	}

	// ================================================================================================================
	// Private methods

	private TranslationContext translate(@NonNull JsonNode metricModel) {
		log.debug("NebulousEmsTranslator.translate():  BEGIN: metric-model={}", metricModel);
		String modelName = metricModel.get("modelName").asText();

		// initialize data structures
		TranslationContext _TC = new TranslationContext(modelName);

		// Analyze metric model
		log.info("NebulousEmsTranslator.translate():  Analyzing model...");
		MetricModelAnalyzer modelAnalyzer = applicationContext.getBean(MetricModelAnalyzer.class);
		modelAnalyzer.analyzeModel(_TC, metricModel);
		log.debug("NebulousEmsTranslator.translate():  Analyzing model... done");

		// transform graph
		log.info("NebulousEmsTranslator.translate():  Transforming DAG...");
		GraphTransformer transformer = applicationContext.getBean(GraphTransformer.class);
		transformer.transformGraph(_TC.getDAG());
		log.debug("NebulousEmsTranslator.translate():  Transforming DAG... done");

		// generate EPL rules
		log.info("NebulousEmsTranslator.translate():  Generating EPL rules...");
		RuleGenerator generator = applicationContext.getBean(RuleGenerator.class);
		generator.generateRules(_TC);
		log.debug("NebulousEmsTranslator.translate():  Generating EPL rules... done");

		log.debug("NebulousEmsTranslator.translate():  END: result={}", _TC);
		return _TC;
	}
}