/*
 * Copyright (C) 2017-2023 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0, unless
 * Esper library is used, in which case it is subject to the terms of General Public License v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.translate;

import gr.iccs.imu.ems.translate.TranslationContext;
import gr.iccs.imu.ems.translate.Translator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class NebulousEmsTranslator implements Translator, InitializingBean {

	private final ApplicationContext applicationContext;
	private final NebulousEmsTranslatorProperties properties;

	@Override
	public void afterPropertiesSet() throws Exception {
		log.info("NebulousYamlToEplTranslator: initialized");
	}

	// ================================================================================================================
	// Public API

	@Override
	public TranslationContext translate(String metricModelPath) {
		if (StringUtils.isBlank(metricModelPath)) {
			log.error("NebulousYamlToEplTranslator: No metric model specified");
			throw new NebulousEmsTranslationException("No metric model specified");
		}
		log.info("NebulousYamlToEplTranslator: Translating metric model: {}", metricModelPath);
		TranslationContext _TC = doTranslate(metricModelPath);
		log.info("NebulousYamlToEplTranslator: Translating metric model completed: {}", metricModelPath);
		return _TC;
	}

	// ================================================================================================================
	// Private methods

	private TranslationContext doTranslate(String metricModelPath) {
		return null;
	}
}