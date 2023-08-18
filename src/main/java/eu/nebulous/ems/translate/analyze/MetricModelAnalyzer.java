/*
 * Copyright (C) 2017-2023 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0, unless
 * Esper library is used, in which case it is subject to the terms of General Public License v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.translate.analyze;

import com.fasterxml.jackson.databind.JsonNode;
import eu.nebulous.ems.translate.NebulousEmsTranslatorProperties;
import gr.iccs.imu.ems.translate.TranslationContext;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class MetricModelAnalyzer {
    private final NebulousEmsTranslatorProperties properties;

    // ================================================================================================================
    // Model analysis methods

    public void analyzeModel(TranslationContext _TC, @NonNull JsonNode metricModel) {
        String modelName = metricModel.get("modelName").asText();
        log.debug("MetricModelAnalyzer.analyzeModel():  Analyzing metric model: {}", modelName);

        log.debug("MetricModelAnalyzer.analyzeModel(): Analyzing Camel model completed: {}", modelName);
    }
}