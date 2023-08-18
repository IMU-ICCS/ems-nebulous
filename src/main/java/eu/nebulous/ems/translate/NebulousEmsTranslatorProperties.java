/*
 * Copyright (C) 2017-2023 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0, unless
 * Esper library is used, in which case it is subject to the terms of General Public License v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.translate;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import gr.iccs.imu.ems.util.EmsConstant;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Data
@Validated
@Configuration
@ConfigurationProperties(prefix = EmsConstant.EMS_PROPERTIES_PREFIX + "translator")
public class NebulousEmsTranslatorProperties implements InitializingBean {
    @Override
    public void afterPropertiesSet() {
        // Convert string in 'translatorPropertiesJson' into a Map<String,String> and put it into 'translatorProperties'
        log.trace("NebulousYamlToEplTranslatorProperties: translatorPropertiesJson: string: {}", translatorPropertiesJson);
        if (StringUtils.isNotBlank(translatorPropertiesJson)) {
            Map<String, String> map = new Gson().fromJson(translatorPropertiesJson, new TypeToken<Map<String, String>>(){}.getType());
            log.trace("NebulousYamlToEplTranslatorProperties: translatorPropertiesJson: map: {}", map);
            if (map!=null && map.size()>0) {
                log.debug("NebulousYamlToEplTranslatorProperties: translatorProperties: Before adding translatorPropertiesJson map: {}", translatorProperties);
                if (translatorProperties != null)
                    translatorProperties.putAll(map);
                else
                    translatorProperties = map;
                log.trace("NebulousYamlToEplTranslatorProperties: translatorProperties: After adding translatorPropertiesJson map: {}", translatorProperties);
            }
        }

        log.debug("NebulousYamlToEplTranslatorProperties: {}", this);
    }

    private Map<String, String> translatorProperties = new LinkedHashMap<>();
    private String translatorPropertiesJson;

    // Translator parameters
    private String sensorConfigurationAnnotation = "MELODICMetadataSchema.ContextAwareSecurityModel.SecurityContextElement.Object.DataArtefact.Configuration.ConfigurationFormat.JSON_FORMAT";
    private long sensorMinInterval;
    private long sensorDefaultInterval;

    private String leafNodeGrouping;
    private boolean pruneMvv = true;
    private boolean addTopLevelMetrics = true;
    private String fullNamePattern;
    private boolean formulaCheckEnabled = true;

    // Load-annotated metric settings
    private String loadMetricAnnotation = "MELODICMetadataSchema.UtilityNotions.UtilityRelatedProperties.Utility.BusyInstanceMetric";
    private String loadMetricVariableFormatter = "busy.%s";

    // Active sink types
    private List<String> sinks;

    // Sink type configurations
    private final Map<String,Map<String,String>> sinkConfig = new HashMap<>();

    public Map<String,Map<String,String>> getSinkConfig() {
        return sinkConfig;
    }
}
