/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.translate.analyze;

import gr.iccs.imu.ems.translate.TranslationContext;
import gr.iccs.imu.ems.translate.dag.DAGNode;
import gr.iccs.imu.ems.translate.model.*;
import eu.nebulous.ems.translate.NebulousEmsTranslatorProperties;
import gr.iccs.imu.ems.util.StrUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static eu.nebulous.ems.translate.analyze.AnalysisUtils.*;

// ------------------------------------------------------------------------
//  Sensor processing methods
// ------------------------------------------------------------------------

@Slf4j
@Service
@RequiredArgsConstructor
class SensorsHelper extends AbstractHelper {
    private static final String DEFAULT_SENSOR_NAME_SUFFIX = "_SENSOR";
    private static final String DEFAULT_SENSOR_TYPE = "netdata";

    private final NebulousEmsTranslatorProperties properties;

    Sensor processSensor(TranslationContext _TC, Map<String, Object> sensorSpec, NamesKey parentNamesKey, NamedElement parent) {
        // Get needed fields
        String sensorName = getSpecName(sensorSpec);
        String sensorType = getSpecField(sensorSpec, "type");
        if (StringUtils.isBlank(sensorName)) sensorName = parentNamesKey.child + DEFAULT_SENSOR_NAME_SUFFIX;
        if (StringUtils.isBlank(sensorType)) sensorType = DEFAULT_SENSOR_TYPE;

        NamesKey sensorNamesKey = createNamesKey(parentNamesKey, sensorName);

        // Get 'push' or 'pull' type
        boolean isPush = getBooleanValue(getSpecField(sensorSpec, "push"), false);
        boolean isPull = getBooleanValue(getSpecField(sensorSpec, "pull"), false);
        if (isPush && isPull)
            throw createException("Sensor cannot be both 'push' and 'pull': sensor '" + sensorName + "' in metric '" + parentNamesKey + "': " + sensorSpec);
        if (!isPush && !isPull)
            isPull = true;

        // Get configuration
        LinkedHashMap<String, Object> configuration = new LinkedHashMap<>();

        Object mapping = sensorSpec.get("mapping");
        Object configObj = sensorSpec.get("config");
        Object installObj = sensorSpec.get("install");
        if (configObj!=null)
            configuration.putAll(asMap(configObj));
        if (mapping instanceof String s && StringUtils.isNotBlank(s))
            configuration.put("mapping", s.trim());
        if (installObj instanceof Map i && !i.isEmpty())
            configuration.put("install", i);

        // overrides 'type' in 'config' (if any)
        configuration.put("type", sensorType);

        // Create pull or push sensor
        Sensor sensor;
        if (isPull) {
            sensor = createPullSensor(sensorSpec, sensorName, sensorNamesKey, configuration);
        } else {
            sensor = createPushSensor(sensorSpec, sensorName, sensorNamesKey, configuration);
        }
        sensor.setConfigurationStr( configObj instanceof String s ? s : null );
        sensor.setConfiguration( configuration );

        // Update TC
        DAGNode sensorNode = _TC.getDAG().addNode(parent, sensor);

        // Add component-sensor pair
        Map<String, Object> metricSpec = asMap($$(_TC).allMetrics.get(parentNamesKey));
        String componentName = getContainerName(metricSpec);
        ObjectContext objCtx = $$(_TC).objectContexts.get(componentName);
        _TC.addComponentSensorPair(objCtx, sensor);

        // Add sensor monitor(s)
        _TC.addMonitorsForSensor(sensorNamesKey.name(), _createMonitorsForSensor(_TC, objCtx, sensor, sensorNode));

        return sensor;
    }

    private Sensor createPullSensor(Map<String, Object> sensorSpec, String sensorName, NamesKey sensorNamesKey, LinkedHashMap<String, Object> configuration) {
        // Convert Map<String,Object> to Map<String,String>
        Map<String, String> cfgMapWithStr = configuration.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));

        // Get pull sensor configuration
        String className = StrUtil.getWithVariations(cfgMapWithStr, "className", "").trim();
        String intervalPeriodStr = StrUtil.getWithVariations(cfgMapWithStr, "intervalPeriod", "").trim();
        String intervalUnitStr = StrUtil.getWithVariations(cfgMapWithStr, "intervalUnit", "").trim();

        // Get interval
        int period = StrUtil.strToInt(intervalPeriodStr,
                (int)properties.getSensorDefaultInterval(), (i)->i>=properties.getSensorMinInterval(), false,
                String.format("    createPullSensor(): Invalid interval period in configuration: sensor=%s, configuration=%s\n",
                        sensorName, cfgMapWithStr));
        Interval.UnitType periodUnit = StrUtil.strToEnum(intervalUnitStr,
                Interval.UnitType.class, Interval.UnitType.SECONDS, false,
                String.format("    createPullSensor(): Invalid interval unit in configuration: sensor=%s, configuration=%s\n",
                        sensorName, cfgMapWithStr));
        Interval interval = Interval.builder()
                .period(period)
                .unit(periodUnit)
                .build();

        // Create pull sensor
        return PullSensor.builder()
                .name(sensorNamesKey.name())
                .object(sensorSpec)
                .isPush(false)
                .className(className)
                .interval(interval)
                .build();
    }

    private static Sensor createPushSensor(Map<String, Object> sensorSpec, String sensorName, NamesKey sensorNamesKey, LinkedHashMap<String, Object> configuration) {
        // Get push sensor port
        Object portObj = configuration.get("port");
        String portStr = portObj!=null ? portObj.toString().trim() : "";
        if (StringUtils.isBlank(portStr)) portStr = null;
        int port = StrUtil.strToInt(portStr, -1, (i)->i>0 && i<=65535, false,
                String.format("    createPushSensor(): ERROR: Invalid port. Using -1: sensor=%s, port=%s\n", sensorName, portStr));

        // Create push sensor
        return PushSensor.builder()
                .name(sensorNamesKey.name())
                .object(sensorSpec)
                .isPush(true)
                .port(port)
                .build();
    }

    private Set<Monitor> _createMonitorsForSensor(TranslationContext _TC, ObjectContext objectContext, Sensor sensor, DAGNode sensorNode) {
        log.debug("    _createMonitorsForSensor(): sensor={}", sensor.getName());

        // Check if sensor monitors have already been created
        if (_TC.containsMonitorsForSensor(sensor.getName())) {
            log.debug("    _createMonitorsForSensor(): sensor={} :: Monitors for this sensor have already been added", sensor.getName());
            return Collections.emptySet();
        }

        // Get monitor component
        String componentName = (objectContext!=null && objectContext.getComponent()!=null)
                ? objectContext.getComponent().getName() : null;

        // Create results set
        Set<Monitor> results = new HashSet<>();
        for (DAGNode parent : _TC.getDAG().getParentNodes(sensorNode)) {
            // Get metric name from sensor
            log.debug("    + _createMonitorsForSensor(): sensor={} :: parent-node={}", sensor.getName(), parent.getName());
            Map<String, Object> rawMetricSpec = asMap(parent.getElement().getObject());
            String metricName = getSpecName(rawMetricSpec);
            log.debug("    + _createMonitorsForSensor(): sensor={} :: metric={}, component={}", sensor.getName(), metricName, componentName);

            // Create a Monitor instance
            Monitor monitor = Monitor.builder()
                    .metric(metricName)
                    .sensor(sensor)
                    .component(componentName)
                    .build();
            // watermark will be set in Coordinator

            results.add(monitor);
        }
        log.debug("    _createMonitorsForSensor(): sensor={} :: monitors={}", sensor.getName(), results);

        return results;
    }
}