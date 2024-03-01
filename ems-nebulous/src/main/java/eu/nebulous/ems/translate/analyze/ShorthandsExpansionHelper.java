/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.translate.analyze;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ParseContext;
import gr.iccs.imu.ems.translate.model.MetricTemplate;
import gr.iccs.imu.ems.translate.model.ValueType;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static eu.nebulous.ems.translate.analyze.AnalysisUtils.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShorthandsExpansionHelper {
    private final static Pattern METRIC_CONSTRAINT_PATTERN =
            Pattern.compile("^([^<>=!]+)([<>]=|=[<>]|<>|!=|[=><])(.+)$");
    private final static Pattern METRIC_WINDOW_PATTERN =
            Pattern.compile("^\\s*(\\w+)\\s+(\\d+(?:\\.\\d*)?|\\.\\d+)\\s*(?:(\\w+)\\s*)?");
    private final static Pattern METRIC_WINDOW_SIZE_PATTERN =
            Pattern.compile("^\\s*(\\d+(?:\\.\\d*)?|\\.\\d+)\\s*(?:(\\w+)\\s*)?");
    private final static Pattern METRIC_OUTPUT_PATTERN =
            Pattern.compile("^\\s*(\\w+)\\s+(\\d+(?:\\.\\d*)?|\\.\\d+)\\s*(\\w+)\\s*");
    private final static Pattern METRIC_OUTPUT_SCHEDULE_PATTERN =
            Pattern.compile("^\\s*(\\d+(?:\\.\\d*)?|\\.\\d+)\\s*(\\w+)\\s*");
    private final static Pattern METRIC_SENSOR_PATTERN =
            Pattern.compile("^\\s*(\\w+)\\s+(\\w+)\\s*");

    // ------------------------------------------------------------------------
    //  Methods for expanding shorthand expressions
    // ------------------------------------------------------------------------

    public void expandShorthandExpressions(Object metricModel, String modelName) throws Exception {
        log.debug("ShorthandsExpansionHelper: model-name: {}", modelName);

        // -- Initialize jsonpath context -------------------------------------
        Configuration jsonpathConfig = Configuration.defaultConfiguration();
        ParseContext parseContext = JsonPath.using(jsonpathConfig);
        DocumentContext ctx = parseContext.parse(metricModel);

        // ----- Expand SLO constraints -----
        List<Object> expandedConstraints = asList(ctx
                .read("$.spec.*.*.requirements.*[?(@.constraint)]", List.class)).stream()
                .filter(item -> JsonPath.read(item, "$.constraint") instanceof String)
                .peek(this::expandConstraint)
                .toList();
        log.debug("ShorthandsExpansionHelper: Constraints expanded: {}", expandedConstraints);

        // ----- Read Metric templates -----
        Map<Object, Map> templateSpecs = new LinkedHashMap<>();
        templateSpecs.putAll( readMetricTemplate("$.templates.*", ctx) );
        templateSpecs.putAll( readMetricTemplate("$.spec.templates.*", ctx) );
        log.debug("ShorthandsExpansionHelper: Metric Templates found: {}", templateSpecs);

        // ----- Expand Metric templates in Metric specifications -----
        List<Object> expandedTemplates = asList(ctx
                .read("$.spec.*.*.metrics.*[?(@.template)]", List.class)).stream()
                .filter(item -> JsonPath.read(item, "$.template") instanceof String)
                .peek(item -> expandTemplate(item, templateSpecs))
                .toList();
        log.debug("ShorthandsExpansionHelper: Templates expanded: {}", expandedTemplates);

        // ----- Expand Metric windows -----
        List<Object> expandedWindows = asList(ctx
                .read("$.spec.*.*.metrics.*[?(@.window)]", List.class)).stream()
                .filter(item -> JsonPath.read(item, "$.window") instanceof String)
                .peek(this::expandWindow)
                .toList();
        log.debug("ShorthandsExpansionHelper: Windows expanded: {}", expandedWindows);

        List<Object> expandedWindowSizes = asList(ctx
                .read("$.spec.*.*.metrics.*.window[?(@.size)]", List.class)).stream()
                .filter(item -> JsonPath.read(item, "$.size") instanceof String)
                .peek(this::expandWindowSize)
                .toList();
        log.debug("ShorthandsExpansionHelper: Windows sizes expanded: {}", expandedWindowSizes);

        // ----- Expand Metric outputs -----
        List<Object> expandedOutputs = asList(ctx
                .read("$.spec.*.*.metrics.*[?(@.output)]", List.class)).stream()
                .filter(item -> JsonPath.read(item, "$.output") instanceof String)
                .peek(this::expandOutput)
                .toList();
        log.debug("ShorthandsExpansionHelper: Outputs expanded: {}", expandedOutputs);

        List<Object> expandedOutputSchedules = asList(ctx
                .read("$.spec.*.*.metrics.*.output[?(@.schedule)]", List.class)).stream()
                .filter(item -> JsonPath.read(item, "$.schedule") instanceof String)
                .peek(this::expandOutputSchedule)
                .toList();
        log.debug("ShorthandsExpansionHelper: Output schedules expanded: {}", expandedOutputSchedules);

        // ----- Expand Metric sensors -----
        List<Object> expandedSensors = asList(ctx
                .read("$.spec.*.*.metrics.*[?(@.sensor)]", List.class)).stream()
                .filter(item -> JsonPath.read(item, "$.sensor") instanceof String)
                .peek(this::expandSensor)
                .toList();
        log.debug("ShorthandsExpansionHelper: Sensors expanded: {}", expandedSensors);
    }

    private static Map<String, Map> readMetricTemplate(@NonNull String path, @NonNull DocumentContext ctx) {
        try {
            return asList(ctx
                    .read(path, List.class)).stream()
                    .filter(x -> x instanceof Map)
                    .map(x -> (Map) x)
                    .filter(x -> x.get("id") != null)
                    .collect(Collectors.toMap(x -> x.get("id").toString(), x -> x));
        } catch (Exception e) {
            log.debug("ShorthandsExpansionHelper.readMetricTemplate: Not found metric templates in path: {}", path);
            return Collections.emptyMap();
        }
    }

    private void expandTemplate(Object spec, Map<Object, Map> templateSpecs) {
        log.debug("ShorthandsExpansionHelper.expandTemplate: {}", spec);
        String templateId = JsonPath.read(spec, "$.template").toString().trim();
        Object tplSpec = templateSpecs.get(templateId);
        if (tplSpec!=null) {
            asMap(spec).put("template", tplSpec);
        } else {
            List<String> parts = Arrays.asList(templateId.split("[ \t\r\n]+"));
            if (parts.size()>=4) {
                MetricTemplate newTemplate;
                if (StringUtils.equalsAnyIgnoreCase(parts.get(0), "double", "float")) {
                    asMap(spec).put("template", MetricTemplate.builder()
                            .valueType(ValueType.DOUBLE_TYPE)
                            .lowerBound("-inf".equalsIgnoreCase(parts.get(1))
                                    ? Double.NEGATIVE_INFINITY : Double.parseDouble(parts.get(1)))
                            .upperBound(StringUtils.equalsAnyIgnoreCase(parts.get(2), "inf", "+inf")
                                    ? Double.POSITIVE_INFINITY : Double.parseDouble(parts.get(2)))
                            .unit(parts.get(3))
                            .build());
                } else
                if (StringUtils.equalsAnyIgnoreCase(parts.get(0), "int", "integer")) {
                    asMap(spec).put("template", MetricTemplate.builder()
                            .valueType(ValueType.INT_TYPE)
                            .lowerBound("-inf".equalsIgnoreCase(parts.get(1))
                                    ? Integer.MIN_VALUE : Integer.parseInt(parts.get(1)))
                            .upperBound(StringUtils.equalsAnyIgnoreCase(parts.get(2), "inf", "+inf")
                                    ? Integer.MAX_VALUE : Integer.parseInt(parts.get(2)))
                            .unit(parts.get(3))
                            .build());
                } else
                    throw createException("Invalid Metric template shorthand expression: " + templateId);
            } else
                throw createException("Metric template id not found: " + templateId);
        }
    }

    private void expandWindow(Object spec) {
        log.debug("ShorthandsExpansionHelper.expandWindow: {}", spec);
        String constraintStr = JsonPath.read(spec, "$.window").toString().trim();
        Matcher matcher = METRIC_WINDOW_PATTERN.matcher(constraintStr);
        if (matcher.matches()) {
            asMap(spec).put("window", Map.of(
                    "type", matcher.group(1),
                    "size", (matcher.groupCount()>2)
                            ? Map.of("value", matcher.group(2), "unit", matcher.group(3))
                            : Map.of("value", matcher.group(2))
            ));
        } else
            throw createException("Invalid metric window shorthand expression: "+spec);
    }

    private void expandWindowSize(Object spec) {
        log.debug("ShorthandsExpansionHelper.expandWindowSize: {}", spec);
        String constraintStr = JsonPath.read(spec, "$.size").toString().trim();
        Matcher matcher = METRIC_WINDOW_SIZE_PATTERN.matcher(constraintStr);
        if (matcher.matches()) {
            asMap(spec).put("size", (matcher.groupCount()>1)
                            ? Map.of("value", matcher.group(1), "unit", matcher.group(2))
                            : Map.of("value", matcher.group(1))
            );
        } else
            throw createException("Invalid metric window shorthand expression: "+spec);
    }

    private void expandOutput(Object spec) {
        log.debug("ShorthandsExpansionHelper.expandOutput: {}", spec);
        String constraintStr = JsonPath.read(spec, "$.output").toString().trim();
        Matcher matcher = METRIC_OUTPUT_PATTERN.matcher(constraintStr);
        if (matcher.matches()) {
            asMap(spec).put("output", Map.of(
                    "type", matcher.group(1),
                    "schedule", Map.of(
                            "value", matcher.group(2),
                            "unit", matcher.group(3))
            ));
        } else
            throw createException("Invalid metric output shorthand expression: "+spec);
    }

    private void expandOutputSchedule(Object spec) {
        log.debug("ShorthandsExpansionHelper.expandOutputSchedule: {}", spec);
        String constraintStr = JsonPath.read(spec, "$.schedule").toString().trim();
        Matcher matcher = METRIC_OUTPUT_SCHEDULE_PATTERN.matcher(constraintStr);
        if (matcher.matches()) {
            asMap(spec).put("schedule", Map.of(
                            "value", matcher.group(1),
                            "unit", matcher.group(2))
            );
        } else
            throw createException("Invalid metric output shorthand expression: "+spec);
    }

    private void expandSensor(Object spec) {
        log.debug("ShorthandsExpansionHelper.expandSensor: {}", spec);
        String constraintStr = JsonPath.read(spec, "$.sensor").toString().trim();
        Matcher matcher = METRIC_SENSOR_PATTERN.matcher(constraintStr);
        if (matcher.matches()) {
            asMap(spec).put("sensor", Map.of(
                    "type", matcher.group(1),
                    "config", Map.of("mapping", matcher.group(2))
            ));
        } else
            throw createException("Invalid metric sensor shorthand expression: "+spec);
    }

    private void expandConstraint(Object spec) {
        log.debug("ShorthandsExpansionHelper.expandConstraint: {}", spec);
        String constraintStr = JsonPath.read(spec, "$.constraint").toString().trim();
        log.warn("ShorthandsExpansionHelper.expandConstraint: BEFORE removeOuterBrackets: {}", constraintStr);
        constraintStr = removeOuterBrackets(constraintStr);
        log.warn("ShorthandsExpansionHelper.expandConstraint:  AFTER removeOuterBrackets: {}", constraintStr);
        Matcher matcher = METRIC_CONSTRAINT_PATTERN.matcher(constraintStr);
        if (matcher.matches()) {
            String g1 = matcher.group(1);
            String g2 = matcher.group(2);
            String g3 = matcher.group(3);

            if (! isComparisonOperator(g2))
                throw createException("Invalid metric constraint shorthand expression in Requirement [Group 2 not a comparison operator]: "+spec);

            // Swap operands
            if (isDouble(g1)) {
                String tmp = g1;
                g1 = g3;
                g3 = tmp;
            }

            if (StringUtils.isBlank(g1) || StringUtils.isBlank(g3))
                throw createException("Invalid metric constraint shorthand expression in Requirement [Group 1 or 3 is blank]: "+spec);

            String metricName = g1.trim();
            double threshold = Double.parseDouble(g3.trim());

            Map<String, Object> constrMap = Map.of(
                    "type", "metric",
                    "metric", metricName,
                    "operator", g2.trim(),
                    "threshold", threshold
            );

            asMap(spec).put("constraint", constrMap);
        } else
            throw createException("Invalid metric constraint shorthand expression: "+spec);
    }

    private String removeOuterBrackets(String s) {
        if (s==null) return null;
        s = s.trim();
        if (s.isEmpty()) return s;
        while (s.startsWith("(") && s.endsWith(")")
            || s.startsWith("[") && s.endsWith("]")
            || s.startsWith("{") && s.endsWith("}"))
        {
            s = s.substring(1, s.length() - 1).trim();
            if (s.isEmpty()) return s;
        }
        return s;
    }
}