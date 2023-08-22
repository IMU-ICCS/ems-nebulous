/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.translate.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class MetricModelNamedElement {
    public enum METRIC_TYPE { COMPOSITE_METRIC, RAW_METRIC, METRIC_VARIABLE }

    @Getter
    @NonNull private final JsonNode element;

    public String getName() {
        if (element.hasNonNull(MetricModel.ENTRY_NAME) && element.get(MetricModel.ENTRY_NAME).isTextual()) {
            return element.get(MetricModel.ENTRY_NAME).asText();
        }
        throw new MetricModelException("Element does not have a non-null 'name': "+element);
    }

    public MetricModelNamedElement getChildNode(@NonNull String childNodeName) {
        return new MetricModelNamedElement( element.get(childNodeName) );
    }

    // ------------------------------------------------------------------------

    public List<MetricModelNamedElement> getArrayAsList(@NonNull String fieldName) {
        if (element.hasNonNull(fieldName) && element.get(fieldName).isArray()) {
            JsonNode arr = element.withArray(fieldName);
            return IteratorUtils.toList(arr.elements()).stream().map(MetricModelNamedElement::new).toList();
        }
        throw new MetricModelException("Field '"+fieldName+"' does not exist or is null or is not an array, in element: "+element);
    }

    public List<String> getArrayAsStringList(@NonNull String fieldName) {
        if (element.hasNonNull(fieldName) && element.get(fieldName).isArray()) {
            JsonNode arr = element.withArray(fieldName);
            return IteratorUtils.toList(arr.elements()).stream().map(JsonNode::asText).toList();
        }
        throw new MetricModelException("Field '"+fieldName+"' does not exist or is null or is not an array, in element: "+element);
    }

    public Map<String, MetricModelNamedElement> getObjectAsMap(@NonNull String fieldName) {
        if (element.hasNonNull(fieldName) && element.get(fieldName).isObject()) {
            JsonNode obj = element.withObject("/"+fieldName);
            return IteratorUtils.toList(obj.fields()).stream().collect(Collectors.toMap(
                    Map.Entry::getKey, x->new MetricModelNamedElement(x.getValue())
            ));
        }
        throw new MetricModelException("Field '"+fieldName+"' does not exist or is null or is not an object, in element: "+element);
    }

    public Map<String, String> getObjectAsStringMap(@NonNull String fieldName) {
        if (element.hasNonNull(fieldName) && element.get(fieldName).isObject()) {
            JsonNode obj = element.withObject("/"+fieldName);
            return IteratorUtils.toList(obj.fields()).stream().collect(Collectors.toMap(
                    Map.Entry::getKey, x->x.getValue().asText()
            ));
        }
        throw new MetricModelException("Field '"+fieldName+"' does not exist or is null or is not an object, in element: "+element);
    }

    // ------------------------------------------------------------------------

    public List<MetricModelNamedElement> getSLOs() {
        log.trace("    MetricModelNamedElement.getSLOs(): BEGIN");
        return getRequirements(MetricModel.SLO_SECTION);
    }

    public List<MetricModelNamedElement> getOptimisationGoals() {
        log.trace("    MetricModelNamedElement.getOptimisationGoals(): BEGIN");
        return getRequirements(MetricModel.OPT_GOALS_SECTION);
    }

    private List<MetricModelNamedElement> getRequirements() {
        log.trace("    MetricModelNamedElement.getRequirements(): BEGIN");
        LinkedList<MetricModelNamedElement> list = new LinkedList<>();
        list.addAll(getSLOs());
        list.addAll(getOptimisationGoals());
        log.trace("    MetricModelNamedElement.getRequirements(): END: {}", list);
        return list;
    }

    private List<MetricModelNamedElement> getRequirements(String section) {
        log.trace("    MetricModelNamedElement.getRequirements(): BEGIN: section={}", section);
        if (element.hasNonNull(MetricModel.REQUIREMENTS_SECTION) && element.get(MetricModel.REQUIREMENTS_SECTION).hasNonNull(section)) {
            JsonNode node = element.get(MetricModel.REQUIREMENTS_SECTION).get(section);
            log.trace("    MetricModelNamedElement.getRequirements(): END: node={}", node);
            return IteratorUtils.toList(node.elements()).stream()
                    .map(MetricModelNamedElement::new)
                    .collect(Collectors.toList());
        }
        log.trace("    MetricModelNamedElement.getRequirements(): END: node=[]");
        return Collections.emptyList();
    }

    // ------------------------------------------------------------------------

    public List<MetricModelNamedElement> getMetrics() {
        if (element.hasNonNull(MetricModel.METRICS_SECTION)) {
            JsonNode node = element.get(MetricModel.METRICS_SECTION);
            return IteratorUtils.toList(node.elements()).stream()
                    .map(MetricModelNamedElement::new)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public List<MetricModelNamedElement> getFunctions() {
        if (element.hasNonNull(MetricModel.FUNCTIONS_SECTION)) {
            JsonNode node = element.get(MetricModel.FUNCTIONS_SECTION);
            return IteratorUtils.toList(node.elements()).stream()
                    .map(MetricModelNamedElement::new)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    // ------------------------------------------------------------------------

    public METRIC_TYPE getMetricType() {
        // Get metric type from 'type' field, if available
        String type = getFieldValue(MetricModel.METRIC_TYPE, null);
        if (StringUtils.isNotBlank(type))
            return METRIC_TYPE.valueOf(type);

        // Infer metric type from other fields
        if (getFieldValue(MetricModel.METRIC_FORMULA)!=null)
            return METRIC_TYPE.COMPOSITE_METRIC;
        if (getFieldValue(MetricModel.METRIC_SENSOR)!=null)
            return METRIC_TYPE.RAW_METRIC;
        return METRIC_TYPE.METRIC_VARIABLE;
    }

    public boolean isRaw() {
        if (getMetricType()==METRIC_TYPE.RAW_METRIC) return true;
        return getMetricType() == METRIC_TYPE.METRIC_VARIABLE && getFieldValue(MetricModel.METRIC_FORMULA) == null;
    }

    public boolean isComposite() {
        return ! isRaw();
    }

    public boolean isMetric() {
        return getMetricType() != METRIC_TYPE.METRIC_VARIABLE;
    }

    public boolean isMetricVariable() {
        return getMetricType() == METRIC_TYPE.METRIC_VARIABLE;
    }

    public boolean isCurrentConfig() {
        return Boolean.parseBoolean( getFieldValue(MetricModel.METRIC_CURRENT_CONFIG, "false") );
    }

    public String getFormula() {
        return getFieldValue(MetricModel.METRIC_FORMULA, null);
    }

    // ------------------------------------------------------------------------

    public String getFieldValue(String fieldName, String defaultValue) {
        String result = getFieldValue(fieldName);
        return result!=null ? result : defaultValue;
    }

    public String getFieldValue(String fieldName) {
        if (log.isTraceEnabled()) {
            boolean a, b = false;
            log.trace("    getFieldValue( {} ): has-non-null={}, is-text={}, text={}", fieldName,
                    a = element.hasNonNull(fieldName),
                    a ? b = element.get(fieldName).isTextual() : "n/a",
                    (a && b) ? element.get(fieldName).asText() : "n/a");
        }
        if (element.hasNonNull(fieldName) && element.get(fieldName).isTextual())
            return element.get(fieldName).asText();
        return null;
    }

    public String getType() {
        if (element.hasNonNull(MetricModel.METRIC_TEMPLATE) && element.get(MetricModel.METRIC_TEMPLATE).isObject()) {
            JsonNode template = element.get((MetricModel.METRIC_TEMPLATE));
            String type = template.get(MetricModel.METRIC_TEMPLATE_TYPE).asText();
            String range = template.get(MetricModel.METRIC_TEMPLATE_RANGE).asText();
            String values = String.join(", ", (CharSequence) IteratorUtils.toList(template.get(MetricModel.METRIC_TEMPLATE_VALUES).elements()).stream().map(x->x.get(MetricModel.ENTRY_NAME).asText()));
            if (StringUtils.isNotBlank(type)) {
                if (StringUtils.isNotBlank(range))
                    return type+"["+range+"]";
                if (StringUtils.isNotBlank(values))
                    return type+"["+values+"]";
                return type;
            }
        }
        return Double.class.getSimpleName();
    }
}