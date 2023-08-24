/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.translate.model;

import com.fasterxml.jackson.databind.JsonNode;
import gr.iccs.imu.ems.translate.model.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Getter
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class MetricModelNamedElement {
    public enum METRIC_TYPE { composite, raw, variable }
    public enum CONSTRAINT_TYPE { metric, logical, conditional }

    //protected final static List<String> CONDITIONAL_OPERATORS = List.of("IF", "THEN", "ELSE");
    protected final static List<String> LOGICAL_OPERATORS = List.of("AND", "OR", "XOR", "NOT");
    protected final static List<String> COMPARISON_OPERATORS = List.of(">", ">=", "<", "<=", "<>", "!=", "=");

    @NonNull
    private final JsonNode element;
    private final String _section;
    @ToString.Exclude
    private final MetricModelNamedElement _parent;
    @NonNull @ToString.Exclude @Getter(AccessLevel.NONE)
    private final MetricModelNamedElement _this = this;

    MetricModelNamedElement(JsonNode modelRoot) {
        element = modelRoot;
        _section = null;
        _parent = null;
    }

    public String getName() {
        if (element.hasNonNull(MetricModel.ENTRY_NAME) && element.get(MetricModel.ENTRY_NAME).isTextual()) {
            return element.get(MetricModel.ENTRY_NAME).asText();
        }
        throw new MetricModelException("Element does not have a non-null 'name': "+element);
    }

    public MetricModelNamedElement getChildNode(@NonNull String childNodeName) {
        return new MetricModelNamedElement( element.get(childNodeName), _section, _this );
    }

    public String getSection() {
        return _section;
    }

    public MetricModelNamedElement getParent() {
        return _parent;
    }

    // ------------------------------------------------------------------------

    public List<MetricModelNamedElement> getArrayAsList(@NonNull String fieldName) {
        if (element.hasNonNull(fieldName) && element.get(fieldName).isArray()) {
            JsonNode arr = element.withArray(fieldName);
            return IteratorUtils.toList(arr.elements()).stream().map(e -> new MetricModelNamedElement(e, _section, _this)).toList();
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
                    Map.Entry::getKey, x->new MetricModelNamedElement(x.getValue(), _section, _this)
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
        log.trace("      MetricModelNamedElement.getSLOs(): BEGIN");
        return getRequirements(MetricModel.SLO_SECTION);
    }

    public List<MetricModelNamedElement> getOptimisationGoals() {
        log.trace("      MetricModelNamedElement.getOptimisationGoals(): BEGIN");
        return getRequirements(MetricModel.OPT_GOALS_SECTION);
    }

    public List<MetricModelNamedElement> getConstraints() {
        log.trace("      MetricModelNamedElement.getConstraints(): BEGIN");
        return getRequirements(MetricModel.CONSTRAINTS_SECTION);
    }

    private List<MetricModelNamedElement> getRequirements() {
        log.trace("      MetricModelNamedElement.getRequirements(): BEGIN");
        LinkedList<MetricModelNamedElement> list = new LinkedList<>();
        list.addAll(getSLOs());
        list.addAll(getOptimisationGoals());
        log.trace("      MetricModelNamedElement.getRequirements(): END: {}", list);
        return list;
    }

    private List<MetricModelNamedElement> getRequirements(String section) {
        log.trace("      MetricModelNamedElement.getRequirements(): BEGIN: section={}", section);
        if (element.hasNonNull(MetricModel.REQUIREMENTS_SECTION) && element.get(MetricModel.REQUIREMENTS_SECTION).hasNonNull(section)) {
            JsonNode node = element.get(MetricModel.REQUIREMENTS_SECTION).get(section);
            log.trace("      MetricModelNamedElement.getRequirements(): END: node={}", node);
            return IteratorUtils.toList(node.elements()).stream()
                    .map(e -> new MetricModelNamedElement(e, section, _this))
                    .collect(Collectors.toList());
        }
        log.trace("      MetricModelNamedElement.getRequirements(): END: node=[]");
        return Collections.emptyList();
    }

    // ------------------------------------------------------------------------

    public List<MetricModelNamedElement> getMetrics() {
        if (element.hasNonNull(MetricModel.METRICS_SECTION)) {
            JsonNode node = element.get(MetricModel.METRICS_SECTION);
            return IteratorUtils.toList(node.elements()).stream()
                    .map(e -> new MetricModelNamedElement(e, _section, _this))
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public List<MetricModelNamedElement> getFunctions() {
        if (element.hasNonNull(MetricModel.FUNCTIONS_SECTION)) {
            JsonNode node = element.get(MetricModel.FUNCTIONS_SECTION);
            return IteratorUtils.toList(node.elements()).stream()
                    .map(e -> new MetricModelNamedElement(e, _section, _this))
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
            return METRIC_TYPE.composite;
        if (getFieldValue(MetricModel.METRIC_SENSOR)!=null)
            return METRIC_TYPE.raw;
        return METRIC_TYPE.variable;
    }

    public boolean isRaw() {
        if (getMetricType()==METRIC_TYPE.raw) return true;
        return getMetricType() == METRIC_TYPE.variable && getFieldValue(MetricModel.METRIC_FORMULA) == null;
    }

    public boolean isComposite() {
        return ! isRaw();
    }

    public boolean isMetric() {
        return getMetricType() != METRIC_TYPE.variable;
    }

    public boolean isMetricVariable() {
        return getMetricType() == METRIC_TYPE.variable;
    }

    public boolean isCurrentConfig() {
        return Boolean.parseBoolean( getFieldValue(MetricModel.METRIC_CURRENT_CONFIG, "false") );
    }

    public String getFormula() {
        return getFieldValue(MetricModel.METRIC_FORMULA, null);
    }

    // ------------------------------------------------------------------------

    public boolean isConstraint() {
        return MetricModel.CONSTRAINTS_SECTION.equals(_section);
    }

    public CONSTRAINT_TYPE getConstraintType() {
        if (!isConstraint())
            throw new MetricModelException("Call of getConstraintType() on a non-constraint element: "+this);
        CONSTRAINT_TYPE result = _getConstraintType();
        log.trace("      MetricModelNamedElement.getConstraintType(): END: name={}, result={}", getName(), result);
        return result;
    }

    private CONSTRAINT_TYPE _getConstraintType() {
        // Get metric type from 'type' field, if available
        String type = getFieldValue(MetricModel.CONSTRAINT_TYPE, null);
        log.trace("      MetricModelNamedElement.getConstraintType(): BEGIN: name={}, type={}", getName(), type);
        if (StringUtils.isNotBlank(type)) {
            log.trace("      MetricModelNamedElement.getConstraintType(): END: name={}, type={}", getName(), CONSTRAINT_TYPE.valueOf(type));
            return CONSTRAINT_TYPE.valueOf(type);
        }

        // Infer constraint type from constraint expression or variable
        String expression = getFieldValue(MetricModel.CONSTRAINT_EXPRESSION, "").trim();
        log.trace("      MetricModelNamedElement.getConstraintType():      name={}, expression={}", getName(), expression);

        if (StringUtils.isBlank(expression)) {
            throw new MetricModelException("Constraint with blank expression: " + element);
        }

        // Tokenize expression
        List<String> tokens = splitExpression(expression).stream().map(String::toUpperCase).toList();
        log.trace("      MetricModelNamedElement.getConstraintType():      name={}, tokens={}", getName(), tokens);
        log.trace("      MetricModelNamedElement.getConstraintType():      name={}, IF-check={}", getName(), "IF".equals(tokens.get(0)));
        log.trace("      MetricModelNamedElement.getConstraintType():      name={}, Logical-check={}", getName(), CollectionUtils.intersection(tokens, LOGICAL_OPERATORS));
        log.trace("      MetricModelNamedElement.getConstraintType():      name={}, Metric-check={}", getName(), CollectionUtils.intersection(tokens, COMPARISON_OPERATORS));
        if ("IF".equals( tokens.get(0) ))
            return CONSTRAINT_TYPE.conditional;
        if (! CollectionUtils.intersection(tokens, LOGICAL_OPERATORS).isEmpty())
            return CONSTRAINT_TYPE.logical;
        if (! CollectionUtils.intersection(tokens, COMPARISON_OPERATORS).isEmpty())
            return CONSTRAINT_TYPE.metric;

        log.trace("      MetricModelNamedElement.getConstraintType(): ERROR: Constraint type not found. Will throw an exception: name={}", getName());
        throw new MetricModelException("Constraint with invalid expression: "+element);
    }

    private List<String> splitExpression(String expr) {
        if (StringUtils.isBlank(expr))
            return Collections.emptyList();

        List<String> tokens = new LinkedList<>();
        int last_match = 0;
        Pattern pat = Pattern.compile("([^A-Za-z0-9_]+)");
        Matcher mat = pat.matcher(expr);

        while (mat.find()) {
            tokens.add(expr.substring(0, mat.start()));
            tokens.add(mat.group());
            last_match = mat.end();
        }
        tokens.add(expr.substring(last_match));
        return tokens.stream().map(String::trim).toList();
    }

    public String getConstraintExpression() {
        return getFieldValue(MetricModel.CONSTRAINT_EXPRESSION, null);
    }

    // ------------------------------------------------------------------------

    public String getFieldValue(String fieldName, String defaultValue) {
        String result = getFieldValue(fieldName);
        return result!=null ? result : defaultValue;
    }

    public String getFieldValue(String fieldName) {
        boolean nonNull = element.hasNonNull(fieldName);
        String text = nonNull ? element.get(fieldName).asText() : null;
        if (log.isTraceEnabled()) {
            String jsonType = nonNull ? element.get(fieldName).getNodeType().name() : null;
            log.trace("        getFieldValue( {} ): has-non-null={}, field-json-type={}, text={}", fieldName, nonNull, jsonType, text);
        }
        return text;
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

    public Metric toMetric() {
        METRIC_TYPE type;
        Metric m = switch (type = getMetricType()) {
            case composite -> CompositeMetric.builder().build();
            case raw -> RawMetric.builder().build();
            case variable -> MetricVariable.builder().build();
            default -> throw new MetricModelException("Unknown metric type: "+type);
        };
        m.setName(getName());
        m.setObject(element);
        return m;
    }

    public MetricVariable toMetricVariable() {
        Metric m = toMetric();
        return (MetricVariable) m;
    }
}