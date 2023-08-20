/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.translate.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class MetricModel {
    static final String HEADER_API_VERSION = "apiVersion";
    static final String HEADER_API_VERSION_VALUE = "apps/v1";
    static final String HEADER_KIND = "kind";
    static final String HEADER_KIND_VALUE = "MetricModel";
    static final String METADATA_SECTION = "metadata";
    static final String METADATA_MODEL_NAME = "name";
    static final String SPEC_SECTION = "spec";
    static final String COMPONENTS_SECTION = "components";
    static final String SCOPES_SECTION = "scopes";

    static final String REQUIREMENTS_SECTION = "requirements";
    static final String SLO_SECTION = "slos";
    static final String OPT_GOALS_SECTION = "optimisation-goals";
    static final String METRICS_SECTION = "metrics";

    private final File modelFile;
    private final JsonNode modelRoot;

    public String getMetricModelName() {
        if (modelRoot.hasNonNull(METADATA_SECTION) && modelRoot.hasNonNull(METADATA_MODEL_NAME)) {
            return modelRoot.get(METADATA_SECTION).get(METADATA_MODEL_NAME).asText();
        }
        return modelFile.getName();
    }

    public List<MetricModelNamedElement> getComponents() {
        return getNamedList(COMPONENTS_SECTION);
    }

    public List<MetricModelNamedElement> getScopes() {
        return getNamedList(SCOPES_SECTION);
    }

    private List<MetricModelNamedElement> getNamedList(@NonNull String section) {
        if (modelRoot.hasNonNull(SPEC_SECTION) && modelRoot.get(SPEC_SECTION).hasNonNull(section)) {
            return IteratorUtils.toList(modelRoot.get(SPEC_SECTION).get(section).elements())
                    .stream().map(MetricModelNamedElement::new)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public void checkMetricModel() {
        checkModelHeaders();
        checkSpec();
    }

    private void checkModelHeaders() {
        if (! modelRoot.hasNonNull(HEADER_KIND) || ! HEADER_KIND_VALUE.equals(modelRoot.get(HEADER_KIND).asText())) {
            throw new MetricModelException("Missing or invalid Kind header in metric model: "+getMetricModelName());
        }
        if (! modelRoot.hasNonNull(HEADER_API_VERSION) || ! HEADER_API_VERSION_VALUE.equals(modelRoot.get(HEADER_API_VERSION).asText())) {
            throw new MetricModelException("Missing or invalid API version header in metric model: "+getMetricModelName());
        }
    }

    private void checkSpec() {
        if (! modelRoot.hasNonNull(SPEC_SECTION)) {
            throw new MetricModelException("Missing 'spec' section in metric model: "+getMetricModelName());
        }
        JsonNode specNode = modelRoot.get(SPEC_SECTION);
        if (! specNode.hasNonNull(COMPONENTS_SECTION) && ! specNode.hasNonNull(SCOPES_SECTION) ) {
            throw new MetricModelException("Missing or null 'components' and 'scopes' sections in metric model: "+getMetricModelName());
        }

        checkComponentsSection();
        checkScopesSection();
    }

    private void checkComponentsSection() {
        JsonNode componentsNode = modelRoot.get(SPEC_SECTION).get(COMPONENTS_SECTION);
        checkNamedList(COMPONENTS_SECTION, componentsNode);
    }

    private void checkScopesSection() {
        JsonNode scopesNode = modelRoot.get(SPEC_SECTION).get(SCOPES_SECTION);
        checkNamedList(SCOPES_SECTION, scopesNode);
    }

    private void checkNamedList(String sectionName, JsonNode node) {
        if (!node.isArray())
            throw new MetricModelException(sectionName+" section is not a list in metric model: "+getMetricModelName());

        StringBuilder errors = new StringBuilder();
        final AtomicInteger cnt = new AtomicInteger(0);
        node.elements().forEachRemaining(childNode -> {
            // Check 'name' property of list entry (component or scope)
            if (childNode==null)
                errors.append("\nNull entry at list position ").append(cnt.get());
            else if (!childNode.hasNonNull("name"))
                errors.append("\nEntry without 'name' at list position ").append(cnt.get());
            else if (!childNode.get("name").isTextual())
                errors.append("\nName property is not textual, at entry at list position ").append(cnt.get());
            else {
                String name = childNode.get("name").asText();
                if (StringUtils.isBlank(name))
                    errors.append("\nName property with blank value, at entry at list position ").append(cnt.get());
            }

            // Check entry
            checkEntry(sectionName, childNode, errors);

            cnt.incrementAndGet();
        });
        if (!errors.isEmpty())
            throw new MetricModelException(sectionName+" section contains errors im metric model: "+getMetricModelName()+errors);
    }

    private void checkEntry(String sectionName, JsonNode node, StringBuilder errors) {

    }
}