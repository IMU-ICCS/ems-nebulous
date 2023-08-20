/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.translate.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RequiredArgsConstructor
public class MetricModel {
    private static final String HEADER_API_VERSION = "apiVersion";
    private static final String HEADER_API_VERSION_VALUE = "apps/v1";
    private static final String HEADER_KIND = "kind";
    private static final String HEADER_KIND_VALUE = "MetricModel";
    private static final String METADATA_SECTION = "metadata";
    private static final String METADATA_MODEL_NAME = "name";
    private static final String SPEC_SECTION = "spec";
    private static final String COMPONENTS_SECTION = "components";
    private static final String SCOPES_SECTION = "scopes";

    private final File modelFile;
    private final JsonNode modelRoot;

    public String getMetricModelName() {
        if (modelRoot.hasNonNull(METADATA_SECTION) && modelRoot.hasNonNull(METADATA_MODEL_NAME)) {
            return modelRoot.get(METADATA_SECTION).get(METADATA_MODEL_NAME).asText();
        }
        return modelFile.getName();
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