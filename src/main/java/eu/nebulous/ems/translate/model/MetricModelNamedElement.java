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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class MetricModelNamedElement {
    @NonNull private final JsonNode element;

    public List<MetricModelNamedElement> getSLOs() {
        return getRequirements(MetricModel.SLO_SECTION);
    }

    public List<MetricModelNamedElement> getOptimisationGoals() {
        return getRequirements(MetricModel.OPT_GOALS_SECTION);
    }

    public List<MetricModelNamedElement> getRequirements(String section) {
        if (element.hasNonNull(MetricModel.REQUIREMENTS_SECTION) && element.hasNonNull(section)) {
            JsonNode node = element.get(MetricModel.REQUIREMENTS_SECTION).get(section);
            return IteratorUtils.toList(node.elements()).stream()
                    .map(MetricModelNamedElement::new)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public List<MetricModelNamedElement> getMetrics() {
        if (element.hasNonNull(MetricModel.METRICS_SECTION)) {
            JsonNode node = element.get(MetricModel.METRICS_SECTION);
            return IteratorUtils.toList(node.elements()).stream()
                    .map(MetricModelNamedElement::new)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }
}