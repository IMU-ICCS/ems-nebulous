/*
 * Copyright (C) 2017-2023 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0, unless
 * Esper library is used, in which case it is subject to the terms of General Public License v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.translate.generate;

import eu.nebulous.ems.translate.NebulousEmsTranslatorProperties;
import gr.iccs.imu.ems.translate.TranslationContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class RuleGenerator implements InitializingBean {
    private final NebulousEmsTranslatorProperties properties;

    @Override
    public void afterPropertiesSet() {
    }

    // ================================================================================================================
    // Public API

    public void generateRules(TranslationContext _TC) {
//        log.debug("RuleGenerator.ruleTemplates:\n{}", ruleTemplatesRegistry.getRuleTemplates());
//        _generateRules(_TC);
//        _TC.getTopicConnections();    // force topicConnections population
    }

    // ================================================================================================================
    // Rule generation methods

}