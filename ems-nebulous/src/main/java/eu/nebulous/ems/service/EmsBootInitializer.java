/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class EmsBootInitializer implements ApplicationListener<ApplicationReadyEvent> {
	private final ExternalBrokerPublisherService publisherService;
	private final TaskScheduler scheduler;
	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		log.info("===================> EMS is ready -- Scheduling EMS Boot message ");
		scheduler.schedule(publisherService::sendEmsBootReadyEvent, Instant.now().plusSeconds(1));
	}
}