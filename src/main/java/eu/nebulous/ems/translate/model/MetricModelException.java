/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.translate.model;

public class MetricModelException extends RuntimeException {
    public MetricModelException(String message) {
        super(message);
    }

    public MetricModelException(Throwable th) {
        super(th);
    }

    public MetricModelException(String message, Throwable th) {
        super(message, th);
    }
}