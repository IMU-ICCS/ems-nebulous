/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmsNebulousConstants {
    public final static String APPLICATION_UID_ENV_VAR = "APPLICATION_ID";
    public static final String EMS_SERVER_POD_UID_ENV_VAR = "EMS_SERVER_POD_UID";
    public static final String EMS_SERVER_POD_NAMESPACE_ENV_VAR = "EMS_SERVER_POD_NAMESPACE";
    public static final String APP_POD_LABEL = "app";
}