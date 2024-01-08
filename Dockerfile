#
# Copyright (C) 2017-2023 Institute of Communication and Computer Systems (imu.iccs.gr)
#
# This Source Code Form is subject to the terms of the Mozilla Public License, v2.0, unless
# Esper library is used, in which case it is subject to the terms of General Public License v2.0.
# If a copy of the MPL was not distributed with this file, you can obtain one at
# https://www.mozilla.org/en-US/MPL/2.0/
#

FROM ems-server:2024-jan

# Add Nebulous EMS plugin to EMS core image
COPY target/ems-nebulous-1.0.0-SNAPSHOT-jar-with-dependencies.jar /plugins/

ENV EXTRA_LOADER_PATHS=/plugins/*
ENV SCAN_PACKAGES=eu.nebulous.ems
