/*
 * Copyright (C) 2017-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0, unless
 * Esper library is used, in which case it is subject to the terms of General Public License v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.k8s;

import eu.nebulous.ems.EmsNebulousConstants;
import gr.iccs.imu.ems.baguette.server.ClientShellCommand;
import gr.iccs.imu.ems.baguette.server.NodeRegistry;
import gr.iccs.imu.ems.common.k8s.K8sClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Kubernetes cluster pods watcher service
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class K8sPodWatcher implements InitializingBean {
    private final K8sServiceProperties properties;
    private final TaskScheduler taskScheduler;
    private final NodeRegistry nodeRegistry;

    private final String EMS_SERVER_POD_UID = StringUtils.defaultIfBlank(
            System.getenv(EmsNebulousConstants.EMS_SERVER_POD_UID_ENV_VAR), "");

    @Override
    public void afterPropertiesSet() throws Exception {
        if (Boolean.parseBoolean(K8sClient.getConfig("K8S_WATCHER_ENABLED", "true"))) {
            Instant initDelay = Instant.now().plusSeconds(20);
            Duration period = Duration.ofSeconds(10);
            taskScheduler.scheduleAtFixedRate(this::doWatch, initDelay, period);
            log.info("K8sPodWatcher: Enabled  (running every {}sec, init-period={})", period, initDelay);
        } else {
            log.info("K8sPodWatcher: Disabled  (to enable set env. var. K8S_WATCHER_ENABLED=true)");
        }
    }

    private void doWatch() {
        try {
            // Get running pods and apply exclusions
            log.debug("K8sPodWatcher: BEGIN: Retrieving active Kubernetes cluster pods");

            HashMap<String, K8sClient.PodEntry> uuidToPodsMap = new HashMap<>();
            HashMap<String, Set<K8sClient.PodEntry>> podsPerHost = new HashMap<>();
            try (K8sClient client = K8sClient.create()) {
                client.getRunningPodsInfo().forEach(pod -> {
                    String ns = pod.getMetadata().getNamespace();
                    String appLabelValue = pod.getMetadata().getLabels().get(EmsNebulousConstants.APP_POD_LABEL);
                    log.trace("K8sPodWatcher: Got pod: uid={}, name={}, address={}, namespace={}, app-label={}",
                            pod.getMetadata().getUid(), pod.getMetadata().getName(), pod.getStatus().getPodIP(),
                            ns, appLabelValue);
                    if (properties.getIgnorePodsInNamespaces().contains(ns))
                        return;
                    if (StringUtils.isNotBlank(appLabelValue) && properties.getIgnorePodsWithAppLabel().contains(appLabelValue))
                        return;
                    K8sClient.PodEntry entry = new K8sClient.PodEntry(pod);
                    uuidToPodsMap.put(pod.getMetadata().getUid(), entry);
                    String podHostIp = pod.getStatus().getHostIP();
                    podsPerHost.computeIfAbsent(podHostIp, s -> new HashSet<>()).add(entry);
                    log.trace("K8sPodWatcher: Keeping pod: uid={}, name={}, address={}",
                            pod.getMetadata().getUid(), pod.getMetadata().getName(), pod.getStatus().getPodIP());
                });

            } // End of try-with-resources
            log.debug("K8sPodWatcher: Active Kubernetes cluster pods: uuidToPodsMap: {}", uuidToPodsMap);
            log.debug("K8sPodWatcher: Active Kubernetes cluster pods:   podsPerHost: {}", podsPerHost);

            // Group running pods per host IP
            log.debug("K8sPodWatcher: Processing active pods per active EMS client: ems-clients: {}", ClientShellCommand.getActive());
            Map<ClientShellCommand,List<String>> emsClientPodLists = new HashMap<>();
            ClientShellCommand.getActive().forEach(csc -> {
                //String id = csc.getId();
                String emsClientPodUuid = csc.getClientId();
                String address = csc.getClientIpAddress();
                log.trace("K8sPodWatcher: EMS client: pod-uid={}, address={}", emsClientPodUuid, address);

                K8sClient.PodEntry emsClientPod = uuidToPodsMap.get(emsClientPodUuid);
                log.trace("K8sPodWatcher: EMS client: pod-entry: {}", emsClientPod);
                String emsClientPodHostIp = emsClientPod.hostIP();
                Set<K8sClient.PodEntry> podsInHost = podsPerHost.get(emsClientPodHostIp);
                log.trace("K8sPodWatcher: EMS client: pod-host-address={}, pods-in-host: {}", emsClientPodHostIp, podsInHost);
                List<K8sClient.PodEntry> podsInHostWithoutEmsClient = podsInHost.stream()
                        .filter(pod -> ! pod.podUid().equalsIgnoreCase(EMS_SERVER_POD_UID))
                        .filter(pod -> ! pod.podUid().equalsIgnoreCase(emsClientPodUuid))
                        .toList();
                log.trace("K8sPodWatcher: EMS client: pod-host-address={}, Filtered-pods-in-host: {}", emsClientPodHostIp, podsInHostWithoutEmsClient);
                LinkedList<String> list = new LinkedList<>();
                podsInHostWithoutEmsClient.forEach(pod -> {
                    String podStr = String.format("uuid=%s, name=%s, address=%s, app=%s",
                            pod.podUid(), pod.podName(), pod.podIP(), pod.labels().get(EmsNebulousConstants.APP_POD_LABEL));
                    list.add(podStr);
                    emsClientPodLists.put(csc, list);
                    log.trace("K8sPodWatcher: EMS client: csc-id={}, host-ip={}, pod-str={}", csc.getClientId(), emsClientPodHostIp, list);
                });
            });
            log.warn("K8sPodWatcher: Active Kubernetes cluster pods per EMS client: {}", emsClientPodLists);

            // Update EMS client configurations

            log.debug("K8sPodWatcher: END");

        } catch (Exception e) {
            log.warn("K8sPodWatcher: ERROR while running doWatch: ", e);
        }
    }
}
