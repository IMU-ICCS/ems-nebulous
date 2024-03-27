/*
 * Copyright (C) 2017-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0, unless
 * Esper library is used, in which case it is subject to the terms of General Public License v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.k8s;

import gr.iccs.imu.ems.baguette.server.NodeRegistry;
import gr.iccs.imu.ems.baguette.server.NodeRegistryEntry;
import gr.iccs.imu.ems.common.k8s.K8sClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Kubernetes cluster pods watcher service
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class K8sPodWatcher implements InitializingBean {
    private final TaskScheduler taskScheduler;
    private final NodeRegistry nodeRegistry;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (Boolean.parseBoolean(K8sClient.getConfig("K8S_WATCHER_ENABLED", "true"))) {
            Instant initDelay = Instant.now().plusSeconds(20);
            Duration period = Duration.ofSeconds(10);
            taskScheduler.scheduleAtFixedRate(this::doWatch, initDelay, period);
            log.debug("K8sPodWatcher: Enabled  (running every {}sec, init-period={})", period, initDelay);
        } else {
            log.info("K8sPodWatcher: Disabled  (to enable set env. var. K8S_WATCHER_ENABLED=true)");
        }
    }

    private void doWatch() {
        Map<String, K8sClient.NodeEntry> addressToNodeMap = new HashMap<>();
        Map<String, Set<K8sClient.PodEntry>> addressToPodMap = new HashMap<>();
        try {
            log.debug("K8sPodWatcher: BEGIN: Retrieving active Kubernetes cluster nodes and pods");

            try (K8sClient client = K8sClient.create()) {
                // Get Kubernetes cluster nodes (Hosts)
                Map<String, K8sClient.NodeEntry> uidToNodeMap = new HashMap<>();
                client.getNodesInfo().forEach(node -> {
                    log.trace("K8sClient.getNodesInfo:  - node: {}", node);
                    K8sClient.NodeEntry entry = uidToNodeMap.computeIfAbsent(
                            node.getMetadata().getUid(), s -> new K8sClient.NodeEntry(node));
                    node.getStatus().getAddresses().stream()
                            .filter(address -> ! "Hostname".equalsIgnoreCase(address.getType()))
                            .forEach(address -> addressToNodeMap.putIfAbsent(address.getAddress(), entry));
                });
                log.debug("K8sPodWatcher: Address-to-Nodes: {}", addressToNodeMap);

                // Get Kubernetes cluster pods
                Map<String, K8sClient.PodEntry> uidToPodMap = new HashMap<>();
                client.getRunningPodsInfo().forEach(pod -> {
                    K8sClient.PodEntry entry = uidToPodMap.computeIfAbsent(
                            pod.getMetadata().getUid(), s -> new K8sClient.PodEntry(pod));
                    pod.getStatus().getPodIPs()
                            .forEach(address ->
                                    addressToPodMap.computeIfAbsent(address.getIp(), s -> new HashSet<>()).add(entry)
                            );
                });
                log.debug("K8sPodWatcher: Address-to-Pods: {}", addressToPodMap);

            } // End of try-with-resources

            // Update Node Registry
//            log.warn("K8sPodWatcher: Updating Node Registry");
//            Map<String, NodeRegistryEntry> addressToNodeEntryMap = nodeRegistry.getNodes().stream()
//                    .collect(Collectors.toMap(NodeRegistryEntry::getIpAddress, entry -> entry));
//
//            // New Pods
//            HashMap<String, Set<K8sClient.PodEntry>> newPods = new HashMap<>(addressToPodMap);
//            newPods.keySet().removeAll(addressToNodeEntryMap.keySet());
//            if (! newPods.isEmpty()) {
//                log.warn("K8sPodWatcher: New Pods found: {}", newPods);
//                newPods.forEach((address, podSet) -> {
//                    K8sClient.PodEntry pod = null;
//                    try {
//                        if (podSet.size() == 1) {
//                            pod = podSet.iterator().next();
//                            nodeRegistry.addNode(new HashMap<>(), pod.podUid());
//                        }
//                    } catch (UnknownHostException e) {
//                        log.warn("K8sPodWatcher: EXCEPTION: while adding new Pod in Node Registry: podSet: {}", pod);
//                    }
//                });
//            } else {
//                log.warn("K8sPodWatcher: No new Pods");
//            }
//
//            // Node Entries to be removed
//            HashMap<String, NodeRegistryEntry> oldEntries = new HashMap<>(addressToNodeEntryMap);
//            oldEntries.keySet().removeAll(addressToPodMap.keySet());
//            if (! oldEntries.isEmpty()) {
//                log.warn("K8sPodWatcher: Node entries to be removed: {}", oldEntries);
//            } else {
//                log.warn("K8sPodWatcher: No node entries to remove");
//            }

        } catch (Exception e) {
            log.warn("K8sPodWatcher: Error while running doWatch: ", e);
        }
    }
}
