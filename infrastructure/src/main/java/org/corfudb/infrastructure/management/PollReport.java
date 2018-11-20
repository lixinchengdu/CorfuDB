package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;

import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.runtime.view.Layout;

/**
 * Poll Report generated by the detectors that poll to detect failed or healed nodes.
 * This is consumed and analyzed by the Management Server to detect changes in the cluster and
 * take appropriate action.
 * Created by zlokhandwala on 3/21/17.
 */
@Data
@Builder
public class PollReport {

    @Default
    private long pollEpoch = Layout.INVALID_EPOCH;
    @Default
    private final ImmutableSet<String> changedNodes = ImmutableSet.of();
    @Default
    private final Map<String, Long> outOfPhaseEpochNodes = new HashMap<>();
    @Default
    private final Map<String, ClusterState> clusterStateMap = new HashMap<>();

    public boolean isChangedNodesEmpty() {
        return changedNodes.isEmpty();
    }
}
