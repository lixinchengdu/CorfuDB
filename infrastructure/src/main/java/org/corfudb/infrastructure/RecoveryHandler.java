package org.corfudb.infrastructure;

import java.time.Duration;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.Sleep;

@Slf4j
public class RecoveryHandler {

    private static Duration RECOVERY_RETRY_INTERVAL = Duration.ofSeconds(1);

    private RecoveryHandler() {
        // Hide implicit public constructor.
    }

    /**
     * This is called when the management server detects an existing layout in the local datastore
     * on startup. This requires a recovery from the same layout and attempt to rejoin the cluster.
     * Recovery is carried out as follows:
     * - Attempt to run reconfiguration on the cluster from the recovered layout found in the
     * local data store by incrementing the epoch.
     * The reconfiguration succeeds if the attempt to reach consensus by re-proposing this
     * recovery layout with its epoch incremented succeeds.
     * - If reconfiguration succeeds, the node is added to the layout and recovery was successful.
     * - If reconfiguration fails, the cluster has moved ahead.
     * * - This node now cannot force its inclusion into the cluster (it has a stale layout).
     * * - This node if marked unresponsive will be detected and unmarked by its peers in cluster.
     * - If multiple nodes are trying to recover, they will retry until they have recovered with
     * the latest layout previously accepted by the majority.
     * eg. Consider 3 nodes [Node(Epoch)]:
     * A(1), B(2), C(2). All 3 nodes crash and attempt to recover at the same time.
     * Node A should not be able to recover as it will detect a higher epoch in the rest of the
     * cluster. Hence either node B or C will succeed in recovering the cluster to epoch 3 with
     * their persisted layout.
     *
     * @return True if recovery was successful. False otherwise.
     */
    private static boolean runRecoveryReconfiguration(@NonNull Layout layout, CorfuRuntime corfuRuntime) {
        Layout localRecoveryLayout = new Layout(layout);
        boolean recoveryReconfigurationResult = corfuRuntime.getLayoutManagementView().attemptClusterRecovery(layout);
        log.info("Recovery reconfiguration attempt result: {}", recoveryReconfigurationResult);

        corfuRuntime.invalidateLayout();
        Layout clusterLayout = corfuRuntime.getLayoutView().getLayout();

        log.info("Recovery layout epoch:{}, Cluster epoch: {}",
                localRecoveryLayout.getEpoch(), clusterLayout.getEpoch());
        // The cluster has moved ahead. This node should not force any layout. Let the other
        // members detect that this node has healed and include it in the layout.
        return clusterLayout.getEpoch() > localRecoveryLayout.getEpoch()
                || recoveryReconfigurationResult;
    }

    static void retryUntilRecovery(@NonNull ServerContext serverContext,
                                   @NonNull CorfuRuntime corfuRuntime) {
        long recoveryAttempts = 0;
        // Recover if flag is false
        while (true) {
            Layout layout = serverContext.copyManagementLayout();
            if (runRecoveryReconfiguration(layout, corfuRuntime)) {
                // If recovery succeeds, reconfiguration was successful.
                // Save the latest management layout.
                serverContext.saveManagementLayout(corfuRuntime.getLayoutView().getLayout());

                log.info("Recovery completed");
                return;
            }

            log.error("detectorTaskScheduler: Recovery failed {} times. Retrying in {}s.",
                    ++recoveryAttempts, RECOVERY_RETRY_INTERVAL);
            Sleep.sleepUninterruptibly(RECOVERY_RETRY_INTERVAL);
        }
    }
}
