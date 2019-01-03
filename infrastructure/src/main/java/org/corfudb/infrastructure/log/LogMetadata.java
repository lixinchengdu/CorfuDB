package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.view.Address;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A container object that holds log tail offsets and the global
 * log tail that has been seen. Note that holes don't belong to any
 * stream therefore the globalTail needs to be tracked separately.
 *
 * <p>Created by maithem on 10/15/18.
 */

@NotThreadSafe
@ToString
@Slf4j
public class LogMetadata {

    @Getter
    private volatile long globalTail;

    @Getter
    private final Map<UUID, Long> streamTails;

    public LogMetadata() {
        this.globalTail = Address.NON_ADDRESS;
        this.streamTails = new HashMap();
    }

    public void update(List<LogData> entries) {
        for (LogData entry : entries) {
            update(entry);
        }
    }

    public void update(LogData entry) {
        long entryAddress = entry.getGlobalAddress();
        updateGlobalTail(entryAddress);
        for (UUID streamId : entry.getStreams()) {
            long currentStreamTail = streamTails.getOrDefault(streamId, Address.NON_ADDRESS);
            streamTails.put(streamId, Math.max(currentStreamTail, entryAddress));
        }

        // We should also consider checkpoint metadata while updating the tails.
        // This is important because there could be streams that have checkpoint
        // data on the checkpoint stream, but not entries on the regular stream.
        // If those streams are not updated, then clients would observe those
        // streams as empty, which is not correct.
        if (entry.hasCheckpointMetadata()) {
            UUID streamId = entry.getCheckpointedStreamId();
            long streamTailAtCP = entry.getCheckpointedStreamStartLogAddress();

            if (Address.isAddress(streamTailAtCP)) {
                // TODO(Maithem) This is needed to filter out checkpoints of empty streams,
                // if the map has an entry (streamId, Address.Non_ADDRESS), then
                // when the sequencer services queries on that stream it will
                // "think" that the tail is not empty and return Address.Non_ADDRESS
                // instead of NON_EXIST. The sequencer, should handle both cases,
                // but that can be addressed in another issue.
                long currentStreamTail = streamTails.getOrDefault(streamId, Address.NON_ADDRESS);
                streamTails.put(streamId, Math.max(currentStreamTail, streamTailAtCP));
            }
        }
    }

    public void updateGlobalTail(long newTail) {
        globalTail = Math.max(globalTail, newTail);
    }

}
