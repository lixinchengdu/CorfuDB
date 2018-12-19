package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.Range;

import io.netty.buffer.ByteBuf;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by mwei on 8/11/16.
 */
@Data
@AllArgsConstructor
public class ReadRequest implements ICorfuPayload<ReadRequest> {

    final Range<Long> range;
    final Integer step;

    public ReadRequest(ByteBuf buf) {
        range = ICorfuPayload.rangeFromBuffer(buf, Long.class);
        step = ICorfuPayload.fromBuffer(buf, Integer.class);
    }

    public ReadRequest(Long address) {
        range = Range.singleton(address);
        step = 1;
    }

    public ReadRequest(Range<Long> range) {
        this.range = range;
        this.step = 1;
    }

    public ReadRequest(Long startAddress, Long endAddress, Integer step) {
        range = Range.closed(startAddress, endAddress);
        this.step = step;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, range);
        ICorfuPayload.serialize(buf, step);
    }

}
