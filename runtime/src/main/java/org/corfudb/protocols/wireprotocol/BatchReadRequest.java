package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BatchReadRequest implements ICorfuPayload<BatchReadRequest> {

	final Range<Long> range;
	final Integer step;

	public BatchReadRequest(ByteBuf buf) {
		range = ICorfuPayload.rangeFromBuffer(buf, Long.class);
		step = ICorfuPayload.fromBuffer(buf, Integer.class);
	}

	public BatchReadRequest(Long address) {
		range = Range.singleton(address);
		step = 1;
	}

	public BatchReadRequest(Long startAddress, Long endAddress, Integer step) {
		range = Range.closed(startAddress, endAddress);
		this.step = step;
	}

	@Override
	public void doSerialize(ByteBuf buf) {
		ICorfuPayload.serialize(buf, range);
		ICorfuPayload.serialize(buf, step);
	}

}
