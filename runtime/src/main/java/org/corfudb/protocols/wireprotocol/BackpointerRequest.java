package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

@Data
@AllArgsConstructor
public class BackpointerRequest implements ICorfuPayload<BackpointerRequest> {

	/** The streams which are written to by this token request. */
	final UUID stream;
	final Long startGlobalAddress;
	final Long endGlobalAddress;


	public BackpointerRequest(ByteBuf buf) {
		stream = ICorfuPayload.fromBuffer(buf, UUID.class);
		startGlobalAddress = ICorfuPayload.fromBuffer(buf, Long.class);
		endGlobalAddress = ICorfuPayload.fromBuffer(buf, Long.class);
	}

	@Override
	public void doSerialize(ByteBuf buf) {
		ICorfuPayload.serialize(buf, stream);
		ICorfuPayload.serialize(buf, startGlobalAddress);
		ICorfuPayload.serialize(buf, endGlobalAddress);
	}
}