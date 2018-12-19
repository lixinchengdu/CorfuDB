package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class BackpointerResponse implements ICorfuPayload<BackpointerResponse> {

	final List<Long> backpointers;

	/**
	 * Deserialization Constructor from a Bytebuf to TokenResponse.
	 *
	 * @param buf The buffer to deserialize
	 */
	public BackpointerResponse(ByteBuf buf) {
		backpointers = ICorfuPayload.listFromBuffer(buf, Long.class);
	}

	@Override
	public void doSerialize(ByteBuf buf) {
		ICorfuPayload.serialize(buf, backpointers);
	}

}
