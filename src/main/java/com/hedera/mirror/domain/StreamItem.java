package com.hedera.mirror.domain;

import lombok.Data;
import java.nio.ByteBuffer;

@Data
public class StreamItem {
    public enum Type { SIG, PAYLOAD }

    ByteBuffer dataBytes;  // read only byte buffer
    final Type dataType;
    final String fileName;
    final String nodeAccountId;
    final StreamType streamType;

    @Override
    public String toString() {
        return "StreamItem{" + dataType + ", name=" + fileName + ", nodeAccountId=" + nodeAccountId + '}';
    }
}
