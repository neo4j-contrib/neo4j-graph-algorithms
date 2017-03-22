package org.neo4j.graphalgo.serialize;

import org.apache.lucene.store.DataInput;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class ByteBufferDataInput extends DataInput {
    private final ByteBuffer bb;

    public ByteBufferDataInput(final ByteBuffer bb) {
        this.bb = bb;
    }

    @Override
    public byte readByte() throws IOException {
        return bb.get();
    }

    @Override
    public void readBytes(
            final byte[] b,
            final int offset,
            final int len) throws IOException {
        bb.get(b, offset, len);
    }

    @Override
    public int readInt() throws IOException {
        return bb.getInt();
    }

    @Override
    public long readLong() throws IOException {
        return bb.getLong();
    }
}
