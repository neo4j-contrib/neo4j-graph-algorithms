package org.neo4j.graphalgo.serialize;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class ByteBufferDataOutput extends org.apache.lucene.store.DataOutput {
    private final ByteBuffer bb;
    private long written;

    public ByteBufferDataOutput(final ByteBuffer bb) {
        this.bb = bb;
    }

    @Override
    public void writeByte(final byte b) throws IOException {
        bb.put(b);
        written += 1L;
    }

    @Override
    public void writeBytes(
            final byte[] b,
            final int offset,
            final int length)
    throws IOException {
        bb.put(b, offset, length);
        written += length;
    }

    @Override
    public void writeInt(final int i) throws IOException {
        bb.putInt(i);
        written += Integer.BYTES;
    }

    @Override
    public void writeLong(final long i) throws IOException {
        bb.putLong(i);
        written += Long.BYTES;
    }

    public long written() {
        return written;
    }
}
