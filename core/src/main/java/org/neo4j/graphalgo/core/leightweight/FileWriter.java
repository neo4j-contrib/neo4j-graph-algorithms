package org.neo4j.graphalgo.core.leightweight;

import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * @author phorn@avantgarde-labs.de
 */
final class FileWriter {

    private static final long BYTES_BOOL = 1L;
    private static final long BYTES_INT = Integer.BYTES;
    private static final long BYTES_LONG = Long.BYTES;
    private static final long BYTES_DOUBLE = Double.BYTES;

    static void write(
            Path outPath,
            IntArray adjacency,
            long[] inOffsets,
            long[] outOffsets,
            int nextGraphId,
            long[] graphIds,
            int capacity,
            LongDoubleMap weights) throws IOException {

        final long adjSize = adjacency.size();
        final long requiredBytes = BYTES_LONG
                + adjSize * BYTES_INT
                + Integer.BYTES
                + inOffsets.length * BYTES_LONG
                + Integer.BYTES
                + outOffsets.length * BYTES_LONG
                + Integer.BYTES
                + nextGraphId * BYTES_LONG
                + BYTES_INT + BYTES_BOOL
                + ((weights != null)
                ? BYTES_INT + weights.size() * (BYTES_LONG + BYTES_DOUBLE)
                : 0L);

        final File file = outPath.toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            final FileChannel channel = raf.getChannel();
            channel.truncate(0);
            final MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_WRITE, 0, requiredBytes);
            final BBDO out = new BBDO(mbb);

            out.writeVLong(adjSize);
            for (IntArray.Cursor cursor : adjacency) {
                cursor.forEach(i -> {
                    out.writeVInt(i);
                    return true;
                });
            }

            out.writeVInt(inOffsets.length);
            for (long i : inOffsets) {
                out.writeVLong(i);
            }
            out.writeVInt(outOffsets.length);
            for (long i : outOffsets) {
                out.writeVLong(i);
            }

            out.writeVInt(nextGraphId);
            for (int i = 0; i < nextGraphId; i++) {
                out.writeVLong(graphIds[i]);
            }

            out.writeVInt(capacity);
            if (weights == null) {
                out.writeByte((byte) 0);
            } else {
                out.writeByte((byte) 1);
                out.writeVInt(weights.size());
                for (final LongDoubleCursor wc : weights) {
                    out.writeVLong(wc.key);
                    out.writeLong(Double.doubleToLongBits(wc.value));
                }
            }
            channel.truncate(out.written);
        }
    }

    private static final class BBDO extends org.apache.lucene.store.DataOutput {
        private final ByteBuffer bb;
        private long written;

        private BBDO(final ByteBuffer bb) {
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
    }
}
