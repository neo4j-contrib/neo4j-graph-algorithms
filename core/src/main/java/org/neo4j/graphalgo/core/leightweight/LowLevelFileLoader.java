package org.neo4j.graphalgo.core.leightweight;

import com.carrotsearch.hppc.LongDoubleHashMap;
import org.apache.lucene.store.DataInput;
import org.neo4j.graphalgo.api.WeightedGraph;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMapping;
import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIntMap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * @author mknobloch
 */
public class LowLevelFileLoader {

    public static WeightedGraph load(Path inFile) throws IOException {
        final File file = inFile.toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            final FileChannel channel = raf.getChannel();
            final MappedByteBuffer mbb = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    channel.size());
            final BBDI in = new BBDI(mbb);

            final long adjacencyLength = in.readVLong();
            final IntArray adjacency = IntArray.newArray(adjacencyLength);
            adjacency.fill(0, adjacencyLength, () -> {
                try {
                    return in.readVInt();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            final int inOffsetsLength = in.readVInt();
            final long[] inOffsets = new long[inOffsetsLength];
            for (int i = 0; i < inOffsetsLength; i++) {
                inOffsets[i] = in.readVLong();
            }

            final int outOffsetsLength = in.readVInt();
            final long[] outOffsets = new long[outOffsetsLength];
            for (int i = 0; i < outOffsetsLength; i++) {
                outOffsets[i] = in.readVLong();
            }


            final int nextGraphId = in.readVInt();
            final long[] graphIds = new long[nextGraphId];
            final PrimitiveLongIntMap map = Primitive.longIntMap(nextGraphId);
            for (int i = 0; i < nextGraphId; i++) {
                final long id = in.readVLong();
                graphIds[i] = id;
                map.put(id, i);
            }
            final IdMap idMap = new IdMap(graphIds, map);


            final WeightMapping weightMapping;
            final int capacity = in.readVInt();
            final boolean hasWeights = in.readByte() == (byte) 1;
            if (hasWeights) {
                final int weightsSize = in.readVInt();
                final LongDoubleHashMap weights = new LongDoubleHashMap(
                        weightsSize);
                for (int i = 0; i < weightsSize; i++) {
                    weights.put(
                            in.readVLong(),
                            Double.longBitsToDouble(in.readLong()));
                }
                weightMapping = new WeightMapping(capacity, weights);
            } else {
                weightMapping = new WeightMapping(capacity);
            }

            return new LightWeightGraph(
                    idMap, weightMapping, adjacency, inOffsets, outOffsets
            );
        }
    }

    private static final class BBDI extends DataInput {
        private final ByteBuffer bb;

        private BBDI(final ByteBuffer bb) {
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

}
