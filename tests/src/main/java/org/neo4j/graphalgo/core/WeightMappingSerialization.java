package org.neo4j.graphalgo.core;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.neo4j.graphalgo.api.WeightMapping;

import java.io.IOException;

public final class WeightMappingSerialization {

    private static final long BOOL_BYTES = 1L;
    private static final long INT_BYTES = Integer.BYTES;
    private static final long LONG_BYTES = Long.BYTES;
    private static final long DOUBLE_DOUBLE = Double.BYTES;

    public static long bytes(WeightMap weights) {
        return INT_BYTES + BOOL_BYTES
                + ((weights.weights() != null)
                ? INT_BYTES + weights
                .weights()
                .size() * (LONG_BYTES + DOUBLE_DOUBLE)
                : 0L);
    }

    public static void write(WeightMap weightMapping, DataOutput out)
    throws IOException {
        int capacity = weightMapping.capacity();
        LongDoubleMap weights = weightMapping.weights();

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
    }

    public static WeightMapping read(DataInput in) throws IOException {
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
            return new WeightMap(capacity, weights, 0.0);
        }
        return new NullWeightMap(0.0); // TODO supply default value
    }

    private WeightMappingSerialization() {
        throw new UnsupportedOperationException("No instances");
    }
}
