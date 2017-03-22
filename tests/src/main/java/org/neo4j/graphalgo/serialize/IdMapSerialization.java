package org.neo4j.graphalgo.serialize;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIntMap;
import org.neo4j.graphalgo.core.IdMap;

import java.io.IOException;

public final class IdMapSerialization {

    private static final long LONG_BYTES = Long.BYTES;
    private static final long INT_BYTES = Integer.BYTES;

    public static long bytes(IdMap idMap) {
        return INT_BYTES + ((long) idMap.size()) * LONG_BYTES;
    }

    public static void write(IdMap idMap, DataOutput out) throws IOException {
        int nextGraphId = idMap.size();
        long[] graphIds = idMap.mappedIds();

        out.writeVInt(nextGraphId);
        for (int i = 0; i < nextGraphId; i++) {
            out.writeVLong(graphIds[i]);
        }
    }

    public static IdMap read(DataInput in) throws IOException {
        final int nextGraphId = in.readVInt();
        final long[] graphIds = new long[nextGraphId];
        final PrimitiveLongIntMap map = Primitive.longIntMap(nextGraphId);
        for (int i = 0; i < nextGraphId; i++) {
            final long id = in.readVLong();
            graphIds[i] = id;
            map.put(id, i);
        }
        return new IdMap(graphIds, map);
    }

    private IdMapSerialization() {
        throw new UnsupportedOperationException("No instances");
    }
}
