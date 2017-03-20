package org.neo4j.graphalgo.core;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIntMap;

import java.util.function.IntConsumer;

/**
 * This is basically a long to int mapper. It sorts the id's in ascending order so its
 * guaranteed that there is no ID greater then nextGraphId / capacity
 */
public final class IdMap {

    private final IdIterator iter;
    private int nextGraphId;
    private long[] graphIds;
    private PrimitiveLongIntMap nodeToGraphIds;

    /**
     * initialize the map with maximum node capacity
     */
    public IdMap(final int capacity) {
        graphIds = new long[capacity];
        nodeToGraphIds = Primitive.longIntMap(capacity);
        iter = new IdIterator();
    }

    /**
     * CTor used by deserializing logic
     */
    public IdMap(
            long[] graphIds,
            PrimitiveLongIntMap nodeToGraphIds) {
        this.nextGraphId = graphIds.length;
        this.graphIds = graphIds;
        this.nodeToGraphIds = nodeToGraphIds;
        iter = new IdIterator();
    }

    public PrimitiveIntIterator iterator() {
        return iter.reset(nextGraphId);
    }

    public int mapOrGet(long longValue) {
        int intValue = nodeToGraphIds.get(longValue);
        if (intValue == -1) {
            intValue = nextGraphId++;
            graphIds[intValue] = longValue;
            nodeToGraphIds.put(longValue, intValue);
        }
        return intValue;
    }

    public void add(long longValue) {
        int intValue = nextGraphId++;
        graphIds[intValue] = longValue;
        nodeToGraphIds.put(longValue, intValue);
    }

    public void addMap(IdMap other, int offset, int length) {
        System.arraycopy(other.graphIds, 0, graphIds, offset, length);
        nextGraphId += length;
        other.nodeToGraphIds.visitEntries((key, value) -> {
            nodeToGraphIds.put(key, value + offset);
            return false;
        });
    }

    public int get(long longValue) {
        return nodeToGraphIds.get(longValue);
    }

    public long unmap(int intValue) {
        return graphIds[intValue];
    }

    public int size() {
        return nextGraphId;
    }

    public long[] mappedIds() {
        return graphIds;
    }

    public void forEach(IntConsumer consumer) {
        int limit = this.nextGraphId;
        for (int i = 0; i < limit; i++) {
            consumer.accept(i);
        }
    }

    private static final class IdIterator implements PrimitiveIntIterator {

        private int current;
        private int limit;

        private PrimitiveIntIterator reset(int limit) {
            current = 0;
            this.limit = limit;
            return this;
        }

        @Override
        public boolean hasNext() {
            return current < limit;
        }

        @Override
        public int next() {
            return current++;
        }
    }
}
