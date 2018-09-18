package org.neo4j.graphalgo.core;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.IntPredicate;

public class DirectIdMap implements IdMap {
    private final int size;

    public DirectIdMap(int size) {
        this.size = size;
    }

    @Override
    public Collection<PrimitiveIntIterable> batchIterables(int batchSize) {
        if (batchSize <= 0) throw new IllegalArgumentException("Invalid batch size: "+batchSize);
        List<PrimitiveIntIterable> result = new ArrayList<>(size / batchSize + 1);
        for (int start = 0; start < size; start +=batchSize) {
            result.add(new IntIterable(start,Math.min(start+batchSize,size)));
        }
        return result;
    }

    @Override
    public int toMappedNodeId(long nodeId) {
        return Math.toIntExact(nodeId);
    }

    @Override
    public long toOriginalNodeId(int nodeId) {
        return nodeId;
    }

    @Override
    public boolean contains(long nodeId) {
        return nodeId >= 0 && nodeId < size;
    }

    @Override
    public long nodeCount() {
        return size;
    }

    @Override
    public void forEachNode(IntPredicate consumer) {
        for (int node=0;node < size; node++) {
            if (!consumer.test(node)) return;
        }
    }

    @Override
    public PrimitiveIntIterator nodeIterator() {
        return new IntIterable(0,size).iterator();
    }

    private static class IntIterable implements PrimitiveIntIterable {
        private final int start;
        private final int end;

        public IntIterable(int start,int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public PrimitiveIntIterator iterator() {
            return new PrimitiveIntIterator() {
                int curr = start;
                @Override
                public boolean hasNext() {
                    return curr < end;
                }

                @Override
                public int next() {
                    return curr++;
                }
            };
        }
    }
}
