package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;
import org.apache.lucene.util.ArrayUtil;
import org.neo4j.graphalgo.core.utils.IdCombiner;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.IntPredicate;

/**
 * Relation Container built of multiple arrays. The node capacity must be constant and the node IDs have to be
 * smaller then the capacity. The number of relations per node is limited only to the maximum array size of the VM
 * and connections can be added dynamically.
 *
 * @author mknblch
 */
class AdjacencyMatrix {

    private static final int[] EMPTY_INTS = new int[0];

    /**
     * mapping from nodeId to outgoing degree
     */
    final int[] outOffsets;
    /**
     * mapping from nodeId to incoming degree
     */
    final int[] inOffsets;
    /**
     * matrix nodeId x [outgoing edge-relationIds..]
     */
    final int[][] outgoing;
    /**
     * matrix nodeId x [incoming edge-relationIds..]
     */
    final int[][] incoming;

    AdjacencyMatrix(int nodeCount) {
        this.outOffsets = new int[nodeCount];
        this.inOffsets = new int[nodeCount];
        this.outgoing = new int[nodeCount][];
        this.incoming = new int[nodeCount][];
        Arrays.fill(outgoing, EMPTY_INTS);
        Arrays.fill(incoming, EMPTY_INTS);
    }

    AdjacencyMatrix(
            final int[] outOffsets,
            final int[] inOffsets,
            final int[][] outgoing,
            final int[][] incoming) {
        this.outOffsets = outOffsets;
        this.inOffsets = inOffsets;
        this.outgoing = outgoing;
        this.incoming = incoming;
    }

    /**
     * initialize array for outgoing connections
     */
    public void armOut(int sourceNodeId, int degree) {
        outgoing[sourceNodeId] = Arrays.copyOf(outgoing[sourceNodeId], degree);
    }

    /**
     * initialize array for incoming connections
     */
    public void armIn(int targetNodeId, int degree) {
        incoming[targetNodeId] = Arrays.copyOf(incoming[targetNodeId], degree);
    }

    /**
     * grow array for outgoing connections
     */
    public void growOut(int sourceNodeId, int length) {
        outgoing[sourceNodeId] = ArrayUtil.grow(outgoing[sourceNodeId], length);
    }

    /**
     * grow array for incoming connections
     */
    public void growIn(int targetNodeId, int length) {
        incoming[targetNodeId] = ArrayUtil.grow(incoming[targetNodeId], length);
    }

    /**
     * add outgoing relation
     */
    public void addOutgoing(int sourceNodeId, int targetNodeId) {
        final int degree = outOffsets[sourceNodeId];
        final int nextDegree = degree + 1;
        if (outgoing[sourceNodeId].length < nextDegree) {
            growOut(sourceNodeId, nextDegree);
        }
        outgoing[sourceNodeId][degree] = targetNodeId;
        outOffsets[sourceNodeId] = nextDegree;
    }

    /**
     * checks for outgoing target node, currently O(n)
     */
    public boolean hasOutgoing(int sourceNodeId, int targetNodeId) {
        final int degree = outOffsets[sourceNodeId];
        int[] rels = outgoing[sourceNodeId];
        for (int offset = degree-1; offset >= 0; offset--) {
            if (rels[offset]==targetNodeId) {
                return true;
            }
        }
        return false;
    }

    /**
     * add incoming relation
     */
    public void addIncoming(int sourceNodeId, int targetNodeId) {
        final int degree = inOffsets[targetNodeId];
        final int nextDegree = degree + 1;
        if (incoming[targetNodeId].length < nextDegree) {
            growIn(targetNodeId, nextDegree);
        }
        incoming[targetNodeId][degree] = sourceNodeId;
        inOffsets[targetNodeId] = nextDegree;
    }

    /**
     * checks for incoming target node, currently O(n)
     */
    public boolean hasIncoming(int sourceNodeId, int targetNodeId) {
        final int degree = inOffsets[sourceNodeId];
        int[] rels = incoming[sourceNodeId];
        for (int offset = degree-1; offset >= 0; offset--) {
            if (rels[offset]==targetNodeId) {
                return true;
            }
        }
        return false;
    }

    /**
     * get the degree for node / direction
     */
    public int degree(int nodeId, Direction direction) {
        switch (direction) {
            case OUTGOING: {
                return outOffsets[nodeId];
            }
            case INCOMING: {
                return inOffsets[nodeId];
            }
            default: {
                return inOffsets[nodeId] + outOffsets[nodeId];
            }
        }
    }

    /**
     * return an iterator for unweighted edges
     */
    public Iterator<RelationshipCursor> relationIterator(int nodeId, Direction direction) {
        switch (direction) {
            case OUTGOING:
                return new RelationIterator(nodeId, outgoing[nodeId], outOffsets[nodeId], direction);
            case INCOMING:
                return new RelationIterator(nodeId, incoming[nodeId], inOffsets[nodeId], direction);
            default:
                throw new IllegalArgumentException("Direction " + direction + " not implemented");
        }

    }

    /**
     * return an iterator for weighted edges
     */
    public Iterator<WeightedRelationshipCursor> weightedRelationIterator(int nodeId, WeightMapping weights, Direction direction) {
        switch (direction) {
            case OUTGOING:
                return new WeightedRelationIterator(nodeId, outgoing[nodeId], outOffsets[nodeId], weights, direction);
            case INCOMING:
                return new WeightedRelationIterator(nodeId, incoming[nodeId], inOffsets[nodeId], weights, direction);
            default:
                throw new IllegalArgumentException("Direction " + direction + " not implemented");
        }

    }

    /**
     * iterate over each edge at the given node using an unweighted consumer
     */
    public void forEach(int nodeId, Direction direction, RelationshipConsumer consumer) {
        switch (direction) {
            case OUTGOING:
                forEachOutgoing(nodeId, consumer);
                break;
            case INCOMING:
                forEachIncoming(nodeId, consumer);
                break;
            default:
                forEachIncoming(nodeId, consumer);
                forEachOutgoing(nodeId, consumer);
                break;
        }
    }

    /**
     * iterate over each edge at the given node using a weighted consumer
     */
    public void forEach(int nodeId, Direction direction, WeightMapping weights, WeightedRelationshipConsumer consumer) {
        switch (direction) {
            case OUTGOING:
                forEachOutgoing(nodeId, weights, consumer);
                break;
            case INCOMING:
                forEachIncoming(nodeId, weights, consumer);
                break;
            default:
                forEachIncoming(nodeId, weights, consumer);
                forEachOutgoing(nodeId, weights, consumer);
                break;
        }
    }

    public int capacity() {
        return outOffsets.length;
    }

    public void addMatrix(AdjacencyMatrix other, int offset, int length) {
        System.arraycopy(other.outOffsets, 0, outOffsets, offset, length);
        System.arraycopy(other.inOffsets, 0, inOffsets, offset, length);
        System.arraycopy(other.outgoing, 0, outgoing, offset, length);
        System.arraycopy(other.incoming, 0, incoming, offset, length);
    }

    private void forEachOutgoing(int nodeId, RelationshipConsumer consumer) {
        final int degree = outOffsets[nodeId];
        final int[] outs = outgoing[nodeId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(nodeId, outs[i], RawValues.combineIntInt(nodeId, outs[i]));
        }
    }

    private void forEachIncoming(int nodeId, RelationshipConsumer consumer) {
        final int degree = inOffsets[nodeId];
        final int[] ins = incoming[nodeId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(nodeId, ins[i], RawValues.combineIntInt(ins[i], nodeId));
        }
    }

    private void forEachOutgoing(int nodeId, WeightMapping weights, WeightedRelationshipConsumer consumer) {
        final int degree = outOffsets[nodeId];
        final int[] outs = outgoing[nodeId];
        for (int i = 0; i < degree; i++) {
            final long relationId = RawValues.combineIntInt(nodeId, outs[i]);
            consumer.accept(nodeId, outs[i], relationId, weights.get(relationId));
        }
    }

    private void forEachIncoming(int nodeId, WeightMapping weights, WeightedRelationshipConsumer consumer) {
        final int degree = inOffsets[nodeId];
        final int[] ins = incoming[nodeId];
        for (int i = 0; i < degree; i++) {
            final long relationId = RawValues.combineIntInt(ins[i], nodeId);
            consumer.accept(nodeId, ins[i], relationId, weights.get(relationId));
        }
    }

    public NodeIterator nodesWithRelationships(Direction direction) {
        if (direction == Direction.OUTGOING) {
            return new DegreeCheckingNodeIterator(outOffsets);
        } else {
            return new DegreeCheckingNodeIterator(inOffsets);
        }
    }

    private static class RelationIterator implements Iterator<RelationshipCursor> {

        private final RelationshipCursor cursor;
        private final int[] targetNodes;
        private final int nodeCount;
        private final IdCombiner relId;
        private int offset = 0;

        private RelationIterator(
                int nodeId,
                int[] targetNodes,
                int nodeCount,
                Direction direction) {
            this.targetNodes = targetNodes;
            this.nodeCount = nodeCount;
            cursor = new RelationshipCursor();
            cursor.sourceNodeId = nodeId;
            relId = RawValues.combiner(direction);
        }

        @Override
        public boolean hasNext() {
            return offset < nodeCount;
        }

        @Override
        public RelationshipCursor next() {
            cursor.targetNodeId = targetNodes[offset++];
            cursor.relationshipId = relId.apply(cursor);
            return cursor;
        }
    }

    private static class WeightedRelationIterator implements Iterator<WeightedRelationshipCursor> {

        private final WeightedRelationshipCursor cursor;
        private final int[] targetNodes;
        private final int nodeCount;
        private final WeightMapping weights;
        private final IdCombiner relId;
        private int offset = 0;

        private WeightedRelationIterator(
                int nodeId,
                int[] targetNodes,
                int nodeCount,
                WeightMapping weights,
                Direction direction) {
            this.targetNodes = targetNodes;
            this.nodeCount = nodeCount;
            this.weights = weights;
            cursor = new WeightedRelationshipCursor();
            cursor.sourceNodeId = nodeId;
            relId = RawValues.combiner(direction);
        }

        @Override
        public boolean hasNext() {
            return offset < nodeCount;
        }

        @Override
        public WeightedRelationshipCursor next() {
            cursor.targetNodeId = targetNodes[offset++];
            long relationId = relId.apply(cursor);
            cursor.relationshipId = relationId;
            cursor.weight = weights.get(relationId);
            return cursor;
        }
    }

    private static class DegreeCheckingNodeIterator implements NodeIterator {
        private final int[] array;

        DegreeCheckingNodeIterator(int[] array) {
            this.array = array;
        }

        @Override
        public void forEachNode(IntPredicate consumer) {
            for (int node = 0; node < array.length; node++) {
                if (array[node] > 0 && !consumer.test(node)) {
                    break;
                }
            }
        }

        @Override
        public PrimitiveIntIterator nodeIterator() {
            return new PrimitiveIntIterator() {
                int index = findNext();

                @Override
                public boolean hasNext() {
                    return index < array.length;
                }

                @Override
                public int next() {
                    try {
                        return index;
                    } finally {
                        index = findNext();
                    }
                }

                private int findNext() {
                    int length = array.length;
                    for (int n = index + 1; n < length; n++) {
                        if (array[n] > 0) {
                            return n;
                        }
                    }
                    return length;
                }
            };
        }
    }
}
