package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.api.*;
import org.apache.lucene.util.ArrayUtil;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Relation Container built of multiple arrays. The node capacity must be constant and the node IDs have to be
 * smaller then the capacity. The number of relations per node is limited only to the maximum array size of the VM
 * and connections can be added dynamically.
 *
 * @author mknblch
 */
class AdjacencyMatrix {

    private static final int[] EMPTY_INTS = new int[0];
    private static final long[] EMPTY_LONGS = new long[0];

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
    /**
     * matrix nodeId x [outgoing relation-relationIds..]
     */
    final long[][] outgoingIds;
    /**
     * matrix nodeId x [incoming relation-relationIds..]
     */
    final long[][] incomingIds;

    AdjacencyMatrix(int nodeCount) {
        this.outOffsets = new int[nodeCount];
        this.inOffsets = new int[nodeCount];
        this.outgoing = new int[nodeCount][];
        this.incoming = new int[nodeCount][];
        this.outgoingIds = new long[nodeCount][];
        this.incomingIds = new long[nodeCount][];
        Arrays.fill(outgoing, EMPTY_INTS);
        Arrays.fill(incoming, EMPTY_INTS);
        Arrays.fill(outgoingIds, EMPTY_LONGS);
        Arrays.fill(incomingIds, EMPTY_LONGS);
    }

    AdjacencyMatrix(
            final int[] outOffsets,
            final int[] inOffsets,
            final int[][] outgoing,
            final int[][] incoming,
            final long[][] outgoingIds,
            final long[][] incomingIds) {
        this.outOffsets = outOffsets;
        this.inOffsets = inOffsets;
        this.outgoing = outgoing;
        this.incoming = incoming;
        this.outgoingIds = outgoingIds;
        this.incomingIds = incomingIds;
    }

    /**
     * initialize array for outgoing connections
     */
    public void armOut(int sourceNodeId, int degree) {
        outgoing[sourceNodeId] = Arrays.copyOf(outgoing[sourceNodeId], degree);
        outgoingIds[sourceNodeId] = Arrays.copyOf(outgoingIds[sourceNodeId], degree);
    }

    /**
     * initialize array for incoming connections
     */
    public void armIn(int targetNodeId, int degree) {
        incoming[targetNodeId] = Arrays.copyOf(incoming[targetNodeId], degree);
        incomingIds[targetNodeId] = Arrays.copyOf(incomingIds[targetNodeId], degree);
    }

    /**
     * grow array for outgoing connections
     */
    public void growOut(int sourceNodeId, int length) {
        outgoing[sourceNodeId] = ArrayUtil.grow(outgoing[sourceNodeId], length);
        outgoingIds[sourceNodeId] = ArrayUtil.grow(outgoingIds[sourceNodeId], length);
    }

    /**
     * grow array for incoming connections
     */
    public void growIn(int targetNodeId, int length) {
        incoming[targetNodeId] = ArrayUtil.grow(incoming[targetNodeId], length);
        incomingIds[targetNodeId] = ArrayUtil.grow(incomingIds[targetNodeId], length);
    }

    /**
     * add outgoing relation
     */
    public void addOutgoing(int sourceNodeId, int targetNodeId, long relationId) {
        final int degree = outOffsets[sourceNodeId];
        final int nextDegree = degree + 1;
        growOut(sourceNodeId, degree + 1);
        outgoing[sourceNodeId][degree] = targetNodeId;
        outgoingIds[sourceNodeId][degree] = relationId;
        outOffsets[sourceNodeId] = nextDegree;
    }

    /**
     * add incoming relation
     */
    public void addIncoming(int sourceNodeId, int targetNodeId, long relationId) {
        final int degree = inOffsets[targetNodeId];
        final int nextDegree = degree + 1;
        growIn(targetNodeId, degree + 1);
        incoming[targetNodeId][degree] = sourceNodeId;
        incomingIds[targetNodeId][degree] = relationId;
        inOffsets[targetNodeId] = nextDegree;
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
                return new RelationIterator(nodeId, outgoing[nodeId], outgoingIds[nodeId], outOffsets[nodeId]);
            case INCOMING:
                return new RelationIterator(nodeId, incoming[nodeId], incomingIds[nodeId], inOffsets[nodeId]);
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
                return new WeightedRelationIterator(nodeId, outgoing[nodeId], outgoingIds[nodeId], outOffsets[nodeId], weights);
            case INCOMING:
                return new WeightedRelationIterator(nodeId, incoming[nodeId], incomingIds[nodeId], inOffsets[nodeId], weights);
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

    public void addMatrix(AdjacencyMatrix other, int offset, int length) {
        System.arraycopy(other.outOffsets, 0, outOffsets, offset, length);
        System.arraycopy(other.inOffsets, 0, inOffsets, offset, length);
        System.arraycopy(other.outgoing, 0, outgoing, offset, length);
        System.arraycopy(other.incoming, 0, incoming, offset, length);
        System.arraycopy(other.outgoingIds, 0, outgoingIds, offset, length);
        System.arraycopy(other.incomingIds, 0, incomingIds, offset, length);
    }

    private void forEachOutgoing(int nodeId, RelationshipConsumer consumer) {
        final int degree = outOffsets[nodeId];
        final int[] outs = outgoing[nodeId];
        final long[] outIds = outgoingIds[nodeId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(nodeId, outs[i], outIds[i]);
        }
    }

    private void forEachIncoming(int nodeId, RelationshipConsumer consumer) {
        final int degree = inOffsets[nodeId];
        final int[] ins = incoming[nodeId];
        final long[] inIds = incomingIds[nodeId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(nodeId, ins[i], inIds[i]);
        }
    }

    private void forEachOutgoing(int nodeId, WeightMapping weights, WeightedRelationshipConsumer consumer) {
        final int degree = outOffsets[nodeId];
        final int[] outs = outgoing[nodeId];
        final long[] outIds = outgoingIds[nodeId];
        for (int i = 0; i < degree; i++) {
            final long relationId = outIds[i];
            consumer.accept(nodeId, outs[i], relationId, weights.get(relationId));
        }
    }

    private void forEachIncoming(int nodeId, WeightMapping weights, WeightedRelationshipConsumer consumer) {
        final int degree = inOffsets[nodeId];
        final int[] ins = incoming[nodeId];
        final long[] inIds = incomingIds[nodeId];
        for (int i = 0; i < degree; i++) {
            final long relationId = inIds[i];
            consumer.accept(nodeId, ins[i], relationId, weights.get(relationId));
        }
    }

    private static class RelationIterator implements Iterator<RelationshipCursor> {

        private final RelationshipCursor cursor;
        private final int[] targetNodes;
        private final long[] relationIds;
        private final int nodeCount;
        private int offset = 0;

        private RelationIterator(int nodeId, int[] targetNodes, long[] originalRelationIds, int nodeCount) {
            this.targetNodes = targetNodes;
            this.relationIds = originalRelationIds;
            this.nodeCount = nodeCount;
            cursor = new RelationshipCursor();
            cursor.sourceNodeId = nodeId;
        }

        @Override
        public boolean hasNext() {
            return offset < nodeCount;
        }

        @Override
        public RelationshipCursor next() {
            cursor.targetNodeId = targetNodes[offset];
            cursor.relationshipId = relationIds[offset++];
            return cursor;
        }
    }

    private static class WeightedRelationIterator implements Iterator<WeightedRelationshipCursor> {

        private final WeightedRelationshipCursor cursor;
        private final int[] targetNodes;
        private final long[] relationIds;
        private final int nodeCount;
        private final WeightMapping weights;
        private int offset = 0;

        private WeightedRelationIterator(int nodeId, int[] targetNodes, long[] relationIds, int nodeCount, WeightMapping weights) {
            this.targetNodes = targetNodes;
            this.relationIds = relationIds;
            this.nodeCount = nodeCount;
            this.weights = weights;
            cursor = new WeightedRelationshipCursor();
            cursor.sourceNodeId = nodeId;
        }

        @Override
        public boolean hasNext() {
            return offset < nodeCount;
        }

        @Override
        public WeightedRelationshipCursor next() {
            cursor.targetNodeId = targetNodes[offset];
            final long relationId = relationIds[offset++];
            cursor.relationshipId = relationId;
            cursor.weight = weights.get(relationId);
            return cursor;
        }
    }

}
