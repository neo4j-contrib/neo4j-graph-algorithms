package org.neo4j.graphalgo.core.heavyweight;

import org.apache.lucene.util.ArrayUtil;
import org.neo4j.graphalgo.api.EdgeConsumer;
import org.neo4j.graphalgo.api.EdgeCursor;
import org.neo4j.graphalgo.api.WeightedEdgeConsumer;
import org.neo4j.graphalgo.api.WeightedEdgeCursor;
import org.neo4j.graphalgo.core.WeightMapping;
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
     * matrix nodeId x [outgoing edge-ids..]
     */
    final int[][] outgoing;
    /**
     * matrix nodeId x [incoming edge-ids..]
     */
    final int[][] incoming;
    /**
     * matrix nodeId x [outgoing relation-ids..]
     */
    final long[][] outgoingIds;
    /**
     * matrix nodeId x [incoming relation-ids..]
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
    public void armOut(int sourceVertexId, int degree) {
        outgoing[sourceVertexId] = new int[degree];
        outgoingIds[sourceVertexId] = new long[degree];
    }

    /**
     * initialize array fro incoming connections
     */
    public void armIn(int targetVertexId, int degree) {
        incoming[targetVertexId] = new int[degree];
        incomingIds[targetVertexId] = new long[degree];
    }

    /**
     * add outgoing edge
     */
    public void addOutgoing(int sourceVertexId, int targetVertexId, long edgeId) {
        final int degree = outOffsets[sourceVertexId];
        final int nextDegree = degree + 1;

        outgoing[sourceVertexId][degree] = targetVertexId;
        outgoingIds[sourceVertexId][degree] = edgeId;
        outOffsets[sourceVertexId] = nextDegree;
    }

    /**
     * add incoming edge
     */
    public void addIncoming(int sourceVertexId, int targetVertexId, long edgeId) {
        final int degree = inOffsets[targetVertexId];
        final int nextDegree = degree + 1;

        incoming[targetVertexId][degree] = sourceVertexId;
        incomingIds[targetVertexId][degree] = edgeId;
        inOffsets[targetVertexId] = nextDegree;
    }

    /**
     * get the degree for vertex / direction
     */
    public int degree(int vertexId, Direction direction) {
        switch (direction) {
            case OUTGOING: {
                return outOffsets[vertexId];
            }
            case INCOMING: {
                return inOffsets[vertexId];
            }
            default: {
                return inOffsets[vertexId] + outOffsets[vertexId];
            }
        }
    }

    /**
     * return an iterator for unweighted edges
     */
    public Iterator<EdgeCursor> edgeIterator(int vertexId, Direction direction) {
        switch (direction) {
            case OUTGOING:
                return new EdgeIterator(vertexId, outgoing[vertexId], outgoingIds[vertexId], outOffsets[vertexId]);
            case INCOMING:
                return new EdgeIterator(vertexId, incoming[vertexId], incomingIds[vertexId], inOffsets[vertexId]);
            default:
                throw new IllegalArgumentException("Direction " + direction + " not implemented");
        }

    }

    /**
     * return an iterator for weighted edges
     */
    public Iterator<WeightedEdgeCursor> weightedEdgeIterator(int vertexId, WeightMapping weights, Direction direction) {
        switch (direction) {
            case OUTGOING:
                return new WeightedEdgeIterator(vertexId, outgoing[vertexId], outgoingIds[vertexId], outOffsets[vertexId], weights);
            case INCOMING:
                return new WeightedEdgeIterator(vertexId, incoming[vertexId], incomingIds[vertexId], inOffsets[vertexId], weights);
            default:
                throw new IllegalArgumentException("Direction " + direction + " not implemented");
        }

    }

    /**
     * iterate over each edge at the given vertex using an unweighted consumer
     */
    public void forEach(int vertexId, Direction direction, EdgeConsumer consumer) {
        switch (direction) {
            case OUTGOING:
                forEachOutgoing(vertexId, consumer);
                break;
            case INCOMING:
                forEachIncoming(vertexId, consumer);
                break;
            default:
                forEachIncoming(vertexId, consumer);
                forEachOutgoing(vertexId, consumer);
                break;
        }
    }

    /**
     * iterate over each edge at the given vertex using a weighted consumer
     */
    public void forEach(int vertexId, Direction direction, WeightMapping weights, WeightedEdgeConsumer consumer) {
        switch (direction) {
            case OUTGOING:
                forEachOutgoing(vertexId, weights, consumer);
                break;
            case INCOMING:
                forEachIncoming(vertexId, weights, consumer);
                break;
            default:
                forEachIncoming(vertexId, weights, consumer);
                forEachOutgoing(vertexId, weights, consumer);
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

    private void forEachOutgoing(int vertexId, EdgeConsumer consumer) {
        final int degree = outOffsets[vertexId];
        final int[] outs = outgoing[vertexId];
        final long[] outIds = outgoingIds[vertexId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(vertexId, outs[i], outIds[i]);
        }
    }

    private void forEachIncoming(int vertexId, EdgeConsumer consumer) {
        final int degree = inOffsets[vertexId];
        final int[] ins = incoming[vertexId];
        final long[] inIds = incomingIds[vertexId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(vertexId, ins[i], inIds[i]);
        }
    }

    private void forEachOutgoing(int vertexId, WeightMapping weights, WeightedEdgeConsumer consumer) {
        final int degree = outOffsets[vertexId];
        final int[] outs = outgoing[vertexId];
        final long[] outIds = outgoingIds[vertexId];
        for (int i = 0; i < degree; i++) {
            final long edgeId = outIds[i];
            consumer.accept(vertexId, outs[i], edgeId, weights.get(edgeId));
        }
    }

    private void forEachIncoming(int vertexId, WeightMapping weights, WeightedEdgeConsumer consumer) {
        final int degree = inOffsets[vertexId];
        final int[] ins = incoming[vertexId];
        final long[] inIds = incomingIds[vertexId];
        for (int i = 0; i < degree; i++) {
            final long edgeId = inIds[i];
            consumer.accept(vertexId, ins[i], edgeId, weights.get(edgeId));
        }
    }

    private static class EdgeIterator implements Iterator<EdgeCursor> {

        private final EdgeCursor cursor;
        private final int[] data;
        private final long[] ids;
        private final int elements;
        private int offset = 0;

        private EdgeIterator(int vertexId, int[] data, long[] ids, int elements) {
            this.data = data;
            this.ids = ids;
            this.elements = elements;
            cursor = new EdgeCursor();
            cursor.sourceVertexId = vertexId;
        }

        @Override
        public boolean hasNext() {
            return offset < elements;
        }

        @Override
        public EdgeCursor next() {
            cursor.targetVertexId = data[offset];
            cursor.edgeId = ids[offset++];
            return cursor;
        }
    }

    private static class WeightedEdgeIterator implements Iterator<WeightedEdgeCursor> {

        private final WeightedEdgeCursor cursor;
        private final int[] data;
        private final long[] ids;
        private final int elements;
        private final WeightMapping weights;
        private int offset = 0;

        private WeightedEdgeIterator(int vertexId, int[] data, long[] ids, int elements, WeightMapping weights) {
            this.data = data;
            this.ids = ids;
            this.elements = elements;
            this.weights = weights;
            cursor = new WeightedEdgeCursor();
            cursor.sourceVertexId = vertexId;
        }

        @Override
        public boolean hasNext() {
            return offset < elements;
        }

        @Override
        public WeightedEdgeCursor next() {
            cursor.targetVertexId = data[offset];
            final long edgeId = ids[offset++];
            cursor.edgeId = edgeId;
            cursor.weight = weights.get(edgeId);
            return cursor;
        }
    }

}
