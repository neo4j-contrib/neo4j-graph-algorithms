package org.neo4j.graphalgo.core.leightweight;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.utils.IdCombiner;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.function.IntPredicate;

/**
 * @author phorn@avantgarde-labs.de
 */
public class LightGraph implements Graph {

    private final IdMap idMapping;
    private WeightMapping weightMapping;
    private IntArray inAdjacency;
    private IntArray outAdjacency;
    private long[] inOffsets;
    private long[] outOffsets;

    LightGraph(
            final IdMap idMapping,
            final WeightMapping weightMapping,
            final IntArray inAdjacency,
            final IntArray outAdjacency,
            final long[] inOffsets,
            final long[] outOffsets) {
        this.idMapping = idMapping;
        this.weightMapping = weightMapping;
        this.inAdjacency = inAdjacency;
        this.outAdjacency = outAdjacency;
        this.inOffsets = inOffsets;
        this.outOffsets = outOffsets;
    }

    @Override
    public int nodeCount() {
        return idMapping.size();
    }

    @Override
    public PrimitiveIntIterator nodeIterator() {
        return idMapping.iterator();
    }

    @Override
    public Collection<PrimitiveIntIterable> batchIterables(final int batchSize) {
        return idMapping.batchIterables(batchSize);
    }

    @Override
    public void forEachNode(IntPredicate consumer) {
        idMapping.forEach(consumer);
    }

    @Override
    public void forEachRelationship(
            int vertexId,
            Direction direction,
            RelationshipConsumer consumer) {
        switch (direction) {
            case INCOMING:
                forEachIncoming(vertexId, consumer);
                return;

            case OUTGOING:
                forEachOutgoing(vertexId, consumer);
                return;

            case BOTH:
                forEachIncoming(vertexId, consumer);
                forEachOutgoing(vertexId, consumer);
                return;

            default:
                throw new IllegalArgumentException(direction + "");
        }
    }

    @Override
    public void forEachRelationship(
            int vertexId,
            Direction direction,
            WeightedRelationshipConsumer consumer) {
        switch (direction) {
            case INCOMING:
                forEachIncoming(vertexId, consumer);
                return;

            case OUTGOING:
                forEachOutgoing(vertexId, consumer);
                return;

            case BOTH:
                forEachIncoming(vertexId, consumer);
                forEachOutgoing(vertexId, consumer);
                return;

            default:
                throw new IllegalArgumentException(direction + "");
        }
    }

    @Override
    public int degree(
            final int node,
            final Direction direction) {
        switch (direction) {
            case INCOMING:
                return (int) (inOffsets[node + 1] - inOffsets[node]);

            case OUTGOING:
                return (int) (outOffsets[node + 1] - outOffsets[node]);

            case BOTH:
                return (int) (inOffsets[node + 1] - inOffsets[node] + outOffsets[node + 1] - outOffsets[node]);

            default:
                throw new IllegalArgumentException(direction + "");
        }

    }

    @Override
    public int toMappedNodeId(long nodeId) {
        return idMapping.get(nodeId);
    }

    @Override
    public long toOriginalNodeId(int vertexId) {
        return idMapping.toOriginalNodeId(vertexId);
    }

    @Override
    public boolean contains(final long nodeId) {
        return idMapping.contains(nodeId);
    }

    public void forEachIncoming(
            final int node,
            final RelationshipConsumer consumer) {
        IntArray.Cursor cursor = cursor(node, inOffsets, inAdjacency);
        consumeNodes(node, cursor, RawValues.INCOMING, consumer);
    }

    public void forEachOutgoing(
            final int node,
            final RelationshipConsumer consumer) {
        IntArray.Cursor cursor = cursor(node, outOffsets, outAdjacency);
        consumeNodes(node, cursor, RawValues.OUTGOING, consumer);
    }

    private void forEachIncoming(
            final int node,
            final WeightedRelationshipConsumer consumer) {
        IntArray.Cursor cursor = cursor(node, inOffsets, inAdjacency);
        consumeNodes(node, cursor, RawValues.INCOMING, consumer);
    }

    private void forEachOutgoing(
            final int node,
            final WeightedRelationshipConsumer consumer) {
        IntArray.Cursor cursor = cursor(node, outOffsets, outAdjacency);
        consumeNodes(node, cursor, RawValues.OUTGOING, consumer);
    }

     private IntArray.Cursor cursor(int node, long[] offsets, IntArray array) {
         final long offset = offsets[node];
         final long length = offsets[node + 1] - offset;
         return array.cursor(offset, length);
    }

    private void consumeNodes(
            int startNode,
            IntArray.Cursor cursor,
            IdCombiner relId,
            WeightedRelationshipConsumer consumer) {
        //noinspection UnnecessaryLocalVariable – prefer access of local var in loop
        final WeightMapping weightMap = this.weightMapping;
        while (cursor.next()) {
            final int[] array = cursor.array;
            int offset = cursor.offset;
            final int limit = cursor.limit;
            while (offset < limit) {
                int targetNode = array[offset++];
                long relationId = relId.apply(startNode, targetNode);
                consumer.accept(
                        startNode,
                        targetNode,
                        relationId,
                        weightMap.get(relationId)
                );
            }
        }
    }

    private void consumeNodes(
            int startNode,
            IntArray.Cursor cursor,
            IdCombiner relId,
            RelationshipConsumer consumer) {
        while (cursor.next()) {
            final int[] array = cursor.array;
            int offset = cursor.offset;
            final int limit = cursor.limit;
            while (offset < limit) {
                int targetNode = array[offset++];
                long relationId = relId.apply(startNode, targetNode);
                consumer.accept(startNode, targetNode, relationId);
            }
        }
    }

    @Override
    public void release() {
        weightMapping = null;
        inAdjacency = null;
        outAdjacency = null;
        inOffsets = null;
        outOffsets = null;
    }
}
