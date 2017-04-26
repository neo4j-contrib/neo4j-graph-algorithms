package org.neo4j.graphalgo.core.leightweight;

import com.carrotsearch.hppc.LongLongMap;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphdb.Direction;

import java.util.Iterator;
import java.util.function.IntConsumer;

/**
 *
 * @author phorn@avantgarde-labs.de
 */
public class LightGraph implements Graph {

    private final IdMap idMapping;
    private final WeightMapping weightMapping;
    private final LongLongMap relationIdMapping;
    private final IntArray adjacency;
    private final long[] inOffsets;
    private final long[] outOffsets;
    private final IntArray.Cursor spare;

    LightGraph(
            final IdMap idMapping,
            final WeightMapping weightMapping,
            final LongLongMap relationIdMapping,
            final IntArray adjacency,
            final long[] inOffsets,
            final long[] outOffsets) {
        this.idMapping = idMapping;
        this.weightMapping = weightMapping;
        this.relationIdMapping = relationIdMapping;
        this.adjacency = adjacency;
        this.inOffsets = inOffsets;
        this.outOffsets = outOffsets;
        this.spare = adjacency.newCursor();
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
    public void forEachNode(IntConsumer consumer) {
        idMapping.forEach(consumer);
    }

    @Override
    public void forEachRelationship(int vertexId, Direction direction, RelationshipConsumer consumer) {
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
    public void forEachRelationship(int vertexId, Direction direction, WeightedRelationshipConsumer consumer) {
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
    public Iterator<WeightedRelationshipCursor> weightedRelationshipIterator(int vertexId, Direction direction) {

        switch (direction) {
            case INCOMING: {
                final long offset = inOffsets[vertexId];
                final int length = adjacency.get(offset);
                return new WeightedRelationIteratorImpl(vertexId, offset + 1, length, weightMapping, relationIdMapping, adjacency);
            }

            case OUTGOING: {
                final long offset = outOffsets[vertexId];
                final int length = adjacency.get(offset);
                return new WeightedRelationIteratorImpl(vertexId, offset + 1, length, weightMapping, relationIdMapping, adjacency);
            }
            default: {
                throw new IllegalArgumentException("Direction.BOTH not yet implemented");
            }
        }
    }

    @Override
    public Iterator<RelationshipCursor> relationshipIterator(int vertexId, Direction direction) {

        switch (direction) {
            case INCOMING: {
                final long offset = inOffsets[vertexId];
                final int length = adjacency.get(offset);
                return new RelationIteratorImpl(vertexId, offset + 1, length, relationIdMapping, adjacency);
            }

            case OUTGOING: {
                final long offset = outOffsets[vertexId];
                final int length = adjacency.get(offset);
                return new RelationIteratorImpl(vertexId, offset + 1, length, relationIdMapping, adjacency);
            }

            default: {
                throw new IllegalArgumentException("Not yet implemented");
            }

        }
    }

    @Override
    public int degree(
            final int node,
            final Direction direction) {
        switch (direction) {
            case INCOMING:
                return adjacency.get(inOffsets[node]);

            case OUTGOING:
                return adjacency.get(outOffsets[node]);

            case BOTH:
                return adjacency.get(inOffsets[node])
                        + adjacency.get(outOffsets[node]);

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
        return idMapping.unmap(vertexId);
    }

    public void forEachIncoming(
            final int node,
            final RelationshipConsumer consumer) {
        consumeNodes(node, cursor(node, inOffsets), consumer);
    }

    public void forEachOutgoing(
            final int node,
            final RelationshipConsumer consumer) {
        consumeNodes(node, cursor(node, outOffsets), consumer);
    }

    public void forEachIncoming(
            final int node,
            final WeightedRelationshipConsumer consumer) {
        consumeNodes(node, cursor(node, inOffsets), consumer);
    }

    public void forEachOutgoing(
            final int node,
            final WeightedRelationshipConsumer consumer) {
        consumeNodes(node, cursor(node, outOffsets), consumer);
    }

    private IntArray.Cursor cursor(int node, long[] offsets) {
        final long offset = offsets[node];
        final int length = adjacency.get(offset);
        return adjacency.cursor(offset + 1, length, spare);
    }

    private void consumeNodes(
            int node,
            IntArray.Cursor cursor,
            WeightedRelationshipConsumer consumer) {
        //noinspection UnnecessaryLocalVariable – prefer access of local var in loop
        final WeightMapping weightMap = this.weightMapping;
        //noinspection UnnecessaryLocalVariable – prefer access of local var in loop
        final LongLongMap relMap = this.relationIdMapping;
        while (cursor.next()) {
            final int[] array = cursor.array;
            int offset = cursor.offset;
            final int limit = cursor.length + offset;
            while (offset < limit) {
                consumer.accept(node, array[offset], relMap.get(offset), weightMap.get((long) offset++));
            }
        }
    }

    private void consumeNodes(
            int node,
            IntArray.Cursor cursor,
            RelationshipConsumer consumer) {
        //noinspection UnnecessaryLocalVariable – prefer access of local var in loop
        final LongLongMap relMap = this.relationIdMapping;
        while (cursor.next()) {
            final int[] array = cursor.array;
            int offset = cursor.offset;
            final int limit = cursor.length + offset;
            while (offset < limit) {
                consumer.accept(node, array[offset], relMap.get(offset++));
            }
        }
    }
}
