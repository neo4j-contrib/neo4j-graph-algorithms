package org.neo4j.graphalgo.core.leightweight;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.IdCombiner;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.IntPredicate;

/**
 *
 * @author phorn@avantgarde-labs.de
 */
public class LightGraph implements Graph {

    private final IdMap idMapping;
    private final WeightMapping weightMapping;
    private final IntArray adjacency;
    private final long[] inOffsets;
    private final long[] outOffsets;

    LightGraph(
            final IdMap idMapping,
            final WeightMapping weightMapping,
            final IntArray adjacency,
            final long[] inOffsets,
            final long[] outOffsets) {
        this.idMapping = idMapping;
        this.weightMapping = weightMapping;
        this.adjacency = adjacency;
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
                return new WeightedRelationIteratorImpl(vertexId, offset + 1, length, weightMapping, adjacency, direction);
            }

            case OUTGOING: {
                final long offset = outOffsets[vertexId];
                final int length = adjacency.get(offset);
                return new WeightedRelationIteratorImpl(vertexId, offset + 1, length, weightMapping, adjacency, direction);
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
                return new RelationIteratorImpl(vertexId, offset + 1, length, adjacency, direction);
            }

            case OUTGOING: {
                final long offset = outOffsets[vertexId];
                final int length = adjacency.get(offset);
                return new RelationIteratorImpl(vertexId, offset + 1, length, adjacency, direction);
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
        consumeNodes(node, cursor(node, inOffsets), RawValues.INCOMING, consumer);
    }

    public void forEachOutgoing(
            final int node,
            final RelationshipConsumer consumer) {
        consumeNodes(node, cursor(node, outOffsets), RawValues.OUTGOING, consumer);
    }

    public void forEachIncoming(
            final int node,
            final WeightedRelationshipConsumer consumer) {
        consumeNodes(node, cursor(node, inOffsets), RawValues.INCOMING, consumer);
    }

    public void forEachOutgoing(
            final int node,
            final WeightedRelationshipConsumer consumer) {
        consumeNodes(node, cursor(node, outOffsets), RawValues.OUTGOING, consumer);
    }

    private IntArray.Cursor cursor(int node, long[] offsets) {
        final long offset = offsets[node];
        final int length = adjacency.get(offset);
        return adjacency.cursor(offset + 1, length);
    }

    private void consumeNodes(
            int node,
            IntArray.Cursor cursor,
            IdCombiner relId,
            WeightedRelationshipConsumer consumer) {
        //noinspection UnnecessaryLocalVariable – prefer access of local var in loop
        final WeightMapping weightMap = this.weightMapping;
        while (cursor.next()) {
            final int[] array = cursor.array;
            int offset = cursor.offset;
            final int limit = cursor.length + offset;
            while (offset < limit) {
                consumer.accept(node, array[offset], relId.apply(node, array[offset]), weightMap.get((long) offset++));
            }
        }
    }

    private void consumeNodes(
            int node,
            IntArray.Cursor cursor,
            IdCombiner relId,
            RelationshipConsumer consumer) {
        while (cursor.next()) {
            final int[] array = cursor.array;
            int offset = cursor.offset;
            final int limit = cursor.length + offset;
            while (offset < limit) {
                consumer.accept(node, array[offset], relId.apply(node, array[offset++]));
            }
        }
    }
}
