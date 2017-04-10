package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphdb.Direction;

import java.util.Iterator;
import java.util.function.IntConsumer;

/**
 * Heavy weighted graph built of an adjacency matrix.
 *
 * Pro:
 *  - ~3 times faster due to little overhead in index lookups
 *  - connections can be added in arbitrary order
 *
 * Cons:
 *  - has a higher memory consumption // TODO evaluate
 *
 * @author mknblch
 */
public class HeavyGraph implements Graph {

    private final IdMap nodeIdMap;
    private final AdjacencyMatrix container;
    private final WeightMapping weights;

    HeavyGraph(
            IdMap nodeIdMap,
            AdjacencyMatrix container,
            final WeightMapping weights) {
        this.nodeIdMap = nodeIdMap;
        this.container = container;
        this.weights = weights;
    }

    @Override
    public int nodeCount() {
        return nodeIdMap.size();
    }

    @Override
    public void forEachNode(IntConsumer consumer) {
        nodeIdMap.forEach(consumer);
    }

    @Override
    public PrimitiveIntIterator nodeIterator() {
        return nodeIdMap.iterator();
    }

    @Override
    public int degree(int nodeId, Direction direction) {
        return container.degree(nodeId, direction);
    }

    @Override
    public void forEachRelation(int nodeId, Direction direction, RelationshipConsumer consumer) {
        container.forEach(nodeId, direction, consumer);
    }

    @Override
    public void forEachRelationship(
            final int nodeId,
            final Direction direction,
            final WeightedRelationshipConsumer consumer) {
        container.forEach(nodeId, direction, weights, consumer);
    }

    @Override
    public Iterator<RelationshipCursor> relationIterator(int nodeId, Direction direction) {
        return container.relationIterator(nodeId, direction);
    }

    @Override
    public Iterator<WeightedRelationshipCursor> weightedRelationshipIterator(
            final int nodeId, final Direction direction) {
        return container.weightedRelationIterator(nodeId, weights, direction);
    }

    @Override
    public int toMappedNodeId(long originalNodeId) {
        return nodeIdMap.get(originalNodeId);
    }

    @Override
    public long toOriginalNodeId(int mappedNodeId) {
        return nodeIdMap.unmap(mappedNodeId);
    }
}
