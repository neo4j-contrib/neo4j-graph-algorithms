package org.neo4j.graphalgo.api;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphdb.Direction;

import java.util.Iterator;
import java.util.function.IntConsumer;

/**
 * @author mknblch
 *         added 02.03.2017.
 */
public interface Graph {

    /**
     * return the count of vertices in the graph
     */
    int nodeCount();

    /**
     * Calls consumer for each node id
     */
    void forEachNode(IntConsumer consumer);

    /**
     * Returns an Iterator on node ids;
     */
    PrimitiveIntIterator nodeIterator();

    /**
     * Return the degree for the given node / direction
     */
    int degree(int nodeId, Direction direction);

    void forEachRelation(int nodeId, Direction direction, RelationConsumer consumer);

    /**
     * Iterates over each edge of the given node / direction
     */
    Iterator<RelationCursor> relationIterator(int nodeId, Direction direction);

    void forEachRelation(int nodeId, Direction direction, WeightedRelationConsumer consumer);

    /**
     * Iterates over each edge of the given node / direction.
     */
    Iterator<WeightedRelationCursor> weightedRelationIterator(int nodeId, Direction direction);

    /**
     * Map node id to node id
     */
    int toMappedNodeId(long nodeId);

    /**
     * Map node id back to original node id
     * @param nodeId
     * @return
     */
    long toOriginalNodeId(int nodeId);
}
