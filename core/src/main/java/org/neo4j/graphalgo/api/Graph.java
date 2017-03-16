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
    int vertexCount();

    /**
     * Calls consumer for each vertex id
     */
    void forEachVertex(IntConsumer consumer);

    /**
     * Returns an Iterator on vertex ids;
     */
    PrimitiveIntIterator vertexIterator();

    /**
     * Return the degree for the given vertex / direction
     */
    int degree(int vertexId, Direction direction);

    void forEachEdge(int vertexId, Direction direction, EdgeConsumer consumer);

    /**
     * Iterates over each edge of the given vertex / direction
     */
    Iterator<EdgeCursor> edgeIterator(int vertexId, Direction direction);

    void forEachEdge(int vertexId, Direction direction, WeightedEdgeConsumer consumer);

    /**
     * Iterates over each edge of the given vertex / direction.
     */
    Iterator<WeightedEdgeCursor> weightedEdgeIterator(int vertexId, Direction direction);

    /**
     * Map node id to vertex id
     */
    int toVertexId(long nodeId);

    /**
     * Map vertex id back to original node id
     * @param vertexId
     * @return
     */
    long toNodeId(int vertexId);
}
