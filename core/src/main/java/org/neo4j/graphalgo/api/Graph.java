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

    int vertexCount();

    void forEachVertex(IntConsumer consumer);

    PrimitiveIntIterator vertexIterator();

    int degree(int vertexId, Direction direction);

    void forEachEdge(int vertexId, Direction direction, EdgeConsumer consumer);

    Iterator<EdgeCursor> edgeIterator(int vertexId, Direction direction);

    void forEachEdge(int vertexId, Direction direction, WeightedEdgeConsumer consumer);

    Iterator<WeightedEdgeCursor> weightedEdgeIterator(int vertexId, Direction direction);

    int toVertexId(long nodeId);

    long toNodeId(int vertexId);
}
