package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

import java.util.Iterator;

/**
 * @author mknblch
 *         added 07.03.2017.
 */
public interface WeightedGraph extends Graph {

    void forEachEdge(int vertexId, Direction direction, WeightedEdgeConsumer consumer);

    Iterator<WeightedEdgeCursor> weightedEdgeIterator(int vertexId, Direction direction);
}
