package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

import java.util.Iterator;

/**
 * @author mknblch
 */
public interface WeightedRelationIterator {

    void forEachRelation(int nodeId, Direction direction, WeightedRelationConsumer consumer);

    Iterator<WeightedRelationCursor> weightedRelationIterator(int nodeId, Direction direction);
}
