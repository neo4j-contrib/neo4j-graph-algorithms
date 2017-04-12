package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.core.WeightMap;

import java.util.Iterator;

/**
 * iterates over each relation
 *
 * @author mknblch
 */
public interface WeightedAllRelationshipIterator {

    /**
     * called once for each relation
     */
    void forEachRelationship(WeightedRelationshipConsumer consumer);

    /**
     * get an iterator for each relation
     */
    Iterator<WeightedRelationshipCursor> allWeightedRelationshipIterator();
}
