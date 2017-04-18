package org.neo4j.graphalgo.api;

import java.util.Iterator;

/**
 * Iterator for incoming relations with weights.
 *
 * @author mknblch
 */
@Deprecated
public interface WeightedIncomingRelationshipIterator {

    void forEachIncoming(int nodeId, WeightedRelationshipConsumer consumer);

    Iterator<WeightedRelationshipCursor> incomingWeightedIterator(int nodeId);
}
