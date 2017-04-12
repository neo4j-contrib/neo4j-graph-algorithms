package org.neo4j.graphalgo.api;

import java.util.Iterator;

/**
 * iterator for outgoing relations with weights
 *
 * @author mknblch
 */
public interface WeightedOutgoingRelationshipIterator {

    /**
     * iterate over each outgoing relation
     * @param nodeId graph-NodeId
     * @param consumer the relation Consumer
     */
    void forEachOutgoing(int nodeId, WeightedRelationshipConsumer consumer);

    /**
     * get an iterator for outgoing relations
     * @param nodeId graph-NodeId
     * @return Iterator for outgoing graph-nodeIds
     */
    Iterator<WeightedRelationshipCursor> outgoingWeightedIterator(int nodeId);
}
