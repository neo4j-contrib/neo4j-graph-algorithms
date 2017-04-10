package org.neo4j.graphalgo.api;

import java.util.Iterator;

/**
 * iterator for outgoing relations
 *
 * @author mknblch
 */
public interface OutgoingRelations {

    /**
     * iterate over each outgoing relation
     * @param nodeId graph-NodeId
     * @param consumer the relation Consumer
     */
    void forEachOutgoing(int nodeId, RelationConsumer consumer);

    /**
     * get an iterator for outgoing relations
     * @param nodeId graph-NodeId
     * @return Iterator for outgoing graph-nodeIds
     */
    Iterator<RelationCursor> outgoingIterator(int nodeId);
}
