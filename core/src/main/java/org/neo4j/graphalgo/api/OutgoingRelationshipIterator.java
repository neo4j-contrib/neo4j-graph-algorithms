package org.neo4j.graphalgo.api;

/**
 * iterator for outgoing relations.
 *
 * @author mknblch
 */
public interface OutgoingRelationshipIterator {

    /**
     * iterate over each outgoing relation or until
     * the consumer stops the iteration.
     *
     * @param nodeId graph-NodeId
     * @param consumer the relation Consumer
     */
    void forEachOutgoing(int nodeId, RelationshipConsumer consumer);
}
