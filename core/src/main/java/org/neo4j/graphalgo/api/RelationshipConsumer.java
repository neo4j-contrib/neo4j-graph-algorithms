package org.neo4j.graphalgo.api;

/**
 * @author mknblch
 */
public interface RelationshipConsumer {

    /**
     * Called for every edge that matches a given relation-constraint
     * @param sourceNodeId mapped source node id
     * @param targetNodeId mapped target node id
     * @param relationId deprecated
     */
    boolean accept(int sourceNodeId, int targetNodeId, @Deprecated long relationId);
}
