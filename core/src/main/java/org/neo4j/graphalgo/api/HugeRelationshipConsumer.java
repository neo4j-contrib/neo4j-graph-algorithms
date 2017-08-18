package org.neo4j.graphalgo.api;

/**
 * consumer interface for unweighted relationships.
 *
 * @author mknblch
 */
public interface HugeRelationshipConsumer {

    /**
     * Called for every edge that matches a given relation-constraint
     *
     * @param sourceNodeId mapped source node id
     * @param targetNodeId mapped target node id
     * @return {@code true} if the iteration shall continue, otherwise {@code false}.
     */
    boolean accept(
            long sourceNodeId,
            long targetNodeId);
}
