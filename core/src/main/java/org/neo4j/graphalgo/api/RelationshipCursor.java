package org.neo4j.graphalgo.api;

/**
 * @author mknobloch
 */
public class RelationshipCursor {

    /**
     * the mapped source node id
     */
    public int sourceNodeId;

    /**
     * the mapped target node id
     */
    public int targetNodeId;

    /**
     * deprecated
     */
    @Deprecated
    public long relationshipId;
}
