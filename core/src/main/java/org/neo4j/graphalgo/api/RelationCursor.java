package org.neo4j.graphalgo.api;

/**
 * @author mknobloch
 */
public class RelationCursor {

    /**
     * the mapped source node id
     */
    public int sourceNodeId;

    /**
     * the mapped target node id
     */
    public int targetNodeId;

    /**
     * the original edge id
     */
    public long relationId;

}
