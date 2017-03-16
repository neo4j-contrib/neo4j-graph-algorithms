package org.neo4j.graphalgo.api;

/**
 * @author mknobloch
 */
public class EdgeCursor {

    /**
     * the mapped source node id
     */
    public int sourceVertexId;

    /**
     * the mapped target node id
     */
    public int targetVertexId;

    /**
     * the original edge id
     */
    public long edgeId;

}
