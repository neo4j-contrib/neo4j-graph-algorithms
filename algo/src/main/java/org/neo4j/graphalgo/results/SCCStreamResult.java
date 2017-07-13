package org.neo4j.graphalgo.results;

/**
 * basic SCC result VO for streaming
 */
public class SCCStreamResult {

    /**
     * the node id
     */
    public final long nodeId;

    /**
     * the set id of the stronly connected component or
     * -1 of not part of a SCC
     */
    public final long partition;

    public SCCStreamResult(long nodeId, int clusterId) {
        this.nodeId = nodeId;
        this.partition = clusterId;
    }
}