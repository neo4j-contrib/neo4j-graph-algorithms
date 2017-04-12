package org.neo4j.graphalgo.core.utils.dss;

/**
 * A structure to manage disjoint sets of ints e.g. nodeIds
 *
 * Implemented using a recursive path compression logic.
 *
 * TODO: naming
 * @author mknblch
 */
public class PathCompressingDSS extends AbstractDisjointSet {

    public PathCompressingDSS(int capacity) {
        super(capacity);
    }

    @Override
    public int find(int p) {
        if (parent[p] == p) return p;
        // path compression optimization
        parent[p] = find(parent[p]); // balance tree while traversing
        return parent[p];
    }
}