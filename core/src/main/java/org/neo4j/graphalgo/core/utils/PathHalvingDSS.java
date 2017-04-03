package org.neo4j.graphalgo.core.utils;

/**
 * A structure to manage disjoint sets of ints e.g. nodeIds
 *
 * Non-recursive implementation using path-halving optimization.
 *
 * @author mknblch
 */
public class PathHalvingDSS extends AbstractDisjointSet {

    public PathHalvingDSS(int capacity) {
        super(capacity);
    }

    @Override
    public int find(int p) {
        while (p != parent[p]) {
            p = parent[p] = parent[parent[p]];
        }
        return p;
    }

}