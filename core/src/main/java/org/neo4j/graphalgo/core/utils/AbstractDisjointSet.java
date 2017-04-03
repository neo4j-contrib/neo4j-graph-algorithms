package org.neo4j.graphalgo.core.utils;

/**
 * @author mknblch
 */
abstract class AbstractDisjointSet implements DisjointSetStruct {

    protected final int[] parent;
    protected final int[] depth;

    /**
     * @param capacity the capacity (maximum node id)
     */
    AbstractDisjointSet(int capacity) {
        parent = new int[capacity];
        depth = new int[capacity];
        for (int i = 0; i < capacity; i++) {
            parent[i] = i;
        }
    }

    /**
     * check if p and q belong to the same set
     * @param p a set item
     * @param q a set item
     * @return true if both items belong to the same set, false otherwise
     */
    @Override
    public boolean connected(int p, int q) {
        return find(p) == find(q);
    }

    /**
     * join set of p (Sp) with set of q (Sq)
     *
     * @param p an item of Sp
     * @param q an item of Sq
     */
    @Override
    public void union(int p, int q) {
        final int pSet = find(p);
        final int qSet = find(q);
        if (pSet == qSet) {
            return;
        }
        // weighted union rule optimization
        int dq = depth[qSet];
        int dp = depth[pSet];
        if (dp < dq) {
            // attach the smaller tree to the root of the bigger tree
            parent[pSet] = qSet;
        } else if (dp > dq) {
            parent[qSet] = pSet;
        } else {
            parent[qSet] = pSet;
            depth[pSet] += depth[qSet] + 1;
        }
    }
}
