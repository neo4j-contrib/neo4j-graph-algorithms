package org.neo4j.graphalgo.core.utils.dss;

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;

/**
 * @author mknblch
 */
public final class DisjointSetStruct {

    private final int[] parent;
    private final int[] depth;

    /**
     * @param capacity the capacity (maximum node id)
     */
    public DisjointSetStruct(int capacity) {
        parent = new int[capacity];
        depth = new int[capacity];
        for (int i = 0; i < capacity; i++) {
            parent[i] = i;
        }
    }

    /**
     * element count
     * @return the element count
     */
    public int count() {
        return parent.length;
    }


    /**
     * find setId of element p.
     * @param p the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    public int find(int p) {
        if (parent[p] == p) return p;
        // path compression optimization
        parent[p] = find(parent[p]); // balance tree while traversing
        return parent[p];
    }

    /**
     * not-optimized implementation (does not change the tree during execution)
     */
    public int findNoOpt(int p) {
        while (p != parent[p]) {
            p = parent[parent[p]];
        }
        return p;
    }

    /**
     * Non-recursive implementation using path-halving optimization.
     */
    public int findPH(int p) {
        while (p != parent[p]) {
            p = parent[p] = parent[parent[p]];
        }
        return p;
    }

    /**
     * find-impl using a recursive path compression logic
     */
    public int findPC(int p) {
        if (parent[p] == p) return p;
        // path compression optimization
        parent[p] = find(parent[p]); // balance tree while traversing
        return parent[p];
    }

    /**
     * check if p and q belong to the same set
     * @param p a set item
     * @param q a set item
     * @return true if both items belong to the same set, false otherwise
     */
    public boolean connected(int p, int q) {
        return find(p) == find(q);
    }


    /**
     * join set of p (Sp) with set of q (Sq)
     *
     * @param p an item of Sp
     * @param q an item of Sq
     */
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

    public IntIntMap getSetSize() {
        final IntIntScatterMap map = new IntIntScatterMap();
        for (int i = parent.length - 1; i >= 0; i--) {
            map.addTo(find(i), 1);
        }
        return map;
    }
}
