package org.neo4j.graphalgo.core.utils.dss;

/**
 * A structure to manage disjoint sets of ints e.g. nodeIds
 *
 * @author mknblch
 */
public interface DisjointSetStruct {

    /**
     * get the element count
     *
     * @return the element count
     */
    int count();

    /**
     * find setId of element p.
     * @param p the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    int find(int p);

    /**
     * check if p and q belong to the same set
     * @param p a set item
     * @param q a set item
     * @return true if both items belong to the same set, false otherwise
     */
    boolean connected(int p, int q);

    /**
     * join set of p (Sp) with set of q (Sq)
     *
     * @param p an item of Sp
     * @param q an item of Sq
     */
    void union(int p, int q);
}
