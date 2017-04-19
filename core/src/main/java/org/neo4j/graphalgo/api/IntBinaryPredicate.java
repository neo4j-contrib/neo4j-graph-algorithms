package org.neo4j.graphalgo.api;

/**
 * @author mknblch
 *         added 20.04.2017.
 */
@FunctionalInterface
public interface IntBinaryPredicate {

    boolean test(int p, int q);
}
