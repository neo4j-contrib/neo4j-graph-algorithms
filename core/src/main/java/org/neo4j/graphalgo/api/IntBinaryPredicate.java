package org.neo4j.graphalgo.api;

/**
 * @author mknblch
 */
@FunctionalInterface
public interface IntBinaryPredicate {

    boolean test(int p, int q);
}
