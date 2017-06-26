package org.neo4j.graphalgo.api;

/**
 * TODO: replace with HPPC iface?
 *
 * @author mknblch
 */
@FunctionalInterface
public interface IntBinaryPredicate {

    boolean test(int p, int q);
}
