package org.neo4j.graphalgo.api;

/**
 * Impl. of a consumer for two int's.
 *
 * TODO: replace with RelationConsumer once the relationId has been removed
 *
 * @author mknblch
 */
@FunctionalInterface
public interface IntBinaryConsumer {

    void accept(int p, int q);
}
