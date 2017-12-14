package org.neo4j.graphalgo.api;

@FunctionalInterface
public interface IntersectionConsumer {
    void accept(long nodeA, long nodeB, long nodeC);
}
