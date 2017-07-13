package org.neo4j.graphalgo.impl.msbfs;

@FunctionalInterface
public interface BfsConsumer {

    void accept(int nodeId, int depth, BfsSources sourceNodeIds);
}
