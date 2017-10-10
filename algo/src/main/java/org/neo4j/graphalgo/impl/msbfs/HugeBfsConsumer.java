package org.neo4j.graphalgo.impl.msbfs;

@FunctionalInterface
public interface HugeBfsConsumer {

    void accept(long nodeId, int depth, HugeBfsSources sourceNodeIds);
}
