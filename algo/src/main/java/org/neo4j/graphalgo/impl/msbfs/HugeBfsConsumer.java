package org.neo4j.graphalgo.impl.msbfs;

@FunctionalInterface
public interface HugeBfsConsumer {

    void accept(long nodeId, long depth, HugeBfsSources sourceNodeIds);
}
