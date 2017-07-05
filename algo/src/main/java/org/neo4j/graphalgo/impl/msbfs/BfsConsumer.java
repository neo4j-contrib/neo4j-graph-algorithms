package org.neo4j.graphalgo.impl.msbfs;

import org.neo4j.collection.primitive.PrimitiveIntIterator;

@FunctionalInterface
public interface BfsConsumer {

    void accept(int nodeId, int depth, PrimitiveIntIterator sourceNodeIds);
}
