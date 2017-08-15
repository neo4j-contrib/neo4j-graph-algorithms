package org.neo4j.graphalgo.impl.msbfs;

import org.neo4j.collection.primitive.PrimitiveIntIterator;

public interface BfsSources extends PrimitiveIntIterator {

    int size();

    void reset();
}
