package org.neo4j.graphalgo.impl.msbfs;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

public interface HugeBfsSources extends PrimitiveLongIterator {

    int size();

    void reset();
}
