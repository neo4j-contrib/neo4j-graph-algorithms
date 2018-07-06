package org.neo4j.graphalgo.impl.walking;

import java.util.stream.Stream;

public abstract class AbstractWalkOutput {
    public AbstractWalkOutput(){
    }

    public abstract void endInput();

    public abstract void addResult(long[] result);

    public Stream<WalkResult> getStream() {
        return Stream.empty();
    }

    public abstract int numberOfResults();
}
