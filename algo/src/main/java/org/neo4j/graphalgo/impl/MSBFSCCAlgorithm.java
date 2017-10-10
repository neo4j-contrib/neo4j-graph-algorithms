package org.neo4j.graphalgo.impl;

import java.util.stream.Stream;

public abstract class MSBFSCCAlgorithm<ME extends MSBFSCCAlgorithm<ME>> extends Algorithm<ME> {

    public abstract Stream<MSClosenessCentrality.Result> resultStream();
}
