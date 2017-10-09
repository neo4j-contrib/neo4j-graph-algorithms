package org.neo4j.graphalgo.impl;

import java.util.stream.Stream;

public abstract class MSBFSASPAlgorithm<ME extends MSBFSASPAlgorithm<ME>> extends Algorithm<ME> {

    public abstract Stream<AllShortestPaths.Result> resultStream();
}
