package org.neo4j.graphalgo.impl.pagerank;

import java.util.concurrent.ExecutorService;

public class NoOpDegreeComputer implements DegreeComputer {

    @Override
    public double[] degree(ExecutorService executor, int concurrency) {
        return new double[0];
    }
}
