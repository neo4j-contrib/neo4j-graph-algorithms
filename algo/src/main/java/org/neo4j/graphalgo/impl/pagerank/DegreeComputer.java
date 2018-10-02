package org.neo4j.graphalgo.impl.pagerank;

import java.util.concurrent.ExecutorService;

public interface DegreeComputer {
    double[] degree(ExecutorService executor, int concurrency);
}
