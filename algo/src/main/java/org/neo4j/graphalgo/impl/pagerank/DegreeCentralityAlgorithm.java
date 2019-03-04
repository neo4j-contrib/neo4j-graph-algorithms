package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.impl.results.CentralityResult;

public interface DegreeCentralityAlgorithm {
    CentralityResult result();

    void compute();

    Algorithm<?> algorithm();
}
