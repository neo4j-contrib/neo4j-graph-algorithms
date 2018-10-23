package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.WeightedDegreeCentrality;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;

public class WeightedDegreeComputer implements DegreeComputer {

    private Graph graph;
    private boolean cacheWeights;

    public WeightedDegreeComputer(Graph graph, boolean cacheWeights) {
        this.graph = graph;
        this.cacheWeights = cacheWeights;
    }

    @Override
    public DegreeCache degree(ExecutorService executor, int concurrency) {
        WeightedDegreeCentrality degreeCentrality = new WeightedDegreeCentrality(graph, executor, concurrency, Direction.OUTGOING);
        degreeCentrality.compute(cacheWeights);
        return new DegreeCache(degreeCentrality.degrees(), degreeCentrality.weights(), -1D);
    }
}
