package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.WeightedDegreeCentrality;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;

public class WeightedDegreeComputer implements DegreeComputer {

    private Graph graph;

    public WeightedDegreeComputer(Graph graph) {
        this.graph = graph;
    }

    @Override
    public double[] degree(ExecutorService executor, int concurrency) {
        WeightedDegreeCentrality degreeCentrality = new WeightedDegreeCentrality(graph, executor, concurrency, Direction.OUTGOING);
        degreeCentrality.compute();
        return degreeCentrality.degrees();
    }
}
