package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.AverageDegreeCentrality;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;

public class BasicDegreeComputer implements DegreeComputer {
    private Graph graph;

    public BasicDegreeComputer(Graph graph) {
        this.graph = graph;
    }

    @Override
    public DegreeCache degree(ExecutorService executor, int concurrency) {
        AverageDegreeCentrality degreeCentrality = new AverageDegreeCentrality(graph, executor, concurrency, Direction.OUTGOING);
        degreeCentrality.compute();
        return new DegreeCache(new double[0], new double[0][], degreeCentrality.average());
    }
}
