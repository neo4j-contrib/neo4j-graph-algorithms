package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.function.IntUnaryOperator;

import static org.neo4j.graphalgo.core.ProcedureConstants.DIRECTION;

/**
 * @author mknblch
 */
public class Louvain extends Algorithm<Louvain> {


    private final CommunityGraph communityGraph;
    private final Graph graph;

    public Louvain(Graph graph, ExecutorService executorService, int concurrency, double scaleFactor) {
        this.graph = graph;
        communityGraph = new CommunityGraph(graph);
    }

    public Louvain compute() {
        communityGraph.reset();
        communityGraph.compute();
        return this;
    }

    public int[] getCommunities() {
        return communityGraph.getCommunityIds();
    }

    @Override
    public Louvain me() {
        return this;
    }

    @Override
    public Louvain release() {
        // TODO
        return this;
    }
}
