package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.multistepscc.ParallelTraverse;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mknblch
 */
public class ForwardBackwardScc {

    private final ParallelTraverse traverse;
    private final IntSet scc = new IntScatterSet();
    private final Graph graph;

    public ForwardBackwardScc(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        traverse = new ParallelTraverse(graph, executorService, concurrency);
    }

    public ForwardBackwardScc compute(int startNodeId) {
        scc.clear();
        // D <- BFS( G(V,E(V)), v)
        final IntScatterSet descendant = new IntScatterSet();
        traverse.bfs(startNodeId, Direction.OUTGOING, node -> true, descendant::add)
                .awaitTermination();
        // ST <- BFS( G(V, E'(V)), v)
        traverse.reset()
                .bfs(startNodeId, Direction.INCOMING, descendant::contains, scc::add)
                .awaitTermination();
        // SCC <- V & ST
        scc.retainAll(descendant);
        return this;
    }

    public Stream<Result> resultStream() {
        return StreamSupport.stream(scc.spliterator(), false)
                .map(node -> new Result(graph.toOriginalNodeId(node.value)));
    }

    public class Result {
        public final long nodeId;
        public Result(long nodeId) {
            this.nodeId = nodeId;
        }
    }

}
