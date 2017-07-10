package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 *
 * Normalized Closeness Centrality
 *
 * @author mknblch
 */
public class MSClosenessCentrality {

    private final Graph graph;

    private final ExecutorService executorService;

    private final AtomicIntegerArray farness;

    private final int nodeCount;

    public MSClosenessCentrality(Graph graph, ExecutorService executorService) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        this.executorService = executorService;
        farness = new AtomicIntegerArray(nodeCount);
    }

    public MSClosenessCentrality compute() {

        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            // TODO size of sourceNodeIds
            while (sourceNodeIds.hasNext()) {
                sourceNodeIds.next();
                farness.addAndGet(nodeId, depth);
            }
        };

        new MultiSourceBFS(graph, graph, Direction.OUTGOING, consumer)
                .run(executorService);

        return this;
    }

    public AtomicIntegerArray getFarness() {
        return farness;
    }

    public double[] getCentrality() {
        final int k = nodeCount - 1;
        final double[] centrality = new double[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            centrality[i] = k / (double) farness.get(i);
        }
        return centrality;
    }

    public Stream<Result> resultStream() {
        final double k = nodeCount - 1;
        return IntStream.range(0, graph.nodeCount())
                .mapToObj(nodeId ->
                        new Result(
                                graph.toOriginalNodeId(nodeId),
                                k / farness.get(nodeId)));
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        public final long nodeId;

        public final double centrality;

        public Result(long nodeId, double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }
    }
}
