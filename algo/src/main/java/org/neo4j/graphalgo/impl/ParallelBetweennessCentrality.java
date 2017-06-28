package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.container.Paths;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Implements Betweenness Centrality for unweighted graphs
 * as specified in <a href="http://www.algo.uni-konstanz.de/publications/b-fabc-01.pdf">this paper</a>
 * using node-partitioning and atomic distance array
 *
 * @author mknblch
 */
public class ParallelBetweennessCentrality {

    private final Graph graph;
    private volatile AtomicInteger nodeQueue = new AtomicInteger();
    private final AtomicDoubleArray centrality;
    private final int nodeCount;
    private final ExecutorService executorService;
    private final int concurrency;

    public ParallelBetweennessCentrality(Graph graph, double scaleFactor, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.centrality = new AtomicDoubleArray(graph.nodeCount(), scaleFactor);
        // init nodeQAI to the highest nodeId
        nodeQueue.set(0);
    }

    /**
     * compute centrality
     * @return itself for method chaining
     */
    public ParallelBetweennessCentrality compute() {
        final ArrayList<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            futures.add(executorService.submit(new BCTask()));
        }
        ParallelUtil.awaitTermination(futures);
        return this;
    }


    public AtomicDoubleArray getCentrality() {
        return centrality;
    }

    /**
     * iterate over each result until every node has
     * been visited or the consumer returns false
     *
     * @param consumer the result consumer
     */
    public void forEach(BetweennessCentrality.ResultConsumer consumer) {
        for (int i = graph.nodeCount() - 1; i >= 0; i--) {
            if (!consumer.consume(graph.toOriginalNodeId(i), centrality.get(i))) {
                return;
            }
        }
    }

    public Stream<BetweennessCentrality.Result> resultStream() {
        return IntStream.range(0, graph.nodeCount())
                .mapToObj(nodeId ->
                        new BetweennessCentrality.Result(
                                graph.toOriginalNodeId(nodeId),
                                centrality.get(nodeId)));
    }

    private class BCTask implements Runnable {

        private final Paths paths;
        private final double[] delta;
        private final int[] sigma;
        private final int[] distance;
        private final IntStack stack;
        private final IntArrayDeque queue;

        private BCTask() {
            this.paths = new Paths();
            this.stack = new IntStack();
            this.sigma = new int[nodeCount];
            this.distance = new int[graph.nodeCount()];
            this.queue = new IntArrayDeque();
            this.delta = new double[graph.nodeCount()];

        }

        @Override
        public void run() {
            for (;;) {
                reset();
                final int startNodeId = nodeQueue.getAndIncrement();
                if (startNodeId >= nodeCount) {
                    return;
                }
                sigma[startNodeId] = 1;
                distance[startNodeId] = 0;
                queue.addLast(startNodeId);
                while (!queue.isEmpty()) {
                    int node = queue.removeLast();
                    stack.push(node);
                    graph.forEachRelationship(node, Direction.OUTGOING, (source, target, relationId) -> {
                        if (distance[target] < 0) {
                            queue.addLast(target);
                            distance[target] = distance[node] + 1;
                        }
                        if (distance[target] == distance[node] + 1) {
                            sigma[target] += sigma[node];
                            paths.append(target, node);
                        }
                        return true;
                    });
                }
                while (!stack.isEmpty()) {
                    final int node = stack.pop();
                    paths.forEach(node, v -> {
                        delta[v] += (double) sigma[v] / (double) sigma[node] * (delta[node] + 1.0);
                        if (node != startNodeId) {
                            centrality.add(node, delta[node]);
                        }
                        return true;
                    });
                }
            }
        }

        private void reset() {
            paths.clear();
            stack.clear();
            queue.clear();
            Arrays.fill(sigma, 0);
            Arrays.fill(delta, 0);
            Arrays.fill(distance, -1);
        }
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        public final long nodeId;

        public final double centrality;

        public Result(Long nodeId, Double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }
    }
}
