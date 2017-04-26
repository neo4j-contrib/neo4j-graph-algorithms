package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.container.Path;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Implements Betweenness Centrality for unweighted graphs
 * as specified in <a href=http://www.algo.uni-konstanz.de/publications/b-fabc-01.pdf">this paper</a>
 *
 * @author mknblch
 */
public class BetweennessCentrality {

    private final Graph graph;

    private final double[] centrality;
    private final double[] delta;
    private final int[] sigma;
    private final int[] d;
    private final IntStack stack;
    private final IntArrayDeque queue;
    private final Path[] paths;

    public BetweennessCentrality(Graph graph) {
        this.graph = graph;
        this.centrality = new double[graph.nodeCount()];
        this.stack = new IntStack();
        this.sigma = new int[graph.nodeCount()];
        this.d = new int[graph.nodeCount()];
        queue = new IntArrayDeque();
        paths = new Path[graph.nodeCount()];
        delta = new double[graph.nodeCount()];
    }

    /**
     * compute centrality
     * @return itself for method chaining
     */
    public BetweennessCentrality compute() {
        graph.forEachNode(this::compute);
        return this;
    }

    /**
     * iterate over each result until every node has
     * been visited or the consumer returns false
     *
     * @param consumer the result consumer
     */
    public void forEach(ResultConsumer consumer) {
        for (int i = graph.nodeCount() - 1; i >= 0; i--) {
            if (!consumer.consume(graph.toOriginalNodeId(i), centrality[i])) {
                return;
            }
        }
    }

    public Stream<Result> resultStream() {
        return IntStream.range(0, graph.nodeCount())
                .mapToObj(nodeId ->
                        new Result(
                                graph.toOriginalNodeId(nodeId),
                                centrality[nodeId]));
    }

    private void compute(int startNode) {
        clearPaths();
        stack.clear();
        queue.clear();
        Arrays.fill(sigma, 0);
        Arrays.fill(delta, 0);
        Arrays.fill(d, -1);
        sigma[startNode] = 1;
        d[startNode] = 0;
        queue.addLast(startNode);
        while (!queue.isEmpty()) {
            int node = queue.removeLast();
            stack.push(node);
            graph.forEachRelationship(node, Direction.OUTGOING, (source, target, relationId) -> {
                if (d[target] < 0) {
                    queue.addLast(target);
                    d[target] = d[node] + 1;
                }
                if (d[target] == d[node] + 1) {
                    sigma[target] += sigma[node];
                    append(target, node);
                }
                return true;
            });
        }
        while (!stack.isEmpty()) {
            final int node = stack.pop();
            if (null == paths[node]) {
                continue;
            }
            paths[node].forEach(v -> {
                delta[v] += (double) sigma[v] / (double) sigma[node] * (delta[node] + 1.0);
                if (node != startNode) {
                    centrality[node] += delta[node];
                }
                return true;
            });
        }
    }

    /**
     * append nodeId to path
     *
     * @param path the selected path
     * @param nodeId the node id
     */
    private void append(int path, int nodeId) {
        if (null == paths[path]) {
            paths[path] = new Path();
        }
        paths[path].append(nodeId);

    }

    private void clearPaths() {
        for (Path path : paths) {
            if (null == path) {
                continue;
            }
            path.clear();
        }
    }

    public interface ResultConsumer {
        boolean consume(long originalNodeId, double value);
    }

    public static final class Result {

        public final Long nodeId;

        public final Double value;

        public Result(Long nodeId, Double value) {
            this.nodeId = nodeId;
            this.value = value;
        }
    }
}
