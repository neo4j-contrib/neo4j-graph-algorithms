package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.container.Path;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Implements Betweenness Centrality for unweighted graphs
 * as specified in <a href="http://www.algo.uni-konstanz.de/publications/b-fabc-01.pdf">this paper</a>
 *
 * @author mknblch
 */
public class BetweennessCentrality extends Algorithm<BetweennessCentrality> {

    private Graph graph;

    private double[] centrality;
    private double[] delta;
    private int[] sigma;
    private int[] distance;
    private IntStack stack;
    private IntArrayDeque queue;
    private Path[] paths; // TODO find a better container impl
    private int nodeCount;

    public BetweennessCentrality(Graph graph) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        this.centrality = new double[nodeCount];
        this.stack = new IntStack();
        this.sigma = new int[nodeCount];
        this.distance = new int[nodeCount];
        queue = new IntArrayDeque();
        paths = new Path[nodeCount];
        delta = new double[nodeCount];
    }

    /**
     * compute centrality
     * @return itself for method chaining
     */
    public BetweennessCentrality compute() {
        Arrays.fill(centrality, 0);
        graph.forEachNode(this::compute);
        return this;
    }

    public double[] getCentrality() {
        return centrality;
    }

    /**
     * iterate over each result until every node has
     * been visited or the consumer returns false
     *
     * @param consumer the result consumer
     */
    public void forEach(ResultConsumer consumer) {
        for (int i = nodeCount - 1; i >= 0; i--) {
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

    private boolean compute(int startNode) {
        clearPaths();
        stack.clear();
        queue.clear();
        Arrays.fill(sigma, 0);
        Arrays.fill(delta, 0);
        Arrays.fill(distance, -1);
        sigma[startNode] = 1;
        distance[startNode] = 0;
        queue.addLast(startNode);
        while (!queue.isEmpty() && running()) {
            int node = queue.removeLast();
            stack.push(node);
            graph.forEachRelationship(node, Direction.OUTGOING, (source, target, relationId) -> {
                if (distance[target] < 0) {
                    queue.addLast(target);
                    distance[target] = distance[node] + 1;
                }
                if (distance[target] == distance[node] + 1) {
                    sigma[target] += sigma[node];
                    append(target, node);
                }
                return true;
            });
        }
        while (!stack.isEmpty() && running()) {
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
        getProgressLogger().logProgress((double) startNode / (nodeCount - 1));
        return true;
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

    @Override
    public BetweennessCentrality me() {
        return this;
    }

    @Override
    public BetweennessCentrality release() {
        graph = null;
        centrality = null;
        delta = null;
        sigma = null;
        distance = null;
        stack = null;
        queue = null;
        paths = null;
        return this;
    }

    /**
     * Consumer interface
     */
    public interface ResultConsumer {
        /**
         * consume nodeId and centrality value as long as the consumer returns true
         *
         * @param originalNodeId the neo4j node id
         * @param value centrality value
         * @return a bool indicating if the loop should continue(true) or stop(false)
         */
        boolean consume(long originalNodeId, double value);
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        public final Long nodeId;

        public final Double centrality;

        public Result(Long nodeId, Double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }
    }
}
