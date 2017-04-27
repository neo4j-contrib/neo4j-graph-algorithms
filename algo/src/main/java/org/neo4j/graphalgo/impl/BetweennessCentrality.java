package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.Exporter;
import org.neo4j.graphalgo.core.utils.container.Path;
import org.neo4j.graphalgo.results.BetweennessCentralityProcResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Implements Betweenness Centrality for unweighted graphs
 * as specified in <a href="http://www.algo.uni-konstanz.de/publications/b-fabc-01.pdf">this paper</a>
 *
 * @author mknblch
 */
public class BetweennessCentrality {

    private final Graph graph;

    private final double[] centrality;
    private final double[] delta;
    private final int[] sigma;
    private final int[] distance;
    private final IntStack stack;
    private final IntArrayDeque queue;
    private final Path[] paths; // TODO find a better container impl

    public BetweennessCentrality(Graph graph) {
        this.graph = graph;
        this.centrality = new double[graph.nodeCount()];
        this.stack = new IntStack();
        this.sigma = new int[graph.nodeCount()];
        this.distance = new int[graph.nodeCount()];
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
        Arrays.fill(distance, -1);
        sigma[startNode] = 1;
        distance[startNode] = 0;
        queue.addLast(startNode);
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

    public static class BCExporter extends Exporter<BetweennessCentrality> {

        private int targetPropertyId = -1;

        public BCExporter(GraphDatabaseAPI api) {
            super(api);
        }

        public BCExporter withTargetProperty(String targetProperty) {
            this.targetPropertyId = getOrCreatePropertyId(targetProperty);
            return this;
        }

        @Override
        public void write(BetweennessCentrality bc) {
            writeInTransaction(writeOp -> {
                bc.forEach(((originalNodeId, value) -> {
                    try {
                        writeOp.nodeSetProperty(originalNodeId, Property.doubleProperty(targetPropertyId, value));
                    } catch (EntityNotFoundException
                            | ConstraintValidationKernelException
                            | InvalidTransactionTypeKernelException
                            | AutoIndexingKernelException e) {
                        throw new RuntimeException(e);
                    }
                    return true;
                }));
            });
        }
    }

}
