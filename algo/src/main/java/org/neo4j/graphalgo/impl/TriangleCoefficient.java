package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * TriangleCoefficient
 *
 * @author mknblch
 */
public class TriangleCoefficient extends Algorithm<TriangleCoefficient> {

    private final TriangleCount triangleCountAlgorithm;

    private final Graph graph;
    private final int nodeCount;
    private double averageClusteringCoefficient = -1d;

    public TriangleCoefficient(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        triangleCountAlgorithm = new TriangleCount(graph, executorService, concurrency);
    }

    public TriangleCoefficient compute() {
        triangleCountAlgorithm.compute();
        return this;
    }

    public Stream<Result> resultStream() {
        final AtomicIntegerArray triangles = triangleCountAlgorithm.getTriangles();
        return IntStream.range(0, nodeCount)
                .mapToObj(i -> new Result(
                        graph.toOriginalNodeId(i),
                        calculateCoefficient(
                                triangles.get(i),
                                graph.degree(i, TriangleCount.D))));
    }

    public double[] getClusteringCoefficients() {
        final AtomicIntegerArray triangles = triangleCountAlgorithm.getTriangles();
        final double[] coefficient = new double[graph.nodeCount()];
        double sum = 0;
        for (int i = 0; i < nodeCount; i++) {
            final double c = calculateCoefficient(triangles.get(i), graph.degree(i, TriangleCount.D));
            coefficient[i] = c;
            sum += c;
        }
        averageClusteringCoefficient = sum / nodeCount;
        return coefficient;
    }

    public double getAverageClusteringCoefficient() {
        return averageClusteringCoefficient;
    }

    private double calculateCoefficient(int triangles, int degree) {
        if (triangles == 0) {
            return 0.0;
        }
        return (2d * triangles) / (degree * (degree - 1));
    }

    @Override
    public TriangleCoefficient me() {
        return this;
    }

    @Override
    public TriangleCoefficient release() {
        return this;
    }

    public static class Result {

        public final long nodeId;
        public final double clusteringCoefficient;

        public Result(long nodeId, double clusteringCoefficient) {
            this.nodeId = nodeId;
            this.clusteringCoefficient = clusteringCoefficient;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", clusteringCoefficient=" + clusteringCoefficient +
                    '}';
        }
    }
}
