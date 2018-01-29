package org.neo4j.graphalgo.impl.triangle;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public interface TriangleCountAlgorithm {

    long getTriangleCount();

    double getAverageCoefficient();

    <V> V getTriangles();

    <V> V getCoefficients();

    Stream<Result> resultStream();

    TriangleCountAlgorithm withProgressLogger(ProgressLogger wrap);

    TriangleCountAlgorithm withTerminationFlag(TerminationFlag wrap);

    TriangleCountAlgorithm release();

    TriangleCountAlgorithm compute();

    static double calculateCoefficient(int triangles, int degree) {
        if (triangles == 0) {
            return 0.0;
        }
        return ((double) (triangles << 1)) / (degree * (degree - 1));
    }

    class Result {

        public final long nodeId;
        public final long triangles;

        public final double coefficient;

        public Result(long nodeId, long triangles, double coefficient) {
            this.nodeId = nodeId;
            this.triangles = triangles;
            this.coefficient = coefficient;
        }
        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", triangles=" + triangles +
                    ", coefficient=" + coefficient +
                    '}';
        }

    }

    static TriangleCountAlgorithm instance(Graph graph, ExecutorService pool, int concurrency) {
        if (graph instanceof HugeGraph) {
            return new HugeTriangleCount((HugeGraph) graph, pool, concurrency, AllocationTracker.create());
        } else {
            return new TriangleCountQueue(graph, pool, concurrency);
        }
    }

}
