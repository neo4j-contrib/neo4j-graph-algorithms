package org.neo4j.graphalgo.impl.closeness;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.Exporter;

import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public interface HarmonicCentralityAlgorithm {

    HarmonicCentralityAlgorithm compute();

    Stream<Result> resultStream();

    HarmonicCentralityAlgorithm withProgressLogger(ProgressLogger wrap);

    HarmonicCentralityAlgorithm withTerminationFlag(TerminationFlag wrap);

    HarmonicCentralityAlgorithm release();

    void export(final String propertyName, final Exporter exporter);

    static HarmonicCentralityAlgorithm instance(Graph graph, AllocationTracker tracker, ExecutorService pool, int concurrency) {
        if (graph instanceof HugeGraph) {
            return new HugeHarmonicCentrality((HugeGraph) graph,
                    tracker,
                    concurrency,
                    pool);
        }
        return new HarmonicCentrality(graph,
                concurrency, pool);
    }

    /**
     * Result class used for streaming
     */
    final class Result {

        public final long nodeId;
        public final double centrality;

        public Result(long nodeId, double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", centrality=" + centrality +
                    '}';
        }
    }
}
