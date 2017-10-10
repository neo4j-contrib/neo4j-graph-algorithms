package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.LongToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Normalized Closeness Centrality
 *
 * @author mknblch
 */
public class MSClosenessCentrality extends MSBFSCCAlgorithm<MSClosenessCentrality> {

    private Graph graph;
    private AtomicIntegerArray farness;

    private final int concurrency;
    private final ExecutorService executorService;
    private final int nodeCount;

    public MSClosenessCentrality(Graph graph, int concurrency, ExecutorService executorService) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.concurrency = concurrency;
        this.executorService = executorService;
        farness = new AtomicIntegerArray(nodeCount);
    }

    @Override
    public MSClosenessCentrality compute() {

        final ProgressLogger progressLogger = getProgressLogger();

        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            int len = sourceNodeIds.size();
            farness.addAndGet(nodeId, len * depth);
            progressLogger.logProgress((double) nodeId / (nodeCount - 1));
        };

        new MultiSourceBFS(graph, graph, Direction.OUTGOING, consumer)
                .run(concurrency, executorService);

        return this;
    }

    @Override
    public Stream<Result> resultStream() {
        final double k = nodeCount - 1;
        return IntStream.range(0, nodeCount)
                .mapToObj(nodeId -> new Result(
                        graph.toOriginalNodeId(nodeId),
                        centrality(farness.get(nodeId), k)));
    }

    @Override
    public LongToIntFunction farness() {
        return (i) -> farness.get((int) i);
    }

    @Override
    public void export(final String propertyName, final Exporter exporter) {
        final double k = nodeCount - 1;
        exporter.write(
                propertyName,
                farness,
                (PropertyTranslator.OfDouble<AtomicIntegerArray>)
                        (data, nodeId) -> centrality(data.get((int) nodeId), k));
    }

    @Override
    public MSClosenessCentrality me() {
        return this;
    }

    @Override
    public MSClosenessCentrality release() {
        graph = null;
        farness = null;
        return this;
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
