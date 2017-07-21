package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.container.MultiQueue;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Implements Betweenness Centrality for unweighted graphs.
 * <p>
 * The algorithm is based on Brandes definition but with some changes
 * regarding the dependency-accumulation step.
 * <p>
 * taken from: http://cass-mt.pnnl.gov/docs/pubs/georgiatechlbnlpnnlfastbc-mtaap2009.pdf
 *
 * @author mknblch
 */
public class BetweennessCentralitySuccessorBrandes extends Algorithm<BetweennessCentralitySuccessorBrandes> {

    // the graph
    private final Graph graph;
    // atomic double array which supports only atomic-add
    private final AtomicDoubleArray centrality;
    // the node count
    private final int nodeCount;
    // global executor service
    private final ExecutorService executorService;

    private final AtomicIntegerArray sigma;
    private AtomicIntegerArray d;
    private final double[] delta;
    private final AtomicInteger count;
    private int phase;
    private MultiQueue successors;
    private MultiQueue phaseQueue;
    private final ArrayList<Future<?>> futures = new ArrayList<>();

    /**
     * constructs a parallel centrality solver
     *
     * @param graph           the graph iface
     * @param scaleFactor     factor used to scale up doubles to integers in AtomicDoubleArray
     * @param executorService the executor service
     */
    public BetweennessCentralitySuccessorBrandes(Graph graph, double scaleFactor, ExecutorService executorService) {
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.executorService = executorService;
        this.centrality = new AtomicDoubleArray(graph.nodeCount(), scaleFactor);
        sigma = new AtomicIntegerArray(nodeCount);
        delta = new double[nodeCount];
        d = new AtomicIntegerArray(nodeCount);
        successors = new MultiQueue(executorService, nodeCount);
        phaseQueue = new MultiQueue(executorService, nodeCount);
        count = new AtomicInteger();
    }

    /**
     * compute centrality
     *
     * @return itself for method chaining
     */
    public BetweennessCentralitySuccessorBrandes compute() {
        graph.forEachNode(this::compute);
        return this;
    }

    private boolean compute(int startNodeId) {

        // initialization

        final AtomicIntegerArray offsets = phaseQueue.getOffsets();

        ParallelUtil.iterateParallel(executorService, nodeCount, 4, i -> {
            d.set(i, -1);
            sigma.set(i, 0);
            offsets.set(i, 0);
        });

        phaseQueue.addOrCreate(0, startNodeId);
        d.set(startNodeId, 0);
        sigma.set(startNodeId, 1);

        phase = 0;
        count.set(1);

        // computation
        while (count.getAndSet(0) > 0 && running()) {
            futures.clear();
            phaseQueue.forEach(futures, phase, v -> { // in parallel
                successors.clear(v);
                graph.forEachRelationship(v, Direction.OUTGOING, (sourceNodeId, w, relationId) -> {
                    int dw = d.get(w);
                    d.compareAndSet(w, -1, phase + 1);
                    if (dw == -1) {
                        phaseQueue.addOrCreate(phase + 1, w);
                        count.incrementAndGet();
                        dw = phase + 1;
                    }
                    if (dw == phase + 1) {
                        sigma.addAndGet(w, sigma.get(v));
                        successors.addOrCreate(v, w);
                    }
                    return true;
                });
            });
            ParallelUtil.awaitTermination(futures);
            phase++;
        }

        // back propagation + dependency accumulation
        Arrays.fill(delta, 0d);
        while (--phase > 0 && running()) {
            futures.clear();
            phaseQueue.forEach(futures, phase, w -> {
                final double[] dsw = {0.0};
                final double sw = sigma.get(w);
                successors.forEach(w, v -> dsw[0] += (sw / (double) sigma.get(v)) * (1.0 + delta[v]));
                delta[w] = dsw[0];
                centrality.add(w, dsw[0]);
            });
            ParallelUtil.awaitTermination(futures);
        }

        return true;
    }

    /**
     * get the centrality array
     *
     * @return array with centrality
     */
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

    /**
     * emit the result stream
     *
     * @return stream if Results
     */
    public Stream<BetweennessCentrality.Result> resultStream() {
        return IntStream.range(0, graph.nodeCount())
                .mapToObj(nodeId ->
                        new BetweennessCentrality.Result(
                                graph.toOriginalNodeId(nodeId),
                                centrality.get(nodeId)));
    }


    @Override
    public BetweennessCentralitySuccessorBrandes me() {
        return this;
    }
}
