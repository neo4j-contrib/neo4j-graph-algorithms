package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntUnaryOperator;

/**
 * @author mknblch
 */
public class Louvain extends Algorithm<Louvain> {


    private static final Direction DIRECTION = Direction.OUTGOING;

    private final Graph graph;
    private final int nodeCount;

    private final int[] cluster;
    private final AtomicDoubleArray sIn;
    private final AtomicDoubleArray sTot;
    private final AtomicLong normalizationFactor;
    private final AtomicDoubleArray modularity;
    private final ExecutorService executorService;
    private final int concurrency;
    private final double scaleFactor;

    public Louvain(Graph graph, ExecutorService executorService, int concurrency, double scaleFactor) {

        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.scaleFactor = scaleFactor;
        this.cluster = new int[nodeCount];
        sIn = new AtomicDoubleArray(nodeCount, scaleFactor);
        sTot = new AtomicDoubleArray(nodeCount, scaleFactor);
        modularity = new AtomicDoubleArray(nodeCount, 10_000);
        normalizationFactor = new AtomicLong();
    }

    private void reset() {
        assign(i -> i);
        ParallelUtil.iterateParallel(executorService, nodeCount, 4, n -> {
            sIn.set(n, 0);
            sTot.set(n, 0);
        });
    }

    public Louvain compute() {

        reset();

        ParallelUtil.iterateParallel(executorService, nodeCount, concurrency, node -> {

            graph.forEachRelationship(node, DIRECTION, (sourceNodeId, targetNodeId, relationId, weight) -> {
                final int sourceCommunity = cluster[node];
                final int targetCommunity = cluster[targetNodeId];
                sTot.addCapped(sourceCommunity, weight); // incident edge weights per community
                sTot.addCapped(targetCommunity, weight); // incident edge weights per community
                normalizationFactor.addAndGet((long) (weight * scaleFactor)); // edge count
                if (sourceCommunity == targetCommunity) {
                    sIn.addCapped(sourceCommunity, weight); // internal edge weights per community
                }
                return true;
            });
        });

        return this;
    }

    public Louvain assign(int nodeId, int community) {
        cluster[nodeId] = community;
        return this;
    }

    public Louvain assign(IntUnaryOperator generator) {
        Arrays.parallelSetAll(cluster, generator);
        return this;
    }

    private double getNormFactor() {
        return normalizationFactor.get() / scaleFactor;
    }

    public void print() {
        int max = Math.min(nodeCount, 100);
        System.out.printf("Node | sIn  | sTot%n");
        for (int i = 0; i < max; i++) {
            System.out.printf("%3d\t | %3.2f\t| %3.2f%n", i, sIn.get(i), sTot.get(i));
        }
        System.out.printf("norm: %3.2f%n", getNormFactor());
    }

    @Override
    public Louvain me() {
        return this;
    }

    @Override
    public Louvain release() {
        // TODO
        return this;
    }
}
