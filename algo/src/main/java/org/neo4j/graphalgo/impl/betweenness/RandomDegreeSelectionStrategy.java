package org.neo4j.graphalgo.impl.betweenness;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphdb.Direction;

import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author mknblch
 */
public class RandomDegreeSelectionStrategy implements RABrandesBetweennessCentrality.SelectionStrategy {

    private final Degrees degrees;
    private final Direction direction;
    private final int maxDegree;
    private final SimpleBitSet bitSet;
    private final int size;

    public RandomDegreeSelectionStrategy(Direction direction, Graph graph, ExecutorService pool, int concurrency) {
        this.degrees = graph;
        this.direction = direction;
        bitSet = new SimpleBitSet(Math.toIntExact(graph.nodeCount()));
        final SecureRandom random = new SecureRandom();
        final AtomicInteger mx = new AtomicInteger(0);
        ParallelUtil.iterateParallel(pool, Math.toIntExact(graph.nodeCount()), concurrency, node -> {
            final int degree = degrees.degree(node, direction);
            int current;
            do {
                current = mx.get();
            } while (degree > current && !mx.compareAndSet(current, degree));
        });
        maxDegree = mx.get();
        ParallelUtil.iterateParallel(pool, Math.toIntExact(graph.nodeCount()), concurrency, node -> {
            if (random.nextDouble() <= degrees.degree(node, direction) / maxDegree) {
                bitSet.put(node);
            }
        });
        size = bitSet.size();
    }

    @Override
    public boolean select(int nodeId) {
        return bitSet.contains(nodeId);
    }

    @Override
    public int size() {
        return size;
    }

}