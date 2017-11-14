package org.neo4j.graphalgo.impl.betweenness;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphdb.Direction;

import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntConsumer;

/**
 * @author mknblch
 */
public class RandomMeanDegreeSelectionStrategy implements SelectionStrategy {

    private final SecureRandom random;

    private final NodeIterator nodeIterator;
    private final Degrees degrees;
    private final Direction direction;
    private final double meanDegree;

    public RandomMeanDegreeSelectionStrategy(IdMapping idMapping, NodeIterator nodeIterator, Degrees degrees, ExecutorService pool, int concurrency, Direction direction) {
        this.nodeIterator = nodeIterator;
        this.degrees = degrees;
        this.direction = direction;
        random = new SecureRandom();
        LongAdder adder = new LongAdder();
        ParallelUtil.iterateParallel(pool, Math.toIntExact(idMapping.nodeCount()), concurrency, node -> {
            adder.add(degrees.degree(node, direction));
        });
        meanDegree = adder.doubleValue() / idMapping.nodeCount();
    }


    @Override
    public void forEachNode(IntConsumer nodeConsumer) {
        nodeIterator.forEachNode(node -> {
            final int degree = degrees.degree(node, direction);
            if (degree >= meanDegree) {
                nodeConsumer.accept(node);
            }
            return true;
        });
    }
}