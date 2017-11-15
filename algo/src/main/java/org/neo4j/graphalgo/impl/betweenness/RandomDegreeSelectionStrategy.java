package org.neo4j.graphalgo.impl.betweenness;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphdb.Direction;

import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * @author mknblch
 */
public class RandomDegreeSelectionStrategy implements NodeIterator {

    private final SecureRandom random;

    private final NodeIterator nodeIterator;
    private final Degrees degrees;
    private final Direction direction;

    private final int maxDegree;

    public RandomDegreeSelectionStrategy(IdMapping idMapping, NodeIterator nodeIterator, Degrees degrees, ExecutorService pool, int concurrency, Direction direction) {
        this.nodeIterator = nodeIterator;
        this.degrees = degrees;
        this.direction = direction;
        random = new SecureRandom();
        AtomicInteger mx = new AtomicInteger(0);
        ParallelUtil.iterateParallel(pool, Math.toIntExact(idMapping.nodeCount()), concurrency, node -> {
            final int degree = degrees.degree(node, direction);
            int current;
            do {
                current = mx.get();
            } while (degree > current && !mx.compareAndSet(current, degree));
        });
        maxDegree = mx.get();
    }

    @Override
    public void forEachNode(IntPredicate consumer) {
        nodeIterator.forEachNode(node -> {
            if (random.nextDouble() > (double) degrees.degree(node, direction) / maxDegree) {
                if (!consumer.test(node)) {
                    return false;
                }
            }
            return true;
        });
    }

    @Override
    public PrimitiveIntIterator nodeIterator() {
        return null;
    }
}