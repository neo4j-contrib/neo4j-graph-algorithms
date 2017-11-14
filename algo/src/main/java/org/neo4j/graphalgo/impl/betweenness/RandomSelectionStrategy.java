package org.neo4j.graphalgo.impl.betweenness;

import org.neo4j.graphalgo.api.NodeIterator;

import java.security.SecureRandom;
import java.util.function.IntConsumer;

/**
 * @author mknblch
 */
public class RandomSelectionStrategy implements SelectionStrategy {

    private final SecureRandom random;
    private final NodeIterator nodeIterator;
    private final double probability;

    public RandomSelectionStrategy(NodeIterator nodeIterator, double probability) {
        this.nodeIterator = nodeIterator;
        this.probability = probability;
        random = new SecureRandom();
    }

    @Override
    public void forEachNode(IntConsumer nodeConsumer) {
        nodeIterator.forEachNode(node -> {
            if (random.nextDouble() < probability) {
                nodeConsumer.accept(node);
            }
            return true;
        });
    }
}