package org.neo4j.graphalgo.impl.betweenness;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;

import java.security.SecureRandom;
import java.util.function.IntConsumer;

/**
 * @author mknblch
 */
public class RandomSelectionStrategy implements RABrandesBetweennessCentrality.SelectionStrategy {

    private final SimpleBitSet bitSet;
    private final int size;

    public RandomSelectionStrategy(Graph graph) {
        this(graph, Math.log10(graph.nodeCount()) / Math.exp(2));
    }

    public RandomSelectionStrategy(Graph graph, double probability) {
        bitSet = new SimpleBitSet(Math.toIntExact(graph.nodeCount()));
        final SecureRandom random = new SecureRandom();
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (random.nextDouble() <= probability) {
                bitSet.put(i);
            }
        }
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