package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.IntDoubleMap;


/**
 * An IntPriorityQueue that holds the smallest value on top and used a
 * shared map for the costs that are associated with the values.
 *
 * @author phorn@avantgarde-labs.de
 */
public final class SharedIntMinPriorityQueue extends IntPriorityQueue {

    private final IntDoubleMap costs;
    private final double defaultCost;

    /**
     * Creates a new queue with the given capacity.
     * The queue dynamically grows to hold all elements.
     * The costs map is shared with the caller for reads and never modified
     * by the queue itself; it is up to the caller to make sure that the
     * costs are up-to-date.
     * The defaultCost is used in case a value has no entry in the costs map.
     */
    public SharedIntMinPriorityQueue(
            int initialCapacity,
            IntDoubleMap costs,
            double defaultCost) {
        super(initialCapacity);
        this.costs = costs;
        this.defaultCost = defaultCost;
    }

    @Override
    protected boolean lessThan(int a, int b) {
        return costs.getOrDefault(a, defaultCost) < costs.getOrDefault(b, defaultCost);
    }
}
