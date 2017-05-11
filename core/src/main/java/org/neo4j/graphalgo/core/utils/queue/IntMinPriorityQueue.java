package org.neo4j.graphalgo.core.utils.queue;

import com.carrotsearch.hppc.IntDoubleScatterMap;


/**
 * An IntPriorityQueue that holds the smallest value on top and keeps it's own
 * storage of costs associated with the values.
 *
 * @author phorn@avantgarde-labs.de
 */
public final class IntMinPriorityQueue extends IntPriorityQueue {

    private static final int[] EMPTY_INT = new int[0];
    private final IntDoubleScatterMap costs;

    /**
     * Creates a new queue with an initial capacity of {@link #DEFAULT_CAPACITY}.
     */
    public IntMinPriorityQueue() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Creates a new queue with the given capacity.
     * The queue dynamically grows to hold all elements.
     */
    public IntMinPriorityQueue(int initialCapacity) {
        super(initialCapacity);
        this.costs = new IntDoubleScatterMap(initialCapacity);
    }

    @Override
    protected boolean lessThan(int a, int b) {
        return costs.get(a) < costs.get(b);
    }

    @Override
    protected double cost(final int element) {
        return costs.get(element);
    }

    @Override
    protected void addCost(final int element, final double cost) {
        costs.put(element, cost);
    }

    @Override
    protected void elementRemoved(final int element) {
        costs.remove(element);
    }

    @Override
    public void release() {
        super.release();

        // costs.release() reallocated new arrays, but we want to completely
        //   throw away all data.
        // costs.clear() does Arrays.fill(keys, 0), so it's linear to the
        //  current key length, therefore we first set keys to an empty array
        //  and null it afterwards
        costs.keys = EMPTY_INT;
        costs.clear();
        costs.keys = null;
        costs.values = null;
    }
}
