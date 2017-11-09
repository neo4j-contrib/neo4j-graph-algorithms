/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
