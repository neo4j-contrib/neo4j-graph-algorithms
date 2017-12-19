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
package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedLongLongDoubleMap;

public final class HugeWeightMap implements HugeWeightMapping {

    private PagedLongLongDoubleMap weights;
    private final double defaultValue;

    public HugeWeightMap(long capacity, double defaultValue, AllocationTracker tracker) {
        this.defaultValue = defaultValue;
        this.weights = PagedLongLongDoubleMap.newMap(capacity, tracker);
    }

    @Override
    public double weight(final long source, final long target) {
        return weights.getOrDefault(source, target, defaultValue);
    }

    public void put(long key1, long key2, Object value) {
        double doubleVal = RawValues.extractValue(value, defaultValue);
        if (doubleVal == defaultValue) {
            return;
        }
        weights.put(key1, key2, doubleVal);
    }

    public double defaultValue() {
        return defaultValue;
    }

    public void put(long key1, long key2, double value) {
        weights.put(key1, key2, value);
    }

    @Override
    public long release() {
        if (weights != null) {
            long freed = weights.release();
            weights = null;
            return freed;
        }
        return 0L;
    }
}
