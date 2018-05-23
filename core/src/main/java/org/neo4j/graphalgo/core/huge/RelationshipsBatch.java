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
package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphdb.Direction;

import java.util.concurrent.ArrayBlockingQueue;

final class RelationshipsBatch implements AutoCloseable {
    static final RelationshipsBatch SENTINEL = new RelationshipsBatch(null);

    private static final int D_OUT = 2;
    private static final int D_IN = 4;
    private static final int D_DIR = D_OUT | D_IN;
    private static final int D_RELATIONSHIP = 8;
    private static final int D_WEIGHT = 16;
    private static final int D_DATA = D_RELATIONSHIP | D_WEIGHT;

    static final int JUST_RELATIONSHIPS = D_RELATIONSHIP;
    static final int JUST_WEIGHTS = D_WEIGHT;
    static final int RELS_AND_WEIGHTS = D_RELATIONSHIP | D_WEIGHT;

    long[] sourceTargetIds;
    int length;

    private final ArrayBlockingQueue<RelationshipsBatch> pool;
    private int dataFlags;

    RelationshipsBatch(final ArrayBlockingQueue<RelationshipsBatch> pool) {
        this.pool = pool;
    }

    void setInfo(Direction direction, int baseFlags) {
        dataFlags = baseFlags | (direction == Direction.OUTGOING ? D_OUT : D_IN);
    }

    boolean isOut() {
        return (dataFlags & D_DIR) == D_OUT;
    }

    boolean isIn() {
        return (dataFlags & D_DIR) == D_IN;
    }

    int dataFlag() {
        return dataFlags & D_DATA;
    }

    @Override
    public void close() {
        if (pool != null) {
            returnToPool();
        }
    }

    private void returnToPool() {
        pool.add(this);
    }
}
