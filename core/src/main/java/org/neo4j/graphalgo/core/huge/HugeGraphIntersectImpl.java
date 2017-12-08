/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.HugeRelationshipConsumer;
import org.neo4j.graphalgo.api.HugeRelationshipIntersect;
import org.neo4j.graphalgo.core.utils.paged.ByteArray;
import org.neo4j.graphalgo.core.utils.paged.LongArray;

class HugeGraphIntersectImpl implements HugeRelationshipIntersect {

    private ByteArray adjacency;
    private LongArray offsets;
    private ByteArray.DeltaCursor empty;
    private ByteArray.DeltaCursor cache;
    private ByteArray.DeltaCursor cacheA;
    private ByteArray.DeltaCursor cacheB;

    HugeGraphIntersectImpl(final ByteArray adjacency, final LongArray offsets) {
        assert adjacency != null;
        assert offsets != null;
        this.adjacency = adjacency;
        this.offsets = offsets;
        cache = adjacency.newCursor();
        cacheA = adjacency.newCursor();
        cacheB = adjacency.newCursor();
        empty = adjacency.newCursor();
    }

    @Override
    public void forEachRelationship(long nodeId, HugeRelationshipConsumer consumer) {
        ByteArray.DeltaCursor cursor = cursor(nodeId, cache, offsets, adjacency);
        consumeNodes(nodeId, cursor, consumer);
    }

    @Override
    public int degree(final long node) {
        return degree(node, offsets, adjacency);
    }

    @Override
    public int intersect(long nodeIdA, long nodeIdB, long[] result, int resultOffset) {
        final ByteArray.DeltaCursor aCursor = cursor(nodeIdA, cacheA, offsets, adjacency);
        final ByteArray.DeltaCursor bCursor = cursor(nodeIdB, cacheB, offsets, adjacency);

        final ByteArray.DeltaCursor lead, follow;
        if (aCursor.cost() <= bCursor.cost()) {
            lead = aCursor;
            follow = bCursor;
        } else {
            lead = bCursor;
            follow = aCursor;
        }

        long s = lead.getVLong();
        while (s < nodeIdB && s != -1L) {
            s = lead.getVLong();
        }
        long t = follow.getVLong();
        int start = resultOffset;

        while (s != -1L && t != -1L) {
            while (t < s && t != -1L) {
                t = follow.getVLong();
            }
            if (t == s) {
                result[resultOffset++] = t;
            }
            s = lead.getVLong();
        }

        return resultOffset - start;
    }

    private int degree(long node, LongArray offsets, ByteArray array) {
        long offset = offsets.get(node);
        if (offset == 0L) {
            return 0;
        }
        return array.getInt(offset);
    }

    private ByteArray.DeltaCursor cursor(
            long node,
            ByteArray.DeltaCursor reuse,
            LongArray offsets,
            ByteArray array) {
        final long offset = offsets.get(node);
        if (offset == 0L) {
            return empty;
        }
        return array.deltaCursor(reuse, offset);
    }

    private void consumeNodes(
            long startNode,
            ByteArray.DeltaCursor cursor,
            HugeRelationshipConsumer consumer) {
        long next;
        //noinspection StatementWithEmptyBody
        while ((next = cursor.getVLong()) != -1L &&
                consumer.accept(startNode, next)) ;
    }
}
