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

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeRelationshipConsumer;
import org.neo4j.graphalgo.api.HugeRelationshipIntersect;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.ByteArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.function.LongPredicate;

/**
 * Huge Graph contains two array like data structures.
 * <p>
 * The adjacency data is stored in a ByteArray, which is a byte[] addressable by
 * longs indices and capable of storing about 2^46 (~ 70k bn) bytes – or 64 TiB.
 * The bytes are stored in byte[] pages of 32 KiB size.
 * <p>
 * The data is in the format:
 * <blockquote>
 * <code>degree</code> ~ <code>targetId</code><sub><code>1</code></sub> ~ <code>targetId</code><sub><code>2</code></sub> ~ <code>targetId</code><sub><code>n</code></sub>
 * </blockquote>
 * The {@code degree} is stored as a fill-sized 4 byte long {@code int}
 * (the neo kernel api returns an int for {@link org.neo4j.kernel.api.ReadOperations#nodeGetDegree(long, Direction)}).
 * Every target ID is first sorted, then delta encoded, and finally written as variable-length vlongs.
 * The delta encoding does not write the actual value but only the difference to the previous value, which plays very nice with the vlong encoding.
 * <p>
 * The seconds data structure is a LongArray, which is a long[] addressable by longs
 * and capable of storing about 2^43 (~9k bn) longs – or 64 TiB worth of 64 bit longs.
 * The data is the offset address into the aforementioned adjacency array, the index is the respective source node id.
 * <p>
 * To traverse all nodes, first access to offset from the LongArray, then read
 * 4 bytes into the {@code degree} from the ByteArray, starting from the offset, then read
 * {@code degree} vlongs as targetId.
 * <p>
 * <p>
 * The graph encoding (sans delta+vlong) is similar to that of the
 * {@link org.neo4j.graphalgo.core.lightweight.LightGraph} but stores degree
 * explicitly into the target adjacency array where the LightGraph would subtract
 * offsets of two consecutive nodes. While that doesn't use up memory to store the
 * degree, it makes it practically impossible to build the array out-of-order,
 * which is necessary for loading the graph in parallel.
 * <p>
 * Reading the degree from the offset position not only does not require the offset array
 * to be sorted but also allows the adjacency array to be sparse. This fact is
 * used during the import – each thread pre-allocates a local chunk of some pages (512 KiB)
 * and gives access to this data during import. Synchronization between threads only
 * has to happen when a new chunk has to be pre-allocated. This is similar to
 * what most garbage collectors do with TLAB allocations.
 *
 * @see <a href="https://developers.google.com/protocol-buffers/docs/encoding#varints">more abount vlong</a>
 * @see <a href="https://shipilev.net/jvm-anatomy-park/4-tlab-allocation/">more abount TLAB allocation</a>
 */
public class HugeGraphImpl implements HugeGraph {

    private final HugeIdMap idMapping;
    private final AllocationTracker tracker;

    private HugeWeightMapping weights;
    private ByteArray inAdjacency;
    private ByteArray outAdjacency;
    private HugeLongArray inOffsets;
    private HugeLongArray outOffsets;
    private ByteArray.DeltaCursor empty;
    private ByteArray.DeltaCursor inCache;
    private ByteArray.DeltaCursor outCache;
    private boolean canRelease = true;

    HugeGraphImpl(
            final AllocationTracker tracker,
            final HugeIdMap idMapping,
            final HugeWeightMapping weights,
            final ByteArray inAdjacency,
            final ByteArray outAdjacency,
            final HugeLongArray inOffsets,
            final HugeLongArray outOffsets) {
        this.idMapping = idMapping;
        this.tracker = tracker;
        this.weights = weights;
        this.inAdjacency = inAdjacency;
        this.outAdjacency = outAdjacency;
        this.inOffsets = inOffsets;
        this.outOffsets = outOffsets;
        inCache = newCursor(this.inAdjacency);
        outCache = newCursor(this.outAdjacency);
        empty = inCache == null ? newCursor(this.outAdjacency) : newCursor(this.inAdjacency);
    }

    @Override
    public long nodeCount() {
        return idMapping.nodeCount();
    }

    @Override
    public Collection<PrimitiveLongIterable> hugeBatchIterables(final int batchSize) {
        return idMapping.hugeBatchIterables(batchSize);
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        idMapping.forEachNode(consumer);
    }

    @Override
    public PrimitiveLongIterator hugeNodeIterator() {
        return idMapping.hugeNodeIterator();
    }

    @Override
    public double weightOf(final long sourceNodeId, final long targetNodeId) {
        return weights.weight(sourceNodeId, targetNodeId);
    }

    @Override
    public void forEachRelationship(long nodeId, Direction direction, HugeRelationshipConsumer consumer) {
        switch (direction) {
            case INCOMING:
                forEachIncoming(nodeId, consumer);
                return;

            case OUTGOING:
                forEachOutgoing(nodeId, consumer);
                return;

            case BOTH:
                forEachOutgoing(nodeId, consumer);
                forEachIncoming(nodeId, consumer);
                return;

            default:
                throw new IllegalArgumentException(direction + "");
        }
    }

    @Override
    public void forEachRelationship(int nodeId, Direction direction, RelationshipConsumer consumer) {
        switch (direction) {
            case INCOMING:
                forEachIncoming(nodeId, consumer);
                return;

            case OUTGOING:
                forEachOutgoing(nodeId, consumer);
                return;

            case BOTH:
                forEachOutgoing(nodeId, consumer);
                forEachIncoming(nodeId, consumer);
                return;

            default:
                throw new IllegalArgumentException(direction + "");
        }
    }

    @Override
    public void forEachRelationship(int nodeId, Direction direction, WeightedRelationshipConsumer consumer) {
        switch (direction) {
            case INCOMING:
                forEachIncoming(nodeId, consumer);
                return;

            case OUTGOING:
                forEachOutgoing(nodeId, consumer);
                return;

            case BOTH:
                forEachOutgoing(nodeId, consumer);
                forEachIncoming(nodeId, consumer);
                return;

            default:
                throw new IllegalArgumentException(direction + "");
        }
    }

    @Override
    public int degree(
            final long node,
            final Direction direction) {
        switch (direction) {
            case INCOMING:
                return degree(node, inOffsets, inAdjacency);

            case OUTGOING:
                return degree(node, outOffsets, outAdjacency);

            case BOTH:
                return degree(node, inOffsets, inAdjacency) + degree(
                        node,
                        outOffsets,
                        outAdjacency);

            default:
                throw new IllegalArgumentException(direction + "");
        }
    }

    @Override
    public long toHugeMappedNodeId(long nodeId) {
        return idMapping.toHugeMappedNodeId(nodeId);
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return idMapping.toOriginalNodeId(nodeId);
    }

    @Override
    public boolean contains(final long nodeId) {
        return idMapping.contains(nodeId);
    }

    @Override
    public void forEachIncoming(long node, final HugeRelationshipConsumer consumer) {
        forEachIncoming(node, inCache, consumer);
    }

    @Override
    public void forEachIncoming(int nodeId, RelationshipConsumer consumer) {
        forEachIncoming((long) nodeId, inAdjacency.newCursor(), toHugeInConsumer(consumer));
    }

    public void forEachIncoming(int nodeId, WeightedRelationshipConsumer consumer) {
        forEachIncoming((long) nodeId, inAdjacency.newCursor(), toHugeInConsumer(consumer));
    }

    private void forEachIncoming(long node, ByteArray.DeltaCursor newCursor, final HugeRelationshipConsumer consumer) {
        ByteArray.DeltaCursor cursor = cursor(node, newCursor, inOffsets, inAdjacency);
        consumeNodes(node, cursor, consumer);
    }

    @Override
    public void forEachOutgoing(long node, final HugeRelationshipConsumer consumer) {
        forEachOutgoing(node, outCache, consumer);
    }

    @Override
    public void forEachOutgoing(int nodeId, RelationshipConsumer consumer) {
        forEachOutgoing((long) nodeId, outAdjacency.newCursor(), toHugeOutConsumer(consumer));
    }

    public void forEachOutgoing(int nodeId, WeightedRelationshipConsumer consumer) {
        forEachOutgoing((long) nodeId, outAdjacency.newCursor(), toHugeOutConsumer(consumer));
    }

    private void forEachOutgoing(long node, ByteArray.DeltaCursor newCursor, final HugeRelationshipConsumer consumer) {
        ByteArray.DeltaCursor cursor = cursor(node, newCursor, outOffsets, outAdjacency);
        consumeNodes(node, cursor, consumer);
    }

    @Override
    public HugeGraph concurrentCopy() {
        return new HugeGraphImpl(
                tracker,
                idMapping,
                weights,
                inAdjacency,
                outAdjacency,
                inOffsets,
                outOffsets
        );
    }

    @Override
    public HugeRelationshipIntersect intersectionCopy() {
        return new HugeGraphIntersectImpl(outAdjacency, outOffsets);
    }

    /**
     * O(n) !
     */
    @Override
    public boolean exists(long sourceNodeId, long targetNodeId, Direction direction) {
        final boolean[] found = {false};
        switch (direction) {
            case OUTGOING:
                forEachOutgoing(sourceNodeId, (s, t) -> {
                    if (t == targetNodeId) {
                        found[0] = true;
                        return false;
                    }
                    return true;
                });
            case INCOMING:
                forEachIncoming(sourceNodeId, (s, t) -> {
                    if (t == targetNodeId) {
                        found[0] = true;
                        return false;
                    }
                    return true;
                });
            default:
                forEachRelationship(sourceNodeId, Direction.BOTH, (s, t) -> {
                    if (t == targetNodeId) {
                        found[0] = true;
                        return false;
                    }
                    return true;
                });
        }
        return found[0];
    }

    @Override
    public boolean exists(int sourceNodeId, int targetNodeId, Direction direction) {
        return exists((long) sourceNodeId, (long) targetNodeId, direction);
    }

    @Override
    public void canRelease(boolean canRelease) {
        this.canRelease = canRelease;
    }

    @Override
    public void release() {
        if (!canRelease) return;
        if (inAdjacency != null) {
            tracker.remove(inAdjacency.release());
            tracker.remove(inOffsets.release());
            inAdjacency = null;
            inOffsets = null;
        }
        if (outAdjacency != null) {
            tracker.remove(outAdjacency.release());
            tracker.remove(outOffsets.release());
            outAdjacency = null;
            outOffsets = null;
        }
        if (weights != null) {
            tracker.remove(weights.release());
        }
        empty = null;
        inCache = null;
        outCache = null;
        weights = null;
    }

    private ByteArray.DeltaCursor newCursor(final ByteArray adjacency) {
        return adjacency != null ? adjacency.newCursor() : null;
    }

    private int degree(long node, HugeLongArray offsets, ByteArray array) {
        long offset = offsets.get(node);
        if (offset == 0L) {
            return 0;
        }
        return array.getInt(offset);
    }

    private ByteArray.DeltaCursor cursor(
            long node,
            ByteArray.DeltaCursor reuse,
            HugeLongArray offsets,
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
        //noinspection StatementWithEmptyBody
        while (cursor.hasNextVLong() && consumer.accept(startNode, cursor.nextVLong()));
    }

    private HugeRelationshipConsumer toHugeOutConsumer(RelationshipConsumer consumer) {
        return (s, t) -> consumer.accept(
                (int) s,
                (int) t,
                RawValues.combineIntInt((int) s, (int) t));
    }

    private HugeRelationshipConsumer toHugeInConsumer(RelationshipConsumer consumer) {
        return (s, t) -> consumer.accept(
                (int) s,
                (int) t,
                RawValues.combineIntInt((int) t, (int) s));
    }

    private HugeRelationshipConsumer toHugeOutConsumer(WeightedRelationshipConsumer consumer) {
        return (s, t) -> {
            double weight = weightOf(s, t);
            return consumer.accept(
                    (int) s,
                    (int) t,
                    RawValues.combineIntInt((int) s, (int) t),
                    weight);
        };
    }

    private HugeRelationshipConsumer toHugeInConsumer(WeightedRelationshipConsumer consumer) {
        return (s, t) -> {
            double weight = weightOf(t, s);
            return consumer.accept(
                    (int) s,
                    (int) t,
                    RawValues.combineIntInt((int) t, (int) s),
                    weight);
        };
    }
}
