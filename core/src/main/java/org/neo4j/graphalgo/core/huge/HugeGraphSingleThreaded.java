package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.HugeRelationshipConsumer;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.ByteArray;
import org.neo4j.graphalgo.core.utils.paged.LongArray;

public class HugeGraphSingleThreaded extends HugeGraphImpl {

    private HugeGraphImpl underlying;

    private ByteArray.DeltaCursor reusedInCursor;
    private ByteArray.DeltaCursor reusedOutCursor;

    HugeGraphSingleThreaded(
            final HugeGraphImpl underlying,
            final AllocationTracker tracker,
            final HugeIdMap idMapping,
            final HugeWeightMapping weights,
            final ByteArray inAdjacency,
            final ByteArray outAdjacency,
            final LongArray inOffsets,
            final LongArray outOffsets) {
        super(tracker, idMapping, weights, inAdjacency, outAdjacency, inOffsets, outOffsets);
        this.underlying = underlying;
        if (inAdjacency != null) {
            this.reusedInCursor = inAdjacency.newCursor();
        }
        if (outAdjacency != null) {
            this.reusedOutCursor = outAdjacency.newCursor();
        }
    }

    @Override
    public HugeGraphImpl singleThreaded() {
        return underlying.singleThreaded();
    }

    @Override
    public void forEachIncoming(
            final long node,
            final HugeRelationshipConsumer consumer) {
        final long offset = inOffsets.get(node);
        reusedInCursor.init(offset);
        consumeNodes(node, reusedInCursor, consumer);
    }

    @Override
    public void forEachOutgoing(
            final long node,
            final HugeRelationshipConsumer consumer) {
        final long offset = outOffsets.get(node);
        if (offset == 0) {
            return;
        }
        reusedOutCursor.init(offset);
        consumeNodes(node, reusedOutCursor, consumer);
    }

    @Override
    public void release() {
        this.views = null;
        this.inAdjacency = null;
        this.inOffsets = null;
        this.outAdjacency = null;
        this.outOffsets = null;
        this.weights = null;
        this.empty = null;
        this.weights = null;
    }

}
