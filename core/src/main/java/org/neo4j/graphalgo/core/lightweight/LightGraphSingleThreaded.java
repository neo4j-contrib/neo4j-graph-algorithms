package org.neo4j.graphalgo.core.lightweight;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.IntArray;

public class LightGraphSingleThreaded extends LightGraph {

    private LightGraph underlying;

    private IntArray.Cursor reusedInCursor;
    private IntArray.Cursor reusedOutCursor;

    LightGraphSingleThreaded(
            final LightGraph underlying,
            final IdMap idMapping,
            final WeightMapping weightMapping,
            final IntArray inAdjacency,
            final IntArray outAdjacency,
            final long[] inOffsets,
            final long[] outOffsets) {
        super(idMapping, weightMapping, inAdjacency, outAdjacency, inOffsets, outOffsets);
        this.underlying = underlying;
        if (inAdjacency != null) {
            this.reusedInCursor = inAdjacency.newCursor();
        }
        if (outAdjacency != null) {
            this.reusedOutCursor = outAdjacency.newCursor();
        }
    }

    @Override
    public Graph singleThreaded() {
        return underlying.singleThreaded();
    }

    @Override
    public void forEachIncoming(
            final int node,
            final RelationshipConsumer consumer) {
        final long offset = inOffsets[node];
        final long length = inOffsets[node + 1] - offset;
        final IntArray.Cursor cursor = inAdjacency.initCursor(offset, length, reusedInCursor);
        consumeNodes(node, cursor, RawValues.INCOMING, consumer);
    }

    @Override
    public void forEachOutgoing(
            final int node,
            final RelationshipConsumer consumer) {
        final long offset = outOffsets[node];
        final long length = outOffsets[node + 1] - offset;
        final IntArray.Cursor cursor = outAdjacency.initCursor(offset, length, reusedOutCursor);
        consumeNodes(node, cursor, RawValues.OUTGOING, consumer);
    }

    @Override
    public void release() {
        inAdjacency = null;
        outAdjacency = null;
        weightMapping = null;
        inOffsets = null;
        outOffsets = null;
    }

}
