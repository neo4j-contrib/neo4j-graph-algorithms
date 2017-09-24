package org.neo4j.graphalgo.core.leightweight;

import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.utils.RawValues;

public class LightGraphSingleThreaded extends LightGraph {

    LightGraphSingleThreaded(
            final IdMap idMapping,
            final WeightMapping weightMapping,
            final IntArray inAdjacency,
            final IntArray outAdjacency,
            final long[] inOffsets,
            final long[] outOffsets) {
     super(idMapping, weightMapping, inAdjacency, outAdjacency, inOffsets, outOffsets);
    }

    private IntArray.Cursor reusedOutCursor = outAdjacency.cursor(0, 0);
    private IntArray.Cursor reusedInCursor = outAdjacency.cursor(0, 0);

    @Override
    public void forEachIncoming(
            final int node,
            final RelationshipConsumer consumer) {
        final long offset = inOffsets[node];
        final long length = inOffsets[node + 1] - offset;
        final IntArray.Cursor cursor = inAdjacency.cursor(offset, length, reusedInCursor);
        consumeNodes(node, cursor, RawValues.INCOMING, consumer);
    }

    @Override
    public void forEachOutgoing(
            final int node,
            final RelationshipConsumer consumer) {
        final long offset = outOffsets[node];
        final long length = outOffsets[node + 1] - offset;
        final IntArray.Cursor cursor = outAdjacency.cursor(offset, length, reusedOutCursor);
        consumeNodes(node, cursor, RawValues.OUTGOING, consumer);
    }

}
