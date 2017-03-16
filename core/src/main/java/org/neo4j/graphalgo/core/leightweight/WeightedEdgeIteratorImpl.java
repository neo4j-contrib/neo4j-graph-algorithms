package org.neo4j.graphalgo.core.leightweight;


import org.neo4j.graphalgo.api.WeightedEdgeCursor;
import org.neo4j.graphalgo.core.WeightMapping;

import java.util.Iterator;

/**
 * @author phorn@avantgarde-labs.de
 */
class WeightedEdgeIteratorImpl implements Iterator<WeightedEdgeCursor> {

    private final WeightedEdgeCursor cursor = new WeightedEdgeCursor();
    private final IntArray.Cursor adjCursor;
    private final WeightMapping weightMapping;

    private long edgeId;

    private int[] array;
    private int pos;
    private int limit;

    WeightedEdgeIteratorImpl(
        int sourceVertexId,
        long offset,
        long length,
        WeightMapping weightMapping,
        IntArray adjacency) {
        this(sourceVertexId,
                offset,
                length,
                weightMapping,
                adjacency,
                adjacency.newCursor());
    }

    WeightedEdgeIteratorImpl(
            int sourceVertexId,
            long offset,
            long length,
            WeightMapping weightMapping,
            IntArray adjacency,
            IntArray.Cursor adjCursor) {
        this.weightMapping = weightMapping;
        edgeId = offset;
        cursor.sourceVertexId = sourceVertexId;
        this.adjCursor = adjacency.cursor(offset, length, adjCursor);
        nextPage();
    }

    @Override
    public boolean hasNext() {
        return pos < limit || nextPage();
    }

    @Override
    public WeightedEdgeCursor next() {
        cursor.weight = weightMapping.get(edgeId);
        cursor.edgeId = edgeId++;
        cursor.targetVertexId = array[pos++];
        return cursor;
    }

    private boolean nextPage() {
        if (adjCursor.next()) {
            array = adjCursor.array;
            pos = adjCursor.offset;
            limit = adjCursor.offset + adjCursor.length;
            return true;
        }
        pos = limit = 0;
        return false;
    }
}
